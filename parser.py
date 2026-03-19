"""
Парсер Федресурса - проверяет компании на банкротство
С продвинутым обходом блокировок: stealth, прокси, человеческое поведение
"""
import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from playwright_stealth import stealth_async
import pandas as pd
from datetime import datetime, timezone, timedelta
import nest_asyncio
import os
import re
import random
import json
from fake_useragent import UserAgent
from tqdm import tqdm
from typing import List, Dict, Tuple, Optional, Set
import logging
import aiohttp
from aiohttp_socks import ProxyConnector

nest_asyncio.apply()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('logs/parser.log', encoding='utf-8', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FedresursBankruptcyChecker:
    # 🎭 Пул "домашних" пользовательских агентов
    HOME_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    
    # 🔄 Список бесплатных прокси (обновляется автоматически)
    PROXY_SOURCES = [
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all",
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=all",
    ]
    
    # 🗃️ Кэш успешных ответов (чтобы не парсить одно и то же)
    RESPONSE_CACHE: Dict[str, Tuple[str, datetime]] = {}
    CACHE_TTL_SECONDS = 3600  # 1 час
    
    # 🎯 Настройки "человеческого" поведения
    HUMAN_BEHAVIOR = {
        'scroll_steps': [0.1, 0.3, 0.6, 1.0],  # Прокрутка частями
        'mouse_jitter': True,  # Случайные движения мыши
        'random_pause_min': 1,
        'random_pause_max': 4,
        'page_load_extra_wait': [1, 3],  # Доп. ожидание после загрузки
    }

    def __init__(self, client_file: str, headless: bool = True, 
                 delay: int = 10, batch_size: int = 2, batch_delay: int = 90,
                 use_proxies: bool = True, max_retries: int = 3):
        self.client_file = client_file
        self.headless = headless
        self.delay = delay
        self.batch_size = batch_size
        self.batch_delay = batch_delay
        self.use_proxies = use_proxies
        self.max_retries = max_retries
        self.moscow_tz = timezone(timedelta(hours=3))
        self.today = datetime.now(self.moscow_tz).strftime('%Y-%m-%d')
        self.output_file = f"logs/excel/fedresurs_{self.today}.xlsx"
        self.html_file = f"logs/html/fedresurs_{self.today}.html"
        self.results = []
        self._working_proxies: List[str] = []
        self._proxy_last_update: Optional[datetime] = None
        self._ua = UserAgent()

        self.key_phrases = [
            "Намерение должника обратиться в суд с заявлением о банкротстве",
            "Намерение кредитора обратиться в суд с заявлением о банкротстве",
            "Сообщение о судебном акте. о признании должника банкротом и открытии конкурсного производства",
            "Сообщение о судебном акте. о введении наблюдения",
            "Предстоящее исключение недействующего юридического лица из реестра",
            "Направление в арбитражный суд заявления уполномоченного органа о признании должника банкротом",
            "Уведомление о проведении собрания работников, бывших работников должника",
            "Сведения о решениях, принятых собранием работников, бывших работников должника",
            "Сообщение о результатах проведения собрания кредиторов",
            "Сообщение о собрании кредиторов"
        ]
        self.intent_phrases = ["намерени", "исключение"]

        self.config = {
            'search': {
                'id_patterns': [
                    r'/companies/([a-f0-9-]{36})',
                    r'data-company-id=["\']([a-f0-9-]{36})["\']',
                ]
            },
            'sections': {
                'bankruptcy': ['Сведения о банкротстве', 'Банкротство'],
                'next_sections': ['Торги', 'Общая информация', 'Публикации', 'Обременения',
                                  'ЕИО', 'Сведения о СРО', 'Членство в СРО', 'Лицензии']
            },
            'patterns': {
                'case_number': r'([АA]\d{2,}[-\s]?\d{2,}[-\s]?\d{4})',
                'message': r'(\d{8})\s+от\s+(\d{2}\.\d{2}\.\d{4})'
            }
        }

    def _clean_html(self, text: str) -> str:
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _extract_message_info(self, text: str) -> Dict[str, str]:
        result = {'number': '', 'date': '', 'title': ''}
        match = re.search(r'(\d{8})\s+от\s+(\d{2}\.\d{2}\.\d{4})', text)
        if match:
            result['number'] = match.group(1)
            result['date'] = match.group(2)
        for phrase in self.key_phrases:
            if phrase.lower() in text.lower():
                result['title'] = phrase
                break
        return result

    def _is_intent(self, title: str) -> bool:
        return any(p in title.lower() for p in self.intent_phrases)

    def _extract_company_status(self, html: str) -> str:
        status_match = re.search(r'info-item-name[^>]*>Статус<[^>]*>\s*<[^>]*>\s*<[^>]*>\s*([^<]+)', html, re.I)
        if status_match:
            status_text = self._clean_html(status_match.group(1))
            if status_text and status_text != "нет данных":
                return status_text
        return ""

    def _extract_bankruptcy_data(self, html: str) -> Tuple[str, bool, List[Dict]]:
        messages, case_number, status, has_data = [], "", "", False
        company_status = self._extract_company_status(html)
        if company_status and "несостоятельным" in company_status.lower():
            status, has_data = company_status, True

        for section_name in self.config['sections']['bankruptcy']:
            pos = html.find(section_name)
            if pos != -1:
                end_pos = len(html)
                for next_section in self.config['sections']['next_sections']:
                    next_pos = html.find(next_section, pos + len(section_name))
                    if next_pos != -1 and next_pos < end_pos:
                        end_pos = next_pos
                section_html = html[pos:end_pos]
                section_clean = self._clean_html(section_html)
                if 'нет данных' in section_clean.lower() and not has_
                    return "Нет данных", False, []
                case_match = re.search(self.config['patterns']['case_number'], section_html)
                if case_match:
                    case_number = case_match.group(1)
                    has_data = True
                if not status:
                    for pattern in [r'конкурсное\s+производство', r'введено\s+наблюдение', 
                                    r'внешнее\s+управление', r'финансовое\s+оздоровление']:
                        if re.search(pattern, section_html, re.I):
                            status_match = re.search(
                                r'([^<>\n]{0,100}(?:конкурсное производство|наблюдение|внешнее управление|финансовое оздоровление)[^<>\n]{0,100})',
                                section_html, re.I)
                            if status_match:
                                status = self._clean_html(status_match.group(1)).strip()
                                has_data = True
                            break
                for match in re.finditer(self.config['patterns']['message'], section_html):
                    msg_num, msg_date = match.groups()
                    context = section_html[max(0, match.start()-300):min(len(section_html), match.end()+300)]
                    context_clean = self._clean_html(context)
                    msg_type = next((p for p in self.key_phrases if p.lower() in context_clean.lower()), "")
                    if msg_type:
                        messages.append({'number': msg_num, 'date': msg_date, 'title': msg_type,
                                       'is_intent': self._is_intent(msg_type)})
                        has_data = True
                break
        if not has_data and not messages:
            return "Нет данных", False, []
        formatted_parts = ["Сведения о банкротстве:"]
        group1 = ([case_number] if case_number else []) + ([status] if status else []) + \
                 [f"{m['number']} от {m['date']} {m['title']}" for m in messages if not m['is_intent']]
        if group1: formatted_parts.append(f"1) {' '.join(group1)}")
        intents = [f"{m['number']} от {m['date']} {m['title']}" for m in messages if m['is_intent']]
        if intents: formatted_parts.append(f"2) Сообщения о намерении {' '.join(intents)}")
        return '\n'.join(formatted_parts), True, messages

    async def _fetch_working_proxies(self) -> List[str]:
        """Получение и проверка рабочих прокси"""
        now = datetime.now()
        # Обновляем список раз в 2 часа
        if self._working_proxies and self._proxy_last_update and \
           (now - self._proxy_last_update).total_seconds() < 7200:
            return self._working_proxies
        proxies = []
        async with aiohttp.ClientSession() as session:
            for source in self.PROXY_SOURCES:
                try:
                    async with session.get(source, timeout=10) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            for line in text.strip().split('\n'):
                                if ':' in line and not line.startswith('#'):
                                    proxies.append(line.strip())
                except: continue
        # 🧪 Быстрая проверка 5 случайных прокси
        test_proxies = random.sample(proxies, min(5, len(proxies)))
        working = []
        for proxy in test_proxies:
            try:
                connector = ProxyConnector.from_url(f'socks5://{proxy}' if 'socks' in proxy else f'http://{proxy}')
                async with aiohttp.ClientSession(connector=connector) as s:
                    async with s.get('https://httpbin.org/ip', timeout=8) as r:
                        if r.status == 200:
                            working.append(proxy)
                            break
            except: continue
        if working:
            self._working_proxies = working + proxies  # рабочие + все остальные как бэкап
            self._proxy_last_update = now
            logger.info(f"🔄 Найдено {len(working)} рабочих прокси из {len(proxies)}")
        return self._working_proxies or proxies

    async def _human_like_scroll(self, page: Page):
        """Имитация человеческой прокрутки"""
        for step in self.HUMAN_BEHAVIOR['scroll_steps']:
            await page.evaluate(f'window.scrollTo(0, document.body.scrollHeight * {step})')
            await asyncio.sleep(random.uniform(0.3, 0.8))
        await page.evaluate('window.scrollTo(0, 0)')
        await asyncio.sleep(random.uniform(0.2, 0.6))

    async def _human_like_mouse(self, page: Page):
        """Имитация случайных движений мыши"""
        if not self.HUMAN_BEHAVIOR['mouse_jitter']: return
        for _ in range(random.randint(2, 5)):
            x = random.randint(100, 800)
            y = random.randint(100, 600)
            await page.mouse.move(x, y, steps=random.randint(10, 20))
            await asyncio.sleep(random.uniform(0.1, 0.4))

    def _get_cached_response(self, url: str) -> Optional[str]:
        """Получение ответа из кэша"""
        if url in self.RESPONSE_CACHE:
            html, timestamp = self.RESPONSE_CACHE[url]
            if (datetime.now() - timestamp).total_seconds() < self.CACHE_TTL_SECONDS:
                logger.debug(f"🗃️ Кэш-хит: {url}")
                return html
            else:
                del self.RESPONSE_CACHE[url]
        return None

    def _cache_response(self, url: str, html: str):
        """Сохранение ответа в кэш"""
        self.RESPONSE_CACHE[url] = (html, datetime.now())
        # Очистка старого кэша
        if len(self.RESPONSE_CACHE) > 100:
            oldest = min(self.RESPONSE_CACHE.items(), key=lambda x: x[1][1])[0]
            del self.RESPONSE_CACHE[oldest]

    async def _create_stealth_context(self, browser: Browser, proxy: Optional[str] = None) -> BrowserContext:
        """Создание контекста браузера с маскировкой"""
        # 🎭 Случайный пользовательский агент
        ua = random.choice(self.HOME_USER_AGENTS) if random.random() < 0.7 else self._ua.random
        
        context_args = {
            'user_agent': ua,
            'viewport': {'width': random.choice([1920, 1366, 1536]), 'height': random.choice([1080, 768, 864])},
            'locale': 'ru-RU', 'timezone_id': 'Europe/Moscow',
            'permissions': ['geolocation'], 'geolocation': {'latitude': 55.75, 'longitude': 37.62},
        }
        if proxy:
            context_args['proxy'] = {'server': f"{proxy.split('@')[-1]}" if '@' in proxy else proxy}
        
        context = await browser.new_context(**context_args)
        await stealth_async(context)  # 🎭 Включение stealth-режима
        return context

    async def find_company_id(self, inn: str) -> Optional[str]:
        """Поиск ID компании с максимальным обходом блокировок"""
        url = f"https://fedresurs.ru/entities?searchString={inn.strip()}"
        
        # 🗃️ Проверка кэша
        cached = self._get_cached_response(url)
        if cached:
            for pattern in self.config['search']['id_patterns']:
                match = re.search(pattern, cached)
                if match: return match.group(1)
        
        # 🔄 Получение прокси
        proxies = await self._fetch_working_proxies() if self.use_proxies else []
        proxy = random.choice(proxies) if proxies else None
        
        for attempt in range(self.max_retries + 1):
            try:
                async with async_playwright() as p:
                    browser_args = ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
                    if proxy: browser_args.append(f'--proxy-server={proxy}')
                    
                    browser = await p.chromium.launch(headless=self.headless, args=browser_args)
                    context = await self._create_stealth_context(browser, proxy)
                    page = await context.new_page()
                    
                    # 🎭 "Человеческие" задержки
                    await asyncio.sleep(random.uniform(3, 8))
                    
                    # 🎭 Переход на страницу
                    await page.goto(url, wait_until='domcontentloaded', timeout=120000)
                    await asyncio.sleep(random.uniform(*self.HUMAN_BEHAVIOR['page_load_extra_wait']))
                    
                    # 🎭 Имитация поведения человека
                    await self._human_like_mouse(page)
                    await self._human_like_scroll(page)
                    await asyncio.sleep(random.uniform(1, 3))
                    
                    html = await page.content()
                    await browser.close()
                    
                    # 🗃️ Кэширование
                    self._cache_response(url, html)
                    
                    for pattern in self.config['search']['id_patterns']:
                        match = re.search(pattern, html)
                        if match: return match.group(1)
                        
            except Exception as e:
                wait = min(60, (attempt + 1) * 20 + random.uniform(5, 15))
                logger.warning(f"⚠️ {inn}: Попытка {attempt+1}/{self.max_retries+1} не удалась ({str(e)[:60]}), ждём {wait:.0f}с")
                await asyncio.sleep(wait)
                # 🔄 Пробуем другой прокси при следующей попытке
                if proxies: proxy = random.choice([p for p in proxies if p != proxy] or proxies)
        return None

    async def check_bankruptcy(self, inn: str) -> Tuple[str, str]:
        """Проверка компании с полным набором обходов"""
        for attempt in range(self.max_retries + 1):
            try:
                company_id = await self.find_company_id(inn)
                if not company_id: return "Компания не найдена", ""
                main_url = f"https://fedresurs.ru/companies/{company_id}"
                pub_url = f"{main_url}/publications"
                
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=self.headless, 
                        args=['--no-sandbox', '--disable-setuid-sandbox'])
                    context = await self._create_stealth_context(browser)
                    page = await context.new_page()
                    
                    await page.goto(main_url, wait_until='domcontentloaded', timeout=120000)
                    await asyncio.sleep(random.uniform(2, 4))
                    await self._human_like_scroll(page)
                    main_html = await page.content()
                    await page.close()
                    
                    if not main_html: return "Ошибка загрузки страницы", ""
                    
                    bankruptcy_status, has_data, _ = self._extract_bankruptcy_data(main_html)
                    trades = await self._extract_trades_data(main_url, browser)
                    publications = await self._extract_publications_from_page(pub_url, browser)
                    await browser.close()
                
                # Формирование результата
                pub_lines = [f"- {p['number']} от {p['date']} {p['title']}" for p in publications]
                trade_lines = []
                for t in trades:
                    line = f"- {t.get('number', '')}"
                    if 'start_date' in t and 'end_date' in t:
                        line += f" Дата торгов {t['start_date']} — {t['end_date']}"
                    if 'organizer' in t: line += f" {t['organizer']}"
                    trade_lines.append(line)
                
                if (publications or trades) and not has_
                    parts = ["Сведения о банкротстве:"]
                    if trades: parts.extend(["Торги\nПродажа имущества при банкротстве"] + trade_lines)
                    if publications: parts.extend(["Публикации:"] + pub_lines)
                    return "\n".join(parts), ""
                if not has_data and not publications and not trades: return "Нет данных", ""
                
                status_parts = ([bankruptcy_status] if has_ else [])
                if trades:
                    prefix = "\nТорги\nПродажа имущества при банкротстве" if status_parts else "Сведения о банкротстве:\nТорги\nПродажа имущества при банкротстве"
                    status_parts.extend([prefix] + trade_lines)
                final_status = "\n".join(status_parts) if status_parts else "Нет данных"
                return final_status, ("\n".join(pub_lines) if pub_lines else "")
                
            except Exception as e:
                if attempt >= self.max_retries:
                    logger.error(f"❌ {inn}: Ошибка после {self.max_retries+1} попыток: {str(e)[:100]}")
                    return f"Ошибка: {str(e)[:100]}", ""
                wait = min(90, (attempt + 1) * 25 + random.uniform(10, 20))
                logger.warning(f"⚠️ {inn}: Попытка {attempt+1}/{self.max_retries+1}, ждём {wait:.0f}с")
                await asyncio.sleep(wait)

    # ... остальные методы (_extract_publications_from_page, _extract_trades_data, read_companies, 
    # generate_html_table, run_with_batches) остаются без изменений, как в предыдущей версии ...
    # Для краткости не дублирую их здесь, но в полном файле они должны быть

    async def _extract_publications_from_page(self, url, browser) -> List[Dict]:
        publications = []
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until='networkidle')
            await asyncio.sleep(2)
            # Упрощённая загрузка публикаций
            cards = await page.query_selector_all('entity-card-publications-search-result-card')
            for card in cards:
                title_el = await card.query_selector('.fw-light')
                if not title_el: continue
                title = (await title_el.inner_text()).strip()
                matched = next((p for p in self.key_phrases if p.lower() in title.lower()), None)
                if matched:
                    link_el = await card.query_selector('a.underlined')
                    if not link_el: continue
                    info = self._extract_message_info((await link_el.inner_text()).strip())
                    if info['number'] and info['date']:
                        publications.append({'number': info['number'], 'date': info['date'],
                                           'title': matched, 'is_intent': self._is_intent(matched)})
            publications.sort(key=lambda x: datetime.strptime(x['date'], '%d.%m.%Y'), reverse=True)
        finally: await page.close()
        return publications

    async def _extract_trades_data(self, url, browser) -> List[Dict]:
        trades = []
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until='networkidle')
            await asyncio.sleep(2)
            if not await page.query_selector('div.info-header:has-text("Торги")'): return trades
            cards = await page.query_selector_all('entity-card-biddings-block-bidding-card')
            for card in cards:
                trade = {}
                num_link = await card.query_selector('a.number-link')
                if num_link: trade['number'] = await num_link.inner_text()
                date_div = await card.query_selector('div:has-text("Дата торгов")')
                if date_div:
                    parent = await date_div.query_selector('xpath=..')
                    if parent:
                        spans = await parent.query_selector_all('span')
                        if len(spans) >= 3:
                            trade['start_date'] = await spans[0].inner_text()
                            trade['end_date'] = await spans[2].inner_text()
                org_link = await card.query_selector('a[href*="/companies/"]')
                if org_link: trade['organizer'] = await org_link.inner_text()
                if trade: trades.append(trade)
        except: pass
        finally: await page.close()
        return trades

    def read_companies(self) -> pd.DataFrame:
        df = pd.read_excel(self.client_file, header=5)
        name_col = inn_col = None
        for col in df.columns:
            if 'ИНН' in str(col): inn_col = col
            if 'Наименование' in str(col): name_col = col
        if not name_col or not inn_col: raise ValueError("Не найдены колонки: ИНН, Наименование")
        companies = df[[name_col, inn_col]].copy()
        companies.columns = ['name', 'inn']
        companies = companies.dropna()
        def clean_inn(v):
            if pd.isna(v): return ""
            try: return str(int(float(v)))
            except: return str(v).strip().rstrip('.0')
        companies['inn'] = companies['inn'].apply(clean_inn)
        companies['name'] = companies['name'].astype(str).str.strip()
        return companies

    def generate_html_table(self, df: pd.DataFrame) -> str:
        total = len(df)
        stats = {"no_data": sum(s in ["Нет данных", "Компания не найдена"] for s in df['Банкротство']),
                 "has_signs": sum(s not in ["Нет данных", "Компания не найдена"] and not s.startswith("Ошибка") for s in df['Банкротство']),
                 "errors": sum(s.startswith("Ошибка") for s in df['Банкротство'])}
        moscow_now = datetime.now(self.moscow_tz).strftime('%d.%m.%Y %H:%M MSK')
        html = f"""<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Проверка банкротства</title><style>@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
*{{margin:0;padding:0;box-sizing:border-box}}body{{font-family:'Roboto',sans-serif;background:linear-gradient(135deg,#2c3e50,#3498db);min-height:100vh;padding:40px 20px}}
.container{{max-width:1400px;margin:0 auto;background:white;border-radius:20px;box-shadow:0 20px 60px rgba(0,0,0,0.3);overflow:hidden}}
.header{{background:linear-gradient(135deg,#2c3e50,#3498db);color:white;padding:30px;text-align:center}}.header h1{{font-size:32px;font-weight:500;margin-bottom:10px}}
.date-badge{{display:inline-block;background:rgba(255,255,255,0.2);padding:8px 20px;border-radius:50px;margin-top:15px;font-size:14px}}
.table-wrapper{{padding:30px;overflow-x:auto}}table{{width:100%;border-collapse:collapse;border-radius:15px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.1)}}
thead{{background:linear-gradient(135deg,#2c3e50,#3498db);color:white}}th{{padding:18px 15px;font-weight:500;font-size:16px;text-transform:uppercase}}
tbody tr:nth-child(even){{background-color:#f8f9ff}}tbody tr:hover{{background-color:#e8f4ff}}
td{{padding:18px 15px;border-bottom:1px solid #e0e0e0;color:#333;font-size:14px;vertical-align:top}}
.inn-cell{{font-family:'Courier New',monospace;font-weight:500;color:#3498db}}.name-cell{{font-weight:500;color:#2c3e50}}
.publications-cell{{max-width:400px;white-space:pre-line;line-height:1.4;font-size:12px}}
.formatted-status{{line-height:1.6;white-space:pre-line;font-family:'Courier New',monospace;font-size:12px}}
.status-badge{{display:inline-block;padding:4px 12px;border-radius:50px;font-size:12px;font-weight:500;margin-bottom:8px}}
.badge-success{{background:#e8f8f0;color:#27ae60}}.badge-warning{{background:#fef5e7;color:#e67e22}}.badge-error{{background:#f9ebea;color:#c0392b}}
.stats{{display:flex;justify-content:space-around;margin:20px;padding:20px;background:#f8f9ff;border-radius:15px}}
.stat-item{{text-align:center}}.stat-value{{font-size:24px;font-weight:700;color:#3498db}}.stat-label{{font-size:14px;color:#666;margin-top:5px}}
.footer{{background:#f9f9f9;padding:20px;text-align:center;border-top:1px solid #e0e0e0}}</style></head><body>
<div class="container"><div class="header"><h1>🔍 Проверка статуса банкротства</h1><p>Федресурс</p>
<div class="date-badge">{moscow_now}</div></div><div class="table-wrapper"><table><thead><tr>
<th>№</th><th>ИНН</th><th>Наименование</th><th>Статус</th><th>Публикации</th></tr></thead><tbody>"""
        for idx, row in df.iterrows():
            status, pubs = row['Банкротство'], row.get('Публикации', '')
            if status in ["Нет данных", "Компания не найдена"]: badge, display, disp_status = "badge-success", "Нет данных", ""
            elif status.startswith("Ошибка"): badge, display, disp_status = "badge-error", "Ошибка", status
            else: badge, display, disp_status = "badge-warning", "Есть признаки", status
            status_html = disp_status.replace('<', '&lt;').replace('>', '&gt;').replace('\n', '<br>')
            html += f"""<tr><td>{idx+1}</td><td class="inn-cell">{row['ИНН']}</td><td class="name-cell"><strong>{row['Наименование']}</strong></td>
<td><span class="status-badge {badge}">{display}</span><div class="formatted-status">{status_html}</div></td>
<td class="publications-cell">{pubs.replace(chr(10), '<br>') if pubs else ''}</td></tr>"""
        html += f"""</tbody></table></div><div class="stats">
<div class="stat-item"><div class="stat-value">{total}</div><div class="stat-label">Всего</div></div>
<div class="stat-item"><div class="stat-value" style="color:#27ae60">{stats['no_data']}</div><div class="stat-label">Нет данных</div></div>
<div class="stat-item"><div class="stat-value" style="color:#e67e22">{stats['has_signs']}</div><div class="stat-label">Есть признаки</div></div>
<div class="stat-item"><div class="stat-value" style="color:#c0392b">{stats['errors']}</div><div class="stat-label">Ошибки</div></div>
</div><div class="footer"><p>© {datetime.now().year} • Отчет сгенерирован автоматически</p></div></div></body></html>"""
        return html

    async def run_with_batches(self) -> Tuple[pd.DataFrame, str, str]:
        logger.info("🚀 Запуск парсинга")
        companies = self.read_companies()
        logger.info(f"📋 Загружено компаний: {len(companies)}")
        for i in range(0, len(companies), self.batch_size):
            batch = companies.iloc[i:i+self.batch_size]
            logger.info(f"📦 Пакет {i//self.batch_size + 1}: компании {i+1}-{min(i+self.batch_size, len(companies))}")
            for _, row in batch.iterrows():
                logger.info(f"🔍 Проверка: {row['inn']} - {row['name']}")
                status, pubs = await self.check_bankruptcy(row['inn'])
                self.results.append({'ИНН': row['inn'], 'Наименование': row['name'],
                                   'Банкротство': status, 'Публикации': pubs,
                                   'timestamp': datetime.now(self.moscow_tz).isoformat()})
                await asyncio.sleep(self.delay)
            if i + self.batch_size < len(companies):
                logger.info(f"⏸️ Пауза {self.batch_delay} сек...")
                await asyncio.sleep(self.batch_delay)
        df = pd.DataFrame(self.results)
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        os.makedirs(os.path.dirname(self.html_file), exist_ok=True)
        df.to_excel(self.output_file, index=False)
        with open(self.html_file, 'w', encoding='utf-8') as f:
            f.write(self.generate_html_table(df))
        logger.info(f"✅ Готово! Excel: {self.output_file}, HTML: {self.html_file}")
        return df, self.output_file, self.html_file
