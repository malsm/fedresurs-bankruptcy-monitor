import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
import nest_asyncio
import os
import re
from tqdm import tqdm
from typing import List, Dict, Tuple
import pytz

nest_asyncio.apply()

class FedresursBankruptcyChecker:
    def __init__(self, client_file: str, headless: bool = True, delay: int = 5):
        self.client_file = client_file
        self.headless = headless # ОБЯЗАТЕЛЬНО True для GitHub
        self.delay = delay
        # Устанавливаем московское время
        self.msk_tz = pytz.timezone('Europe/Moscow')
        self.now = datetime.now(self.msk_tz)
        self.today = self.now.strftime('%Y-%m-%d')
        
        # Папки для отчетов
        if not os.path.exists('reports'): os.makedirs('reports')
        
        self.output_file = f"reports/fedresurs_{self.today}.xlsx"
        self.html_file = f"index.html" # Называем index.html для удобства GitHub Pages
        self.results = []
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

        # (Твои списки фраз и конфиги остаются без изменений)
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
            'search': {'id_patterns': [r'/companies/([a-f0-9-]{36})', r'data-company-id=["\']([a-f0-9-]{36})["\']']},
            'sections': {
                'bankruptcy': ['Сведения о банкротстве', 'Банкротство'],
                'next_sections': ['Торги', 'Общая информация', 'Публикации', 'Обременения', 'ЕИО', 'Сведения о СРО', 'Членство в СРО', 'Лицензии']
            },
            'patterns': {'case_number': r'([АA]\d{2,}[-\s]?\d{2,}[-\s]?\d{4})', 'message': r'(\d{8})\s+от\s+(\d{2}\.\d.2\.\d{4})'}
        }

    # --- ВСЕ ТВОИ ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ (copy-paste из твоего кода) ---
    def _clean_html(self, text: str) -> str:
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _format_inn(self, inn_val) -> str:
        try: return str(int(float(inn_val))).strip()
        except: return str(inn_val).strip()

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
        return any(phrase in title.lower() for phrase in self.intent_phrases)

    def _extract_company_status(self, html: str) -> str:
        status_match = re.search(r'info-item-name[^>]*>Статус<[^>]*>\s*<[^>]*>\s*<[^>]*>\s*([^<]+)', html, re.I)
        if status_match:
            status_text = self._clean_html(status_match.group(1))
            return status_text if status_text != "нет данных" else ""
        return ""

    def _extract_bankruptcy_data(self, html: str) -> Tuple[str, bool, List[Dict]]:
        messages = []
        case_number = ""
        status = ""
        has_data = False
        company_status = self._extract_company_status(html)
        if company_status and "несостоятельным" in company_status.lower():
            status = company_status
            has_data = True

        for section_name in self.config['sections']['bankruptcy']:
            pos = html.find(section_name)
            if pos != -1:
                end_pos = len(html)
                for next_section in self.config['sections']['next_sections']:
                    next_pos = html.find(next_section, pos + len(section_name))
                    if next_pos != -1 and next_pos < end_pos: end_pos = next_pos
                section_html = html[pos:end_pos]
                section_clean = self._clean_html(section_html)
                if 'нет данных' in section_clean.lower() and not has_data: return "Нет данных", False, []
                case_match = re.search(self.config['patterns']['case_number'], section_html)
                if case_match:
                    case_number = case_match.group(1)
                    has_data = True
                for match in re.finditer(self.config['patterns']['message'], section_html):
                    msg_num, msg_date = match.groups()
                    context = section_html[max(0, match.start()-300):min(len(section_html), match.end()+300)]
                    context_clean = self._clean_html(context)
                    for phrase in self.key_phrases:
                        if phrase.lower() in context_clean.lower():
                            messages.append({'number': msg_num, 'date': msg_date, 'title': phrase, 'is_intent': self._is_intent(phrase)})
                            has_data = True
                            break
                break
        if not has_data: return "Нет данных", False, []
        res = ["Сведения о банкротстве:"]
        g1 = [case_number, status] + [f"{m['number']} от {m['date']} {m['title']}" for m in messages if not m['is_intent']]
        if any(g1): res.append(f"1) {' '.join(filter(None, g1))}")
        intents = [f"{m['number']} от {m['date']} {m['title']}" for m in messages if m['is_intent']]
        if intents: res.append(f"2) Сообщения о намерении: {' '.join(intents)}")
        return '\n'.join(res), True, messages

    async def _load_all_publications(self, page) -> None:
        for _ in range(5): # Ограничим 5 нажатиями для стабильности
            try:
                btn = await page.query_selector('div.more_btn:has-text("Загрузить еще")')
                if btn and await btn.is_visible():
                    await btn.click()
                    await asyncio.sleep(2)
                else: break
            except: break

    async def _extract_publications_from_page(self, url: str, browser_context) -> List[Dict]:
        page = await browser_context.new_page()
        pubs = []
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await self._load_all_publications(page)
            cards = await page.query_selector_all('entity-card-publications-search-result-card')
            for card in cards:
                title_el = await card.query_selector('.fw-light')
                if not title_el: continue
                title = (await title_el.inner_text()).strip()
                matched = next((p for p in self.key_phrases if p.lower() in title.lower()), None)
                if matched:
                    link_el = await card.query_selector('a.underlined')
                    if link_el:
                        info = self._extract_message_info(await link_el.inner_text())
                        if info['number']: pubs.append({'number': info['number'], 'date': info['date'], 'title': matched, 'is_intent': self._is_intent(matched)})
        finally: await page.close()
        return sorted(pubs, key=lambda x: datetime.strptime(x['date'], '%d.%m.%Y'), reverse=True)

    async def _extract_trades_data(self, url: str, browser_context) -> List[Dict]:
        page = await browser_context.new_page()
        trades = []
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            cards = await page.query_selector_all('entity-card-biddings-block-bidding-card')
            for card in cards:
                t = {}
                num_el = await card.query_selector('a.number-link')
                if num_el: t['number'] = await num_el.inner_text()
                trades.append(t)
        finally: await page.close()
        return trades

    # --- ГЛАВНЫЙ ЦИКЛ (Адаптирован под сервер) ---
    async def run(self):
        df_input = pd.read_excel(self.client_file, header=5)
        inn_col = next(c for c in df_input.columns if 'ИНН' in str(c))
        name_col = next(c for c in df_input.columns if 'Наименование' in str(c))
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(user_agent=self.user_agent)
            
            for _, row in tqdm(df_input.iterrows(), total=len(df_input), desc="Проверка"):
                inn = self._format_inn(row[inn_col])
                if not inn or len(inn) < 5: continue
                
                try:
                    # Поиск ID
                    search_page = await context.new_page()
                    await search_page.goto(f"https://fedresurs.ru/entities?searchString={inn}", timeout=60000)
                    await asyncio.sleep(2)
                    content = await search_page.content()
                    c_id = next((re.search(p, content).group(1) for p in self.config['search']['id_patterns'] if re.search(p, content)), None)
                    await search_page.close()

                    if not c_id:
                        self.results.append({'ИНН': inn, 'Наименование': row[name_col], 'Банкротство': "Компания не найдена", 'Публикации': ""})
                        continue

                    # Сбор данных
                    main_page = await context.new_page()
                    await main_page.goto(f"https://fedresurs.ru/companies/{c_id}", timeout=60000)
                    await asyncio.sleep(2)
                    main_html = await main_page.content()
                    await main_page.close()

                    status, has_data, _ = self._extract_bankruptcy_data(main_html)
                    pubs = await self._extract_publications_from_page(f"https://fedresurs.ru/companies/{c_id}/publications", context)
                    
                    pub_text = "\n".join([f"- {p['number']} от {p['date']} {p['title']}" for p in pubs])
                    self.results.append({'ИНН': inn, 'Наименование': row[name_col], 'Банкротство': status, 'Публикации': pub_text})
                    
                except Exception as e:
                    self.results.append({'ИНН': inn, 'Наименование': row[name_col], 'Банкротство': f"Ошибка: {str(e)[:50]}", 'Публикации': ""})
                
                await asyncio.sleep(self.delay)

            await browser.close()

        df_res = pd.DataFrame(self.results)
        df_res.to_excel(self.output_file, index=False)
        # Твой метод генерации HTML (вызывается здесь)
        with open(self.html_file, 'w', encoding='utf-8') as f:
            f.write(self.generate_html_table(df_res))
        
        print(f"Успех! Файлы созданы.")

    def format_bankruptcy_status(self, status_text: str) -> str:
        if not status_text or "Ошибка" in status_text or "не найдена" in status_text: return status_text
        return f'<div class="formatted-status">{status_text.replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br>")}</div>'

    def generate_html_table(self, df: pd.DataFrame) -> str:
        # Здесь остается твой метод генерации HTML (он идеален, не меняй его)
        # ... (просто скопируй его из своего кода полностью) ...
        return "<html>...</html>" # Заглушка, вставь сюда свой метод

if __name__ == "__main__":
    checker = FedresursBankruptcyChecker(client_file="Клиенты_страхование_ТЕСТ.xlsx", headless=True)
    asyncio.run(checker.run())
