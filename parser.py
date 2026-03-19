"""
Парсер Федресурса - проверяет компании на банкротство
"""
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime, timezone, timedelta
import nest_asyncio
import os
import re
from tqdm import tqdm
from typing import List, Dict, Tuple, Optional
import logging

nest_asyncio.apply()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class FedresursBankruptcyChecker:
    def __init__(self, client_file: str, headless: bool = True, 
                 delay: int = 3, batch_size: int = 5, batch_delay: int = 30):
        self.client_file = client_file
        self.headless = headless
        self.delay = delay
        self.batch_size = batch_size
        self.batch_delay = batch_delay
        self.moscow_tz = timezone(timedelta(hours=3))
        self.today = datetime.now(self.moscow_tz).strftime('%Y-%m-%d')
        self.output_file = f"logs/excel/fedresurs_{self.today}.xlsx"
        self.html_file = f"logs/html/fedresurs_{self.today}.html"
        self.results = []

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
                    if next_pos != -1 and next_pos < end_pos:
                        end_pos = next_pos

                section_html = html[pos:end_pos]
                section_clean = self._clean_html(section_html)

                if 'нет данных' in section_clean.lower():
                    if has_data:
                        break
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
                    start_pos = match.start()
                    context = section_html[max(0, start_pos-300):min(len(section_html), match.end()+300)]
                    context_clean = self._clean_html(context)

                    msg_type = next((p for p in self.key_phrases if p.lower() in context_clean.lower()), "")
                    if msg_type:
                        messages.append({
                            'number': msg_num, 'date': msg_date, 'title': msg_type,
                            'is_intent': self._is_intent(msg_type)
                        })
                        has_data = True
                break

        if not has_data and not messages:
            return "Нет данных", False, []

        formatted_parts = ["Сведения о банкротстве:"]
        group1 = []
        if case_number: group1.append(case_number)
        if status: group1.append(status)
        for msg in messages:
            if not msg['is_intent']:
                group1.append(f"{msg['number']} от {msg['date']} {msg['title']}")
        if group1:
            formatted_parts.append(f"1) {' '.join(group1)}")

        intents = [m for m in messages if m['is_intent']]
        if intents:
            group2 = ["Сообщения о намерении"] + [f"{m['number']} от {m['date']} {m['title']}" for m in intents]
            formatted_parts.append(f"2) {' '.join(group2)}")

        return '\n'.join(formatted_parts), True, messages

    async def _load_all_publications(self, page):
        for _ in range(50):
            try:
                btn = await page.wait_for_selector('div.more_btn:has-text("Загрузить еще")', timeout=3000)
                if btn and await btn.is_visible():
                    await btn.click()
                    await asyncio.sleep(2)
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                else:
                    break
            except:
                break

    async def _extract_publications_from_page(self, url, browser) -> List[Dict]:
        publications = []
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until='networkidle')
            await asyncio.sleep(2)
            await self._load_all_publications(page)

            cards = await page.query_selector_all('entity-card-publications-search-result-card')
            for card in cards:
                title_el = await card.query_selector('.fw-light')
                if not title_el: continue
                title = (await title_el.inner_text()).strip()
                matched = next((p for p in self.key_phrases if p.lower() in title.lower()), None)
                if matched:
                    link_el = await card.query_selector('a.underlined')
                    if not link_el: continue
                    link_text = (await link_el.inner_text()).strip()
                    info = self._extract_message_info(link_text)
                    if info['number'] and info['date']:
                        publications.append({
                            'number': info['number'], 'date': info['date'],
                            'title': matched, 'is_intent': self._is_intent(matched)
                        })
            publications.sort(key=lambda x: datetime.strptime(x['date'], '%d.%m.%Y'), reverse=True)
        finally:
            await page.close()
        return publications

    async def _extract_trades_data(self, url, browser) -> List[Dict]:
        trades = []
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until='networkidle')
            await asyncio.sleep(2)
            
            if not await page.query_selector('div.info-header:has-text("Торги")'):
                return trades
            if not await page.query_selector('div.type-header:has-text("Продажа имущества при банкротстве")'):
                return trades
            if await page.query_selector('div:has-text("Нет данных")'):
                return trades

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
        except Exception as e:
            logger.error(f"Ошибка торгов: {e}")
        finally:
            await page.close()
        return trades

    async def find_company_id(self, inn: str) -> Optional[str]:
        url = f"https://fedresurs.ru/entities?searchString={inn.strip()}"
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=self.headless)
                page = await browser.new_page()
                await page.goto(url, wait_until='networkidle', timeout=60000)
                await asyncio.sleep(2)
                html = await page.content()
                await browser.close()
                for pattern in self.config['search']['id_patterns']:
                    match = re.search(pattern, html)
                    if match:
                        return match.group(1)
        except Exception as e:
            logger.warning(f"Ошибка поиска {inn}: {e}")
        return None

    async def check_bankruptcy(self, inn: str) -> Tuple[str, str]:
        try:
            company_id = await self.find_company_id(inn)
            if not company_id:
                return "Компания не найдена", ""

            main_url = f"https://fedresurs.ru/companies/{company_id}"
            pub_url = f"https://fedresurs.ru/companies/{company_id}/publications"

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=self.headless)
                page = await browser.new_page()
                await page.goto(main_url, wait_until='networkidle', timeout=60000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
                main_html = await page.content()
                await page.close()

                if not main_html:
                    return "Ошибка загрузки страницы", ""

                bankruptcy_status, has_data, _ = self._extract_bankruptcy_data(main_html)
                trades = await self._extract_trades_data(main_url, browser)
                publications = await self._extract_publications_from_page(pub_url, browser)
                await browser.close()

                pub_lines = [f"- {p['number']} от {p['date']} {p['title']}" for p in publications]
                trade_lines = []
                for t in trades:
                    line = f"- {t.get('number', '')}"
                    if 'start_date' in t and 'end_date' in t:
                        line += f" Дата торгов {t['start_date']} — {t['end_date']}"
                    if 'organizer' in t:
                        line += f" {t['organizer']}"
                    trade_lines.append(line)

                if (publications or trades) and not has_data:
                    parts = ["Сведения о банкротстве:"]
                    if trades: parts.extend(["Торги\nПродажа имущества при банкротстве"] + trade_lines)
                    if publications: parts.extend(["Публикации:"] + pub_lines)
                    return "\n".join(parts), ""

                if not has_data and not publications and not trades:
                    return "Нет данных", ""

                status_parts = []
                if has_data: status_parts.append(bankruptcy_status)
                if trades:
                    prefix = "\nТорги\nПродажа имущества при банкротстве" if status_parts else "Сведения о банкротстве:\nТорги\nПродажа имущества при банкротстве"
                    status_parts.append(prefix)
                    status_parts.extend(trade_lines)

                final_status = "\n".join(status_parts) if status_parts else "Нет данных"
                pub_text = "\n".join(pub_lines) if pub_lines else ""
                return final_status, pub_text

        except Exception as e:
            logger.error(f"Ошибка {inn}: {e}")
            return f"Ошибка: {str(e)[:100]}", ""

    def read_companies(self) -> pd.DataFrame:
        df = pd.read_excel(self.client_file, header=5)
        name_col = inn_col = None
        for col in df.columns:
            if 'ИНН' in str(col): inn_col = col
            if 'Наименование' in str(col): name_col = col
        if not name_col or not inn_col:
            raise ValueError("Не найдены колонки: ИНН, Наименование")
        
        companies = df[[name_col, inn_col]].copy()
        companies.columns = ['name', 'inn']
        companies = companies.dropna()
        
        def clean_inn(value):
            if pd.isna(value): return ""
            try: return str(int(float(value)))
            except: return str(value).strip().rstrip('.0')
        
        companies['inn'] = companies['inn'].apply(clean_inn)
        companies['name'] = companies['name'].astype(str).str.strip()
        return companies

    def generate_html_table(self, df: pd.DataFrame) -> str:
        total = len(df)
        stats = {"no_data": 0, "has_signs": 0, "errors": 0}
        for s in df['Банкротство']:
            if s in ["Нет данных", "Компания не найдена"]: stats["no_data"] += 1
            elif s.startswith("Ошибка"): stats["errors"] += 1
            else: stats["has_signs"] += 1

        moscow_now = datetime.now(self.moscow_tz).strftime('%d.%m.%Y %H:%M MSK')
        
        html = f"""<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Проверка банкротства</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Roboto',sans-serif;background:linear-gradient(135deg,#2c3e50,#3498db);min-height:100vh;padding:40px 20px}}
.container{{max-width:1400px;margin:0 auto;background:white;border-radius:20px;box-shadow:0 20px 60px rgba(0,0,0,0.3);overflow:hidden}}
.header{{background:linear-gradient(135deg,#2c3e50,#3498db);color:white;padding:30px;text-align:center}}
.header h1{{font-size:32px;font-weight:500;margin-bottom:10px}}
.date-badge{{display:inline-block;background:rgba(255,255,255,0.2);padding:8px 20px;border-radius:50px;margin-top:15px;font-size:14px}}
.table-wrapper{{padding:30px;overflow-x:auto}}
table{{width:100%;border-collapse:collapse;border-radius:15px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.1)}}
thead{{background:linear-gradient(135deg,#2c3e50,#3498db);color:white}}
th{{padding:18px 15px;font-weight:500;font-size:16px;text-transform:uppercase}}
tbody tr:nth-child(even){{background-color:#f8f9ff}}
tbody tr:hover{{background-color:#e8f4ff}}
td{{padding:18px 15px;border-bottom:1px solid #e0e0e0;color:#333;font-size:14px;vertical-align:top}}
.inn-cell{{font-family:'Courier New',monospace;font-weight:500;color:#3498db}}
.name-cell{{font-weight:500;color:#2c3e50}}
.publications-cell{{max-width:400px;white-space:pre-line;line-height:1.4;font-size:12px}}
.formatted-status{{line-height:1.6;white-space:pre-line;font-family:'Courier New',monospace;font-size:12px}}
.status-badge{{display:inline-block;padding:4px 12px;border-radius:50px;font-size:12px;font-weight:500;margin-bottom:8px}}
.badge-success{{background:#e8f8f0;color:#27ae60}}
.badge-warning{{background:#fef5e7;color:#e67e22}}
.badge-error{{background:#f9ebea;color:#c0392b}}
.stats{{display:flex;justify-content:space-around;margin:20px;padding:20px;background:#f8f9ff;border-radius:15px}}
.stat-item{{text-align:center}}
.stat-value{{font-size:24px;font-weight:700;color:#3498db}}
.stat-label{{font-size:14px;color:#666;margin-top:5px}}
.footer{{background:#f9f9f9;padding:20px;text-align:center;border-top:1px solid #e0e0e0}}
</style></head><body>
<div class="container"><div class="header">
<h1> Проверка статуса банкротства</h1><p>Федресурс</p>
<div class="date-badge">{moscow_now}</div></div>
<div class="table-wrapper"><table><thead><tr>
<th>№</th><th>ИНН</th><th>Наименование</th><th>Статус</th><th>Публикации</th></tr></thead><tbody>"""
        
        for idx, row in df.iterrows():
            status = row['Банкротство']
            pubs = row.get('Публикации', '')
            if status in ["Нет данных", "Компания не найдена"]:
                badge, display, disp_status = "badge-success", "Нет данных", ""
            elif status.startswith("Ошибка"):
                badge, display, disp_status = "badge-error", "Ошибка", status
            else:
                badge, display, disp_status = "badge-warning", "Есть признаки", status
            
            status_html = disp_status.replace('<', '&lt;').replace('>', '&gt;').replace('\n', '<br>')
            html += f"""<tr>
<td>{idx+1}</td><td class="inn-cell">{row['ИНН']}</td>
<td class="name-cell"><strong>{row['Наименование']}</strong></td>
<td><span class="status-badge {badge}">{display}</span><div class="formatted-status">{status_html}</div></td>
<td class="publications-cell">{pubs.replace(chr(10), '<br>') if pubs else ''}</td></tr>"""
        
        html += f"""</tbody></table></div>
<div class="stats">
<div class="stat-item"><div class="stat-value">{total}</div><div class="stat-label">Всего</div></div>
<div class="stat-item"><div class="stat-value" style="color:#27ae60">{stats['no_data']}</div><div class="stat-label">Нет данных</div></div>
<div class="stat-item"><div class="stat-value" style="color:#e67e22">{stats['has_signs']}</div><div class="stat-label">Есть признаки</div></div>
<div class="stat-item"><div class="stat-value" style="color:#c0392b">{stats['errors']}</div><div class="stat-label">Ошибки</div></div>
</div><div class="footer"><p>© {datetime.now().year} • Отчет сгенерирован автоматически</p></div></div></body></html>"""
        return html

    async def run_with_batches(self) -> Tuple[pd.DataFrame, str, str]:
        logger.info(" Запуск парсинга")
        companies = self.read_companies()
        logger.info(f" Загружено компаний: {len(companies)}")
        
        for i in range(0, len(companies), self.batch_size):
            batch = companies.iloc[i:i+self.batch_size]
            logger.info(f" Пакет {i//self.batch_size + 1}: компании {i+1}-{min(i+self.batch_size, len(companies))}")
            
            for _, row in batch.iterrows():
                logger.info(f" Проверка: {row['inn']} - {row['name']}")
                status, pubs = await self.check_bankruptcy(row['inn'])
                self.results.append({
                    'ИНН': row['inn'],
                    'Наименование': row['name'],
                    'Банкротство': status,
                    'Публикации': pubs,
                    'timestamp': datetime.now(self.moscow_tz).isoformat()
                })
                await asyncio.sleep(self.delay)
            
            if i + self.batch_size < len(companies):
                logger.info(f" Пауза {self.batch_delay} сек...")
                await asyncio.sleep(self.batch_delay)
        
        df = pd.DataFrame(self.results)
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        os.makedirs(os.path.dirname(self.html_file), exist_ok=True)
        df.to_excel(self.output_file, index=False)
        with open(self.html_file, 'w', encoding='utf-8') as f:
            f.write(self.generate_html_table(df))
        
        logger.info(f" Готово! Excel: {self.output_file}, HTML: {self.html_file}")
        return df, self.output_file, self.html_file
