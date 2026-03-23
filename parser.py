"""
Парсер Федресурса — рабочая версия с сохранением в logs/
"""
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
import nest_asyncio
import os
import re
from tqdm import tqdm
from typing import List, Dict, Tuple

nest_asyncio.apply()


class FedresursBankruptcyChecker:
    def __init__(self, client_file: str, headless: bool = False, delay: int = 3):
        self.client_file = client_file
        self.headless = headless
        self.delay = delay
        self.today = datetime.now().strftime('%Y-%m-%d')
        
        # Сохраняем в папки logs/ для гибридной модели
        self.output_file = f"logs/excel/fedresurs_{self.today}.xlsx"
        self.html_file = f"logs/html/fedresurs_{self.today}.html"
        self.results = []

        # Создаём папки
        os.makedirs("logs/excel", exist_ok=True)
        os.makedirs("logs/html", exist_ok=True)

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
        title_lower = title.lower()
        return any(phrase in title_lower for phrase in self.intent_phrases)

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
                    status_patterns = [
                        r'конкурсное\s+производство',
                        r'введено\s+наблюдение',
                        r'внешнее\s+управление',
                        r'финансовое\s+оздоровление'
                    ]
                    for pattern in status_patterns:
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
                    context_start = max(0, start_pos - 300)
                    context_end = min(len(section_html), match.end() + 300)
                    context = section_html[context_start:context_end]
                    context_clean = self._clean_html(context)

                    msg_type = ""
                    for phrase in self.key_phrases:
                        if phrase.lower() in context_clean.lower():
                            msg_type = phrase
                            has_data = True
                            break

                    if msg_type:
                        messages.append({
                            'number': msg_num,
                            'date': msg_date,
                            'title': msg_type,
                            'is_intent': self._is_intent(msg_type)
                        })

                break

        if not has_data and not messages:
            return "Нет данных", False, []

        formatted_parts = []
        formatted_parts.append("Сведения о банкротстве:")

        group1_parts = []
        if case_number:
            group1_parts.append(case_number)
        if status:
            group1_parts.append(status)
        for msg in messages:
            if not msg['is_intent']:
                group1_parts.append(f"{msg['number']} от {msg['date']} {msg['title']}")
        if group1_parts:
            formatted_parts.append(f"1) {' '.join(group1_parts)}")

        intent_messages = [m for m in messages if m['is_intent']]
        if intent_messages:
            group2_parts = ["Сообщения о намерении"]
            for msg in intent_messages:
                group2_parts.append(f"{msg['number']} от {msg['date']} {msg['title']}")
            formatted_parts.append(f"2) {' '.join(group2_parts)}")

        return '\n'.join(formatted_parts), True, messages

    async def _load_all_publications(self, page) -> None:
        max_attempts = 50
        attempts = 0
        while attempts < max_attempts:
            try:
                more_button = await page.wait_for_selector('div.more_btn:has-text("Загрузить еще")', timeout=3000)
                if more_button and await more_button.is_visible():
                    await more_button.click()
                    await asyncio.sleep(2)
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    attempts += 1
                else:
                    break
            except:
                break

    async def _extract_publications_from_page(self, publications_url: str, browser) -> List[Dict]:
        publications = []
        page = await browser.new_page()
        try:
            await page.goto(publications_url, wait_until='networkidle')
            await asyncio.sleep(2)
            await self._load_all_publications(page)

            cards = await page.query_selector_all('entity-card-publications-search-result-card')
            for card in cards:
                title_element = await card.query_selector('.fw-light')
                if not title_element:
                    continue
                title = await title_element.inner_text()
                title = title.strip()

                matched_phrase = None
                for phrase in self.key_phrases:
                    if phrase.lower() in title.lower():
                        matched_phrase = phrase
                        break

                if matched_phrase:
                    link_element = await card.query_selector('a.underlined')
                    if not link_element:
                        continue
                    link_text = await link_element.inner_text()
                    link_text = link_text.strip()

                    msg_info = self._extract_message_info(link_text)
                    if msg_info['number'] and msg_info['date']:
                        publications.append({
                            'number': msg_info['number'],
                            'date': msg_info['date'],
                            'title': matched_phrase,
                            'is_intent': self._is_intent(matched_phrase)
                        })

            def extract_date(pub: Dict) -> datetime:
                return datetime.strptime(pub['date'], '%d.%m.%Y')

            publications.sort(key=extract_date, reverse=True)
        finally:
            await page.close()

        return publications

    async def _extract_trades_data(self, main_url: str, browser) -> List[Dict]:
        trades = []
        page = await browser.new_page()
        try:
            await page.goto(main_url, wait_until='networkidle')
            await asyncio.sleep(2)
            
            trades_header = await page.query_selector('div.info-header:has-text("Торги")')
            if not trades_header:
                return trades
            
            sales_header = await page.query_selector('div.type-header:has-text("Продажа имущества при банкротстве")')
            if not sales_header:
                return trades
            
            no_data = await page.query_selector('div:has-text("Нет данных")')
            if no_data:
                return trades
            
            cards = await page.query_selector_all('entity-card-biddings-block-bidding-card')
            for card in cards:
                trade = {}
                
                number_link = await card.query_selector('a.number-link')
                if number_link:
                    trade['number'] = await number_link.inner_text()
                
                date_div = await card.query_selector('div:has-text("Дата торгов")')
                if date_div:
                    date_parent = await date_div.query_selector('xpath=..')
                    if date_parent:
                        date_spans = await date_parent.query_selector_all('span')
                        if len(date_spans) >= 3:
                            trade['start_date'] = await date_spans[0].inner_text()
                            trade['end_date'] = await date_spans[2].inner_text()
                
                operator_link = await card.query_selector('a[href*="/companies/"]')
                if operator_link:
                    trade['organizer'] = await operator_link.inner_text()
                
                if trade:
                    trades.append(trade)
        except Exception as e:
            print(f"Ошибка при парсинге торгов: {e}")
        finally:
            await page.close()
        
        return trades

    async def find_company_id(self, inn: str) -> str:
        url = f"https://fedresurs.ru/entities?searchString={inn}"
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
        except:
            pass
        return None

    async def check_bankruptcy(self, inn: str) -> Tuple[str, str]:
        try:
            company_id = await self.find_company_id(inn)
            if not company_id:
                return "Компания не найдена", ""

            main_url = f"https://fedresurs.ru/companies/{company_id}"
            publications_url = f"{main_url}/publications"

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=self.headless)

                page = await browser.new_page()
                await page.goto(main_url, wait_until='networkidle', timeout=60000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
                main_html = await page.content()
                await page.close()

                if not main_html:
                    await browser.close()
                    return "Ошибка загрузки страницы", ""

                bankruptcy_status, has_data, bankruptcy_messages = self._extract_bankruptcy_data(main_html)
                trades_data = await self._extract_trades_data(main_url, browser)
                publications = await self._extract_publications_from_page(publications_url, browser)
                await browser.close()

                pub_lines = []
                for pub in publications:
                    pub_lines.append(f"- {pub['number']} от {pub['date']} {pub['title']}")
                
                trades_lines = []
                for trade in trades_data:
                    trade_line = f"- {trade.get('number', '')}"
                    if 'start_date' in trade and 'end_date' in trade:
                        trade_line += f" Дата торгов {trade['start_date']} — {trade['end_date']}"
                    if 'organizer' in trade:
                        trade_line += f" {trade['organizer']}"
                    trades_lines.append(trade_line)

                if (publications or trades_data) and not has_data:
                    status_parts = ["Сведения о банкротстве:"]
                    
                    if trades_data:
                        status_parts.append("Торги\nПродажа имущества при банкротстве")
                        status_parts.extend(trades_lines)
                    
                    if publications:
                        status_parts.append("Публикации:")
                        status_parts.extend(pub_lines)
                    
                    final_status = "\n".join(status_parts)
                    return final_status, ""

                if not has_data and not publications and not trades_data:
                    return "Нет данных", ""

                status_parts = []
                
                if has_data:
                    status_parts.append(bankruptcy_status)
                
                if trades_data:
                    if status_parts:
                        status_parts.append("\nТорги\nПродажа имущества при банкротстве")
                    else:
                        status_parts.append("Сведения о банкротстве:\nТорги\nПродажа имущества при банкротстве")
                    status_parts.extend(trades_lines)

                final_status = "\n".join(status_parts) if status_parts else "Нет данных"
                publications_text = "\n".join(pub_lines) if pub_lines else ""

                return final_status, publications_text

        except Exception as e:
            return f"Ошибка: {str(e)[:100]}", ""

    def read_companies(self) -> pd.DataFrame:
        df = pd.read_excel(self.client_file, header=5)
        name_col, inn_col = None, None
        for col in df.columns:
            col_str = str(col)
            if 'ИНН' in col_str:
                inn_col = col
            if 'Наименование' in col_str:
                name_col = col
        if not name_col or not inn_col:
            raise ValueError("Не найдены нужные колонки")
        companies = df[[name_col, inn_col]].copy()
        companies.columns = ['name', 'inn']
        companies = companies.dropna()
        
        def clean_inn(value):
            if pd.isna(value):
                return ""
            try:
                return str(int(float(value)))
            except (ValueError, TypeError):
                return str(value).strip().rstrip('.0')
        
        companies['inn'] = companies['inn'].apply(clean_inn)
        companies['name'] = companies['name'].astype(str).str.strip()
        return companies

    def format_bankruptcy_status(self, status_text: str) -> str:
        if not status_text or status_text in ["Компания не найдена", "Ошибка загрузки страницы"]:
            return status_text
        if status_text.startswith("Ошибка:"):
            return status_text
        if status_text == "Нет данных":
            return ""
        status_text = status_text.replace('<', '&lt;').replace('>', '&gt;')
        status_text = status_text.replace('\n', '<br>')
        return f'<div class="formatted-status">{status_text}</div>'

    def generate_html_table(self, df: pd.DataFrame) -> str:
        total = len(df)
        no_bankruptcy = 0
        has_bankruptcy = 0
        errors = 0
        for status in df['Банкротство']:
            if status == "Нет данных" or status == "Компания не найдена":
                no_bankruptcy += 1
            elif status.startswith("Ошибка"):
                errors += 1
            elif status != "Нет данных":
                has_bankruptcy += 1
            else:
                no_bankruptcy += 1

        html = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Результаты проверки банкротства</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
font-family: 'Roboto', sans-serif;
background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
min-height: 100vh;
padding: 40px 20px;
}}
.container {{
max-width: 1400px;
margin: 0 auto;
background: white;
border-radius: 20px;
box-shadow: 0 20px 60px rgba(0,0,0,0.3);
overflow: hidden;
}}
.header {{
background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
color: white;
padding: 30px;
text-align: center;
}}
.header h1 {{ font-size: 32px; font-weight: 500; margin-bottom: 10px; }}
.header p {{ font-size: 16px; opacity: 0.9; }}
.date-badge {{
display: inline-block;
background: rgba(255,255,255,0.2);
padding: 8px 20px;
border-radius: 50px;
margin-top: 15px;
font-size: 14px;
}}
.table-wrapper {{ padding: 30px; overflow-x: auto; }}
table {{
width: 100%;
border-collapse: collapse;
border-radius: 15px;
overflow: hidden;
box-shadow: 0 5px 15px rgba(0,0,0,0.1);
}}
thead {{ background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%); color: white; }}
th {{ padding: 18px 15px; font-weight: 500; font-size: 16px; text-transform: uppercase; }}
tbody tr:nth-child(even) {{ background-color: #f8f9ff; }}
tbody tr:hover {{ background-color: #e8f4ff; }}
td {{ padding: 18px 15px; border-bottom: 1px solid #e0e0e0; color: #333; font-size: 14px; vertical-align: top; }}
.inn-cell {{ font-family: 'Courier New', monospace; font-weight: 500; color: #3498db; }}
.name-cell {{ font-weight: 500; color: #2c3e50; }}
.publications-cell {{
max-width: 400px;
white-space: pre-line;
line-height: 1.4;
font-size: 12px;
}}
.formatted-status {{
line-height: 1.6;
white-space: pre-line;
font-family: 'Courier New', monospace;
font-size: 12px;
}}
.status-badge {{
display: inline-block;
padding: 4px 12px;
border-radius: 50px;
font-size: 12px;
font-weight: 500;
margin-bottom: 8px;
}}
.badge-success {{ background: #e8f8f0; color: #27ae60; }}
.badge-warning {{ background: #fef5e7; color: #e67e22; }}
.badge-error {{ background: #f9ebea; color: #c0392b; }}
.stats {{
display: flex;
justify-content: space-around;
margin: 20px;
padding: 20px;
background: #f8f9ff;
border-radius: 15px;
}}
.stat-item {{ text-align: center; }}
.stat-value {{ font-size: 24px; font-weight: 700; color: #3498db; }}
.stat-label {{ font-size: 14px; color: #666; margin-top: 5px; }}
.footer {{ background: #f9f9f9; padding: 20px; text-align: center; border-top: 1px solid #e0e0e0; }}
</style>
</head>
<body>
<div class="container">
<div class="header">
<h1>Проверка статуса банкротства</h1>
<p>Федресурс</p>
<div class="date-badge">{datetime.now().strftime('%d.%m.%Y %H:%M')}</div>
</div>
<div class="table-wrapper">
<table>
<thead>
<tr>
<th>№</th>
<th>ИНН</th>
<th>Наименование</th>
<th>Статус банкротства</th>
<th>Публикации</th>
</tr>
</thead>
<tbody>
"""
        for idx, row in df.iterrows():
            status = row['Банкротство']
            publications = row['Публикации'] if 'Публикации' in row else ""

            if status == "Нет данных" or status == "Компания не найдена":
                badge_class = "badge-success"
                badge_text = "Нет данных"
                display_status = ""
            elif status.startswith("Ошибка"):
                badge_class = "badge-error"
                badge_text = "Ошибка"
                display_status = status
            else:
                badge_class = "badge-warning"
                badge_text = "Есть признаки"
                display_status = status

            formatted_status = self.format_bankruptcy_status(display_status)
            formatted_publications = publications.replace('\n', '<br>') if publications else ""

            html += f"""
<tr>
<td>{idx + 1}</td>
<td class="inn-cell">{row['ИНН']}</td>
<td class="name-cell"><strong>{row['Наименование']}</strong></td>
<td>
<span class="status-badge {badge_class}">{badge_text}</span>
{formatted_status}
</td>
<td class="publications-cell">{formatted_publications}</td>
</tr>
"""

        html += f"""
</tbody>
</table>
</div>
<div class="stats">
<div class="stat-item">
<div class="stat-value">{total}</div>
<div class="stat-label">Всего</div>
</div>
<div class="stat-item">
<div class="stat-value" style="color: #27ae60;">{no_bankruptcy}</div>
<div class="stat-label">Нет данных</div>
</div>
<div class="stat-item">
<div class="stat-value" style="color: #e67e22;">{has_bankruptcy}</div>
<div class="stat-label">Есть признаки</div>
</div>
<div class="stat-item">
<div class="stat-value" style="color: #c0392b;">{errors}</div>
<div class="stat-label">Ошибки</div>
</div>
</div>
<div class="footer">
<p>© {datetime.now().year} • Отчет сгенерирован автоматически</p>
</div>
</div>
</body>
</html>
"""
        return html

    async def run(self):
        print("\n" + "=" * 100)
        print("ПРОВЕРКА СТАТУСА БАНКРОТСТВА".center(100))
        print("=" * 100 + "\n")
        companies = self.read_companies()
        print(f" Загружено компаний: {len(companies)}")
        print(f" Компании для проверки:")
        for _, row in companies.iterrows():
            print(f"   - {row['inn']} - {row['name']}")
        print()
        for _, row in tqdm(companies.iterrows(), total=len(companies), desc="Проверка"):
            status, publications = await self.check_bankruptcy(row['inn'])
            self.results.append({
                'ИНН': row['inn'],
                'Наименование': row['name'],
                'Банкротство': status,
                'Публикации': publications
            })
            await asyncio.sleep(self.delay)
        df = pd.DataFrame(self.results)
        df.to_excel(self.output_file, index=False)
        with open(self.html_file, 'w', encoding='utf-8') as f:
            f.write(self.generate_html_table(df))
        return df, self.html_file


async def main():
    checker = FedresursBankruptcyChecker(
        client_file="Клиенты_страхование_ТЕСТ.xlsx",
        headless=False,
        delay=3
    )
    results, html_file = await checker.run()
    print("\n" + "=" * 120)
    print("РЕЗУЛЬТАТЫ".center(120))
    print("=" * 120)
    print(results.to_string(index=False))
    print("\n" + "=" * 120)
    print(f" Excel файл: {checker.output_file}".center(120))
    print(f" HTML отчет: {html_file}".center(120))
    print("=" * 120)
    import webbrowser
    webbrowser.open(f"file://{os.path.abspath(html_file)}")


if __name__ == "__main__":
    import webbrowser
    asyncio.run(main())