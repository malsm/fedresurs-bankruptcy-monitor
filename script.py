import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
import pytz
import os
import re
from tqdm import tqdm
from typing import List, Dict, Tuple

class FedresursBankruptcyChecker:
    def __init__(self, client_file: str, headless: bool = True, delay: int = 3):
        self.client_file = client_file
        self.headless = headless
        self.delay = delay
        self.moscow_tz = pytz.timezone('Europe/Moscow')
        self.now = datetime.now(self.moscow_tz)
        self.today_str = self.now.strftime('%Y-%m-%d')
        self.output_file = f"results_{self.today_str}.xlsx"
        self.html_file = "index.html"
        self.results = []

        # Твои проверенные ключевые фразы
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
            'patterns': {
                'case_number': r'([АA]\d{2,}[-\s]?\d{2,}[-\s]?\d{4})',
                'message': r'(\d{8})\s+от\s+(\d{2}\.\d{2}\.\d{4})'
            }
        }

    def _clean_html(self, text: str) -> str:
        text = re.sub(r'<[^>]+>', '', text)
        return re.sub(r'\s+', ' ', text).strip()

    def _extract_message_info(self, text: str) -> Dict[str, str]:
        result = {'number': '', 'date': '', 'title': ''}
        match = re.search(r'(\d{8})\s+от\s+(\d{2}\.\d{2}\.\d{4})', text)
        if match:
            result['number'], result['date'] = match.group(1), match.group(2)
        for phrase in self.key_phrases:
            if phrase.lower() in text.lower():
                result['title'] = phrase
                break
        return result

    def _is_intent(self, title: str) -> bool:
        return any(phrase in title.lower() for phrase in self.intent_phrases)

    def _extract_company_status(self, html: str) -> str:
        status_match = re.search(r'info-item-name[^>]*>Статус<[^>]*>\s*<[^>]*>\s*<[^>]*>\s*([^<]+)', html, re.I)
        return self._clean_html(status_match.group(1)) if status_match else ""

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
                    if next_pos != -1 and next_pos < end_pos: end_pos = next_pos

                section_html = html[pos:end_pos]
                if 'нет данных' in self._clean_html(section_html).lower() and not has_data:
                    return "Нет данных", False, []

                case_match = re.search(self.config['patterns']['case_number'], section_html)
                if case_match: case_number, has_data = case_match.group(1), True

                for match in re.finditer(self.config['patterns']['message'], section_html):
                    msg_num, msg_date = match.groups()
                    context = self._clean_html(section_html[max(0, match.start()-300):min(len(section_html), match.end()+300)])
                    for phrase in self.key_phrases:
                        if phrase.lower() in context.lower():
                            messages.append({'number': msg_num, 'date': msg_date, 'title': phrase, 'is_intent': self._is_intent(phrase)})
                            has_data = True
                            break
                break

        if not has_data and not messages: return "Нет данных", False, []

        formatted_parts = ["Сведения о банкротстве:"]
        g1 = [case_number] + [status] + [f"{m['number']} от {m['date']} {m['title']}" for m in messages if not m['is_intent']]
        if any(g1): formatted_parts.append(f"1) {' '.join(filter(None, g1))}")
        
        intents = [f"{m['number']} от {m['date']} {m['title']}" for m in messages if m['is_intent']]
        if intents: formatted_parts.append(f"2) Сообщения о намерении: {' '.join(intents)}")
        
        return '\n'.join(formatted_parts), True, messages

    async def _extract_trades_data(self, main_url: str, browser) -> List[Dict]:
        trades = []
        page = await browser.new_page()
        try:
            await page.goto(main_url, wait_until='networkidle', timeout=30000)
            cards = await page.query_selector_all('entity-card-biddings-block-bidding-card')
            for card in cards:
                trade = {}
                num_link = await card.query_selector('a.number-link')
                if num_link: trade['number'] = await num_link.inner_text()
                trades.append(trade)
        except: pass
        finally: await page.close()
        return trades

    async def find_company_id(self, inn: str, browser) -> str:
        page = await browser.new_page()
        try:
            await page.goto(f"https://fedresurs.ru/entities?searchString={inn}", wait_until='networkidle', timeout=30000)
            html = await page.content()
            for pattern in self.config['search']['id_patterns']:
                match = re.search(pattern, html)
                if match: return match.group(1)
        except: pass
        finally: await page.close()
        return None

    async def check_bankruptcy(self, inn: str, browser) -> Tuple[str, str]:
        try:
            company_id = await self.find_company_id(inn, browser)
            if not company_id: return "Компания не найдена", ""

            main_url = f"https://fedresurs.ru/companies/{company_id}"
            page = await browser.new_page()
            await page.goto(main_url, wait_until='networkidle', timeout=60000)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
            main_html = await page.content()
            await page.close()

            status, has_data, _ = self._extract_bankruptcy_data(main_html)
            trades = await self._extract_trades_data(main_url, browser)
            
            if trades and not has_data:
                status = "Сведения о банкротстве:\nЕсть активные торги"
            
            return status, ""
        except Exception as e:
            return f"Ошибка: {str(e)[:50]}", ""

    def read_companies(self) -> pd.DataFrame:
        # header=5 означает, что названия колонок в 6-й строке Excel
        df = pd.read_excel(self.client_file, header=5)
        
        # Поиск нужных колонок (ИНН и Наименование)
        inn_col = next((c for c in df.columns if 'ИНН' in str(c).upper()), None)
        name_col = next((c for c in df.columns if 'НАИМЕНОВАНИЕ' in str(c).upper()), None)
        
        if not inn_col or not name_col:
            raise ValueError(f"Колонки не найдены. Доступны: {list(df.columns)}")
            
        companies = df[[name_col, inn_col]].dropna().copy()
        companies.columns = ['name', 'inn']
        companies['inn'] = companies['inn'].astype(str).str.strip().str.replace('.0', '', regex=False)
        return companies

    def generate_html_table(self, df: pd.DataFrame) -> str:
        rows = ""
        for idx, row in df.iterrows():
            status = row['Банкротство']
            badge = "badge-success" if status == "Нет данных" else "badge-warning"
            if "Ошибка" in status: badge = "badge-error"
            
            rows += f"""
            <tr>
                <td>{idx+1}</td>
                <td>{row['ИНН']}</td>
                <td><strong>{row['Наименование']}</strong></td>
                <td><span class="status-badge {badge}">{status}</span></td>
            </tr>"""
        return f"<html><head><style>body{{font-family:sans-serif;padding:20px}} table{{width:100%;border-collapse:collapse}} td,th{{padding:10px;border:1px solid #ddd}} .status-badge{{padding:4px 8px;border-radius:4px;font-size:12px}} .badge-success{{background:#e8f8f0;color:#27ae60}} .badge-warning{{background:#fff3cd;color:#856404}} .badge-error{{background:#f8d7da;color:#721c24}}</style></head><body><h1>Отчет Федресурс {self.now.strftime('%d.%m.%Y')}</h1><table><thead><tr><th>№</th><th>ИНН</th><th>Компания</th><th>Статус</th></tr></thead><tbody>{rows}</tbody></table></body></html>"

    async def run(self):
        companies = self.read_companies()
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            
            # Обработка батчами по 5
            for i in range(0, len(companies), 5):
                batch = companies.iloc[i:i+5]
                print(f"Парсинг группы {i//5 + 1} из {(len(companies)-1)//5 + 1}...")
                
                for _, row in batch.iterrows():
                    status, _ = await self.check_bankruptcy(row['inn'], browser)
                    self.results.append({'ИНН': row['inn'], 'Наименование': row['name'], 'Банкротство': status})
                
                if i + 5 < len(companies):
                    await asyncio.sleep(self.delay)

            await browser.close()

        df_final = pd.DataFrame(self.results)
        df_final.to_excel(self.output_file, index=False)
        with open(self.html_file, 'w', encoding='utf-8') as f: f.write(self.generate_html_table(df_final))
        print(f"Готово! Результаты в {self.output_file}")

if __name__ == "__main__":
    checker = FedresursBankruptcyChecker("Клиенты_страхование_ТЕСТ.xlsx")
    asyncio.run(checker.run())
