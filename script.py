import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
import nest_asyncio
import os
import re
from tqdm import tqdm
from typing import List, Dict, Tuple
import random

nest_asyncio.apply()

class FedresursBankruptcyChecker:
    def __init__(self, client_file: str, headless: bool = True, delay: int = 3):
        self.client_file = client_file
        self.headless = headless
        self.delay = delay
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.output_file = f"fedresurs_{self.today}.xlsx"
        self.html_file = f"fedresurs_{self.today}.html"
        self.results = []

        # Ваша оригинальная логика фраз
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
                'message': r'(\d{8})\s+от\s+(\d{2}\.\d.2}\.\d{4})'
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
        return any(phrase in title.lower() for phrase in self.intent_phrases)

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
                if 'нет данных' in section_clean.lower() and not has_data:
                    return "Нет данных", False, []
                case_match = re.search(self.config['patterns']['case_number'], section_html)
                if case_match:
                    case_number = case_match.group(1)
                    has_data = True
                # Поиск сообщений
                for match in re.finditer(r'(\d{8})\s+от\s+(\d{2}\.\d{2}\.\d{4})', section_html):
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
        
        # Формирование строки результата (ваша логика)
        res_str = f"Дело: {case_number} " if case_number else ""
        res_str += f"Статус: {status} " if status else ""
        for m in messages:
            res_str += f"\n{m['number']} от {m['date']} {m['title']}"
        return res_str, True, messages

    async def _load_all_publications(self, page) -> None:
        for _ in range(5): # Ограничим 5 кликами для сервера
            try:
                more_button = await page.wait_for_selector('div.more_btn:has-text("Загрузить еще")', timeout=3000)
                if more_button:
                    await more_button.click()
                    await asyncio.sleep(2)
                else: break
            except: break

    async def _extract_publications_from_page(self, publications_url: str, browser) -> List[Dict]:
        publications = []
        page = await browser.new_page()
        try:
            await page.goto(publications_url, wait_until='domcontentloaded', timeout=60000)
            await asyncio.sleep(3)
            await self._load_all_publications(page)
            cards = await page.query_selector_all('entity-card-publications-search-result-card')
            for card in cards:
                title_el = await card.query_selector('.fw-light')
                if title_el:
                    title = (await title_el.inner_text()).strip()
                    for phrase in self.key_phrases:
                        if phrase.lower() in title.lower():
                            link_el = await card.query_selector('a.underlined')
                            if link_el:
                                msg_info = self._extract_message_info(await link_el.inner_text())
                                if msg_info['number']:
                                    publications.append({**msg_info, 'title': phrase, 'is_intent': self._is_intent(phrase)})
                            break
        finally:
            await page.close()
        return publications

    async def _extract_trades_data(self, main_url: str, browser) -> List[Dict]:
        trades = []
        page = await browser.new_page()
        try:
            await page.goto(main_url, wait_until='domcontentloaded', timeout=60000)
            cards = await page.query_selector_all('entity-card-biddings-block-bidding-card')
            for card in cards:
                num_el = await card.query_selector('a.number-link')
                if num_el:
                    trades.append({'number': await num_el.inner_text()})
        except: pass
        finally:
            await page.close()
        return trades

    async def find_company_id(self, inn: str, browser) -> str:
        page = await browser.new_page()
        try:
            await page.goto(f"https://fedresurs.ru/entities?searchString={inn}", wait_until='domcontentloaded', timeout=60000)
            await asyncio.sleep(4)
            html = await page.content()
            for pattern in self.config['search']['id_patterns']:
                match = re.search(pattern, html)
                if match: return match.group(1)
        finally:
            await page.close()
        return None

    async def check_bankruptcy(self, inn: str, browser) -> Tuple[str, str]:
        company_id = await self.find_company_id(inn, browser)
        if not company_id: return "Компания не найдена", ""

        # Собираем данные
        main_page = await browser.new_page()
        await main_page.goto(f"https://fedresurs.ru/companies/{company_id}", wait_until='domcontentloaded', timeout=60000)
        await asyncio.sleep(3)
        main_html = await main_page.content()
        await main_page.close()

        b_status, has_data, b_msgs = self._extract_bankruptcy_data(main_html)
        trades = await self._extract_trades_data(f"https://fedresurs.ru/companies/{company_id}", browser)
        pubs = await self._extract_publications_from_page(f"https://fedresurs.ru/companies/{company_id}/publications", browser)

        pub_text = "\n".join([f"- {p['number']} от {p['date']} {p['title']}" for p in pubs])
        trade_text = "\n".join([f"Торги: {t['number']}" for t in trades])
        
        final_status = b_status
        if trade_text: final_status += f"\n{trade_text}"
        
        return final_status, pub_text

    def read_companies(self) -> pd.DataFrame:
        df = pd.read_excel(self.client_file, header=5)
        inn_col = next(c for c in df.columns if 'ИНН' in str(c))
        name_col = next(c for c in df.columns if 'Наименование' in str(c))
        companies = df[[name_col, inn_col]].dropna().copy()
        companies.columns = ['name', 'inn']
        companies['inn'] = companies['inn'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        return companies

    def generate_html_table(self, df: pd.DataFrame) -> str:
        # Ваш оригинальный красивый HTML генератор (сокращено для краткости, оставьте ваш полный стиль)
        rows = ""
        for idx, row in df.iterrows():
            rows += f"<tr><td>{idx+1}</td><td>{row['ИНН']}</td><td>{row['Наименование']}</td><td>{row['Банкротство']}</td><td>{row['Публикации']}</td></tr>"
        
        return f"<html><head><meta charset='UTF-8'><style>table{{width:100%;border-collapse:collapse;}}th,td{{border:1px solid #ddd;padding:8px;}}th{{background:#f2f2f2;}}</style></head><body><h1>Отчет Федресурс</h1><table><tr><th>№</th><th>ИНН</th><th>Наименование</th><th>Статус</th><th>Публикации</th></tr>{rows}</table></body></html>"

    async def run(self):
        companies = self.read_companies()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, 
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
            )
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            
            for i, row in tqdm(companies.iterrows(), total=len(companies)):
                if i > 0 and i % 5 == 0: await asyncio.sleep(15)
                status, publications = await self.check_bankruptcy(row['inn'], context)
                self.results.append({'ИНН': row['inn'], 'Наименование': row['name'], 'Банкротство': status, 'Публикации': publications})
                await asyncio.sleep(random.uniform(2, 4))
            
            await browser.close()
        
        df = pd.DataFrame(self.results)
        df.to_excel(self.output_file, index=False)
        with open(self.html_file, 'w', encoding='utf-8') as f:
            f.write(self.generate_html_table(df))

async def main():
    checker = FedresursBankruptcyChecker(client_file="Клиенты_страхование_ТЕСТ.xlsx")
    await checker.run()

if __name__ == "__main__":
    asyncio.run(main())
