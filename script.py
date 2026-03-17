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
    def __init__(self, client_file: str, headless: bool = True, delay: int = 3, batch_size: int = 5, batch_pause: int = 15):
        self.client_file = client_file
        self.headless = headless
        self.delay = delay
        self.batch_size = batch_size
        self.batch_pause = batch_pause
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.output_file = f"fedresurs_{self.today}.xlsx"
        self.html_file = f"fedresurs_{self.today}.html"
        self.results = []

        # Ключевые фразы для фильтрации
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
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _is_intent(self, title: str) -> bool:
        return any(phrase in title.lower() for phrase in self.intent_phrases)

    async def find_company_id(self, inn: str, context) -> str:
        url = f"https://fedresurs.ru/entities?searchString={inn}"
        page = await context.new_page()
        try:
            await page.goto(url, wait_until='networkidle', timeout=90000)
            await asyncio.sleep(2)
            html = await page.content()
            for pattern in self.config['search']['id_patterns']:
                match = re.search(pattern, html)
                if match: return match.group(1)
        finally:
            await page.close()
        return None

    def _extract_bankruptcy_data(self, html: str) -> Tuple[str, bool, List[Dict]]:
        messages = []
        case_number = ""
        has_data = False
        
        for section_name in self.config['sections']['bankruptcy']:
            pos = html.find(section_name)
            if pos != -1:
                section_html = html[pos:pos+10000]
                if 'нет данных' in self._clean_html(section_html[:500]).lower(): continue

                case_match = re.search(self.config['patterns']['case_number'], section_html)
                if case_match:
                    case_number = case_match.group(1)
                    has_data = True

                for match in re.finditer(self.config['patterns']['message'], section_html):
                    msg_num, msg_date = match.groups()
                    ctx = section_html[max(0, match.start()-300):match.end()+300]
                    ctx_clean = self._clean_html(ctx)
                    for phrase in self.key_phrases:
                        if phrase.lower() in ctx_clean.lower():
                            messages.append({'number': msg_num, 'date': msg_date, 'title': phrase, 'is_intent': self._is_intent(phrase)})
                            has_data = True
                            break
                break

        if not has_data: return "Нет данных", False, []

        res = [f"Дело: {case_number}"] if case_number else []
        g1 = [f"{m['number']} от {m['date']} {m['title']}" for m in messages if not m['is_intent']]
        g2 = [f"{m['number']} от {m['date']} {m['title']}" for m in messages if m['is_intent']]
        
        if g1: res.append(f"1) " + "; ".join(g1))
        if g2: res.append(f"2) Намерения: " + "; ".join(g2))
        return "\n".join(res), True, messages

    async def check_bankruptcy(self, inn: str, browser) -> Tuple[str, str]:
        # Создаем контекст с User-Agent, чтобы сайт меньше ругался
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        try:
            company_id = await self.find_company_id(inn, context)
            if not company_id: return "Компания не найдена", ""

            page = await context.new_page()
            await page.goto(f"https://fedresurs.ru/companies/{company_id}", wait_until='networkidle', timeout=90000)
            await asyncio.sleep(2)
            main_html = await page.content()
            await page.close()

            status_text, _, _ = self._extract_bankruptcy_data(main_html)
            return status_text, ""
        except Exception as e:
            if "Timeout" in str(e): return "Ошибка: Таймаут (сайт не ответил)", ""
            return f"Ошибка: {str(e)[:50]}", ""
        finally:
            await context.close()

    def read_companies(self) -> pd.DataFrame:
        if not os.path.exists(self.client_file):
            raise FileNotFoundError(f"Файл {self.client_file} не найден!")
        df = pd.read_excel(self.client_file, header=5)
        df.columns = [str(c).strip() for c in df.columns]
        inn_col = next((c for c in df.columns if 'ИНН' in c), None)
        name_col = next((c for c in df.columns if 'Наименование' in c), None)
        if not inn_col or not name_col: raise ValueError("Колонки ИНН/Наименование не найдены!")
        companies = df[[name_col, inn_col]].dropna().copy()
        companies.columns = ['name', 'inn']
        companies['inn'] = companies['inn'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        return companies

    def generate_html_table(self, df: pd.DataFrame) -> str:
        # Упрощенный, но надежный стиль для HTML
        rows_html = ""
        for i, row in df.iterrows():
            st = row['Банкротство']
            badge = "color: #27ae60;" if "Нет данных" in st else "color: #c0392b;" if "Ошибка" in st else "color: #e67e22;"
            rows_html += f"<tr><td>{i+1}</td><td>{row['ИНН']}</td><td>{row['Наименование']}</td><td style='{badge}'>{st.replace(chr(10), '<br>')}</td></tr>"
        
        return f"""<html><head><meta charset='UTF-8'><style>table{{width:100%;border-collapse:collapse;}}th,td{{border:1px solid #ddd;padding:8px;font-size:12px;}}th{{background:#f2f2f2;}}</style></head>
        <body><h2>Отчет Федресурс ({datetime.now().strftime('%d.%m.%Y')})</h2><table><tr><th>№</th><th>ИНН</th><th>Название</th><th>Статус</th></tr>{rows_html}</table></body></html>"""

    async def run(self):
        companies = self.read_companies()
        print(f"Запуск: {len(companies)} компаний. Батч: {self.batch_size}, Пауза: {self.batch_pause}с")
        
        async with async_playwright() as p:
            # КРИТИЧЕСКИЕ АРГУМЕНТЫ ДЛЯ GITHUB ACTIONS
            browser = await p.chromium.launch(
                headless=self.headless,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-setuid-sandbox"]
            )
            
            for i, (_, row) in enumerate(tqdm(companies.iterrows(), total=len(companies))):
                if i > 0 and i % self.batch_size == 0:
                    print(f" Пауза {self.batch_pause} сек...")
                    await asyncio.sleep(self.batch_pause)

                status, pubs = await self.check_bankruptcy(row['inn'], browser)
                self.results.append({'ИНН': row['inn'], 'Наименование': row['name'], 'Банкротство': status, 'Публикации': pubs})
                await asyncio.sleep(self.delay)

            await browser.close()
        
        df = pd.DataFrame(self.results)
        df.to_excel(self.output_file, index=False)
        with open(self.html_file, 'w', encoding='utf-8') as f:
            f.write(self.generate_html_table(df))
        return df, self.html_file

async def main():
    checker = FedresursBankruptcyChecker(
        client_file="Клиенты_страхование_ТЕСТ.xlsx",
        headless=True, # Обязательно True для сервера
        delay=3,
        batch_size=5,
        batch_pause=15
    )
    await checker.run()

if __name__ == "__main__":
    asyncio.run(main())
