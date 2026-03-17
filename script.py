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
    def __init__(self, client_file: str, headless: bool = False, delay: int = 3, batch_size: int = 5, batch_pause: int = 15):
        self.client_file = client_file
        self.headless = headless
        self.delay = delay  # Пауза между запросами
        self.batch_size = batch_size  # Парсить по 5
        self.batch_pause = batch_pause  # Таймаут после каждых 5
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.output_file = f"fedresurs_{self.today}.xlsx"
        self.html_file = f"fedresurs_{self.today}.html"
        self.results = []

        # Ваши ключевые фразы
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
                'id_patterns': [r'/companies/([a-f0-9-]{36})', r'data-company-id=["\']([a-f0-9-]{36})["\']']
            },
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

    async def find_company_id(self, inn: str, browser_context) -> str:
        url = f"https://fedresurs.ru/entities?searchString={inn}"
        page = await browser_context.new_page()
        try:
            # Увеличен таймаут до 90 сек для стабильности
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
        status = ""
        has_data = False
        
        # Поиск номера дела и статуса в блоке банкротства
        for section_name in self.config['sections']['bankruptcy']:
            pos = html.find(section_name)
            if pos != -1:
                section_html = html[pos:pos+5000] # Берем кусок текста
                if 'нет данных' in section_html.lower()[:500]: continue
                
                case_match = re.search(self.config['patterns']['case_number'], section_html)
                if case_match:
                    case_number = case_match.group(1)
                    has_data = True
                
                # Поиск сообщений
                for match in re.finditer(self.config['patterns']['message'], section_html):
                    msg_num, msg_date = match.groups()
                    for phrase in self.key_phrases:
                        if phrase.lower() in section_html[match.start()-200:match.end()+200].lower():
                            messages.append({
                                'number': msg_num, 'date': msg_date, 
                                'title': phrase, 'is_intent': self._is_intent(phrase)
                            })
                            has_data = True
                            break
        
        if not has_data: return "Нет данных", False, []

        formatted = f"Дело: {case_number}\n" if case_number else ""
        group1 = [f"{m['number']} от {m['date']} {m['title']}" for m in messages if not m['is_intent']]
        group2 = [f"{m['number']} от {m['date']} {m['title']}" for m in messages if m['is_intent']]
        
        if group1: formatted += "1) " + "; ".join(group1) + "\n"
        if group2: formatted += "2) Намерения: " + "; ".join(group2)
        
        return formatted.strip(), True, messages

    async def check_bankruptcy(self, inn: str, browser) -> Tuple[str, str]:
        context = await browser.new_context()
        try:
            company_id = await self.find_company_id(inn, context)
            if not company_id: return "Компания не найдена", ""

            page = await context.new_page()
            # Увеличен таймаут до 90 сек
            await page.goto(f"https://fedresurs.ru/companies/{company_id}", wait_until='networkidle', timeout=90000)
            await asyncio.sleep(2)
            main_html = await page.content()
            await page.close()

            status_text, has_data, _ = self._extract_bankruptcy_data(main_html)
            return status_text, ""
        except Exception as e:
            err_msg = str(e)
            if "Timeout" in err_msg:
                return "Ошибка: Сайт не ответил (Таймаут)", ""
            return f"Ошибка: {err_msg[:50]}", ""
        finally:
            await context.close()

    def read_companies(self) -> pd.DataFrame:
        # Читаем с 6-й строки (header=5)
        df = pd.read_excel(self.client_file, header=5)
        df.columns = [str(c).strip() for c in df.columns]
        inn_col = next((c for c in df.columns if 'ИНН' in c), None)
        name_col = next((c for c in df.columns if 'Наименование' in c), None)
        
        if not inn_col or not name_col:
            raise ValueError(f"Колонки ИНН/Наименование не найдены. Доступны: {df.columns.tolist()}")
            
        companies = df[[name_col, inn_col]].dropna().copy()
        companies.columns = ['name', 'inn']
        companies['inn'] = companies['inn'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        return companies

    def generate_html_table(self, df: pd.DataFrame) -> str:
        # Здесь остается ваш красивый HTML шаблон из примера
        # (Для краткости я не дублирую весь CSS, он идентичен вашему)
        return super_cool_html_template_logic(df, self.today) # Вызывается ваш метод

    async def run(self):
        companies = self.read_companies()
        print(f"Загружено: {len(companies)} компаний. Начинаем парсинг по {self.batch_size} шт...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            
            for i, (_, row) in enumerate(tqdm(companies.iterrows(), total=len(companies))):
                # Логика батчей: каждые N записей делаем большую паузу
                if i > 0 and i % self.batch_size == 0:
                    print(f"\nПауза {self.batch_pause} сек. для стабильности...")
                    await asyncio.sleep(self.batch_pause)

                status, pubs = await self.check_bankruptcy(row['inn'], browser)
                self.results.append({
                    'ИНН': row['inn'],
                    'Наименование': row['name'],
                    'Банкротство': status,
                    'Публикации': pubs,
                    'Дата проверки': datetime.now().strftime('%d.%m.%Y %H:%M')
                })
                await asyncio.sleep(self.delay)

            await browser.close()
        
        df = pd.DataFrame(self.results)
        df.to_excel(self.output_file, index=False)
        # Генерация HTML (используйте ваш метод generate_html_table)
        html_content = self.generate_html_table(df)
        with open(self.html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        return df, self.html_file

# Вставьте сюда ваш метод generate_html_table целиком из вашего кода

async def main():
    checker = FedresursBankruptcyChecker(
        client_file="Клиенты_страхование_ТЕСТ.xlsx",
        headless=False, # Можно поставить True для скорости
        delay=3,
        batch_size=5,
        batch_pause=15
    )
    results, html_file = await checker.run()
    print(f"Готово! Excel: {checker.output_file}")
    webbrowser.open(f"file://{os.path.abspath(html_file)}")

if __name__ == "__main__":
    import webbrowser
    asyncio.run(main())
