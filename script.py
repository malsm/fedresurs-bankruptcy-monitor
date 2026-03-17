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
        self.delay = delay            # Пауза между отдельными компаниями
        self.batch_size = batch_size  # Кол-во компаний в одной пачке
        self.batch_pause = batch_pause # Пауза после каждой пачки (в секундах)
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.output_file = f"fedresurs_{self.today}.xlsx"
        self.html_file = f"fedresurs_{self.today}.html"
        self.results = []

        # Ключевые фразы (ваши оригинальные)
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
            # Увеличен таймаут до 90 секунд для стабильности в GitHub Actions
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
                # Берем кусок кода вокруг раздела для поиска данных
                section_html = html[pos:pos+10000]
                if 'нет данных' in self._clean_html(section_html[:500]).lower():
                    continue

                case_match = re.search(self.config['patterns']['case_number'], section_html)
                if case_match:
                    case_number = case_match.group(1)
                    has_data = True

                for match in re.finditer(self.config['patterns']['message'], section_html):
                    msg_num, msg_date = match.groups()
                    context = section_html[max(0, match.start()-300):match.end()+300]
                    context_clean = self._clean_html(context)
                    
                    for phrase in self.key_phrases:
                        if phrase.lower() in context_clean.lower():
                            messages.append({
                                'number': msg_num, 'date': msg_date, 
                                'title': phrase, 'is_intent': self._is_intent(phrase)
                            })
                            has_data = True
                            break
                break

        if not has_data:
            return "Нет данных", False, []

        res = []
        if case_number: res.append(f"Дело: {case_number}")
        
        g1 = [f"{m['number']} от {m['date']} {m['title']}" for m in messages if not m['is_intent']]
        g2 = [f"{m['number']} от {m['date']} {m['title']}" for m in messages if m['is_intent']]
        
        if g1: res.append(f"1) " + "; ".join(g1))
        if g2: res.append(f"2) Намерения: " + "; ".join(g2))
        
        return "\n".join(res), True, messages

    async def check_bankruptcy(self, inn: str, browser) -> Tuple[str, str]:
        # Создаем чистый контекст для каждого запроса (помогает от блокировок)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        try:
            company_id = await self.find_company_id(inn, context)
            if not company_id:
                return "Компания не найдена", ""

            page = await context.new_page()
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
        df = pd.read_excel(self.client_file, header=5)
        # Очистка названий колонок от пробелов
        df.columns = [str(c).strip() for c in df.columns]
        
        inn_col = next((c for c in df.columns if 'ИНН' in c), None)
        name_col = next((c for c in df.columns if 'Наименование' in c), None)
        
        if not inn_col or not name_col:
            raise ValueError(f"Не найдены колонки ИНН/Наименование. Проверьте Excel файл.")
            
        companies = df[[name_col, inn_col]].dropna().copy()
        companies.columns = ['name', 'inn']
        # Исправляем формат ИНН (убираем .0 если Excel прочитал как число)
        companies['inn'] = companies['inn'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        return companies

    def generate_html_table(self, df: pd.DataFrame) -> str:
        # Ваш оригинальный блок генерации HTML (оставлен без изменений для сохранения стиля)
        total = len(df)
        no_bankruptcy = len(df[(df['Банкротство'] == "Нет данных") | (df['Банкротство'] == "Компания не найдена")])
        errors = len(df[df['Банкротство'].str.contains("Ошибка", na=False)])
        has_signs = total - no_bankruptcy - errors

        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Отчет Федресурс</title>
            <style>
                body {{ font-family: sans-serif; background: #f4f7f6; padding: 20px; }}
                .container {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                th {{ background: #2c3e50; color: white; padding: 12px; text-align: left; }}
                td {{ padding: 12px; border-bottom: 1px solid #eee; vertical-align: top; font-size: 13px; }}
                .status-badge {{ padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }}
                .badge-success {{ background: #e8f8f0; color: #27ae60; }}
                .badge-warning {{ background: #fef5e7; color: #e67e22; }}
                .badge-error {{ background: #f9ebea; color: #c0392b; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>Результаты проверки на {datetime.now().strftime('%d.%m.%Y %H:%M')}</h2>
                <p>Всего: {total} | Ошибок: {errors} | Есть признаки: {has_signs}</p>
                <table>
                    <tr><th>№</th><th>ИНН</th><th>Наименование</th><th>Статус</th></tr>
        """
        for i, row in df.iterrows():
            st = row['Банкротство']
            cls = "badge-success" if "Нет данных" in st or "не найдена" in st else "badge-error" if "Ошибка" in st else "badge-warning"
            txt = "OK" if cls == "badge-success" else "ОШИБКА" if cls == "badge-error" else "ВНИМАНИЕ"
            
            html_template += f"""
            <tr>
                <td>{i+1}</td>
                <td>{row['ИНН']}</td>
                <td>{row['Наименование']}</td>
                <td><span class="status-badge {cls}">{txt}</span><br>{st.replace(chr(10), '<br>')}</td>
            </tr>"""
            
        html_template += "</table></div></body></html>"
        return html_template

    async def run(self):
        companies = self.read_companies()
        print(f"\nЗагружено {len(companies)} компаний. Работаем по схеме {self.batch_size} через {self.batch_pause} сек.")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            
            for i, (_, row) in enumerate(tqdm(companies.iterrows(), total=len(companies), desc="Обработка")):
                # ЛОГИКА БАТЧЕЙ (5 через 15)
                if i > 0 and i % self.batch_size == 0:
                    print(f"\nВыполнено {i} шт. Пауза {self.batch_pause} сек...")
                    await asyncio.sleep(self.batch_pause)

                status, pubs = await self.check_bankruptcy(row['inn'], browser)
                self.results.append({
                    'ИНН': row['inn'],
                    'Наименование': row['name'],
                    'Банкротство': status,
                    'Публикации': pubs
                })
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
        headless=True,  # ОБЯЗАТЕЛЬНО True для GitHub Actions
        delay=3,
        batch_size=5,
        batch_pause=15
    )
    results, html_file = await checker.run()
    print(f"\nГотово! Результаты сохранены в {html_file}")

if __name__ == "__main__":
    asyncio.run(main())
