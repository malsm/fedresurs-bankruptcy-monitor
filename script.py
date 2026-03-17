import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
import nest_asyncio
import os
import re
from tqdm import tqdm
import random

nest_asyncio.apply()

class FedresursBankruptcyChecker:
    def __init__(self, client_file: str, headless: bool = True):
        self.client_file = client_file
        self.headless = headless
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.output_file = f"fedresurs_{self.today}.xlsx"
        self.html_file = f"fedresurs_{self.today}.html"
        self.results = []

        # Ваши ключевые фразы (без изменений)
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

    def _clean_html(self, text: str) -> str:
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _extract_bankruptcy_data(self, html: str):
        # Ваша оригинальная логика парсинга
        found_messages = []
        html_clean = self._clean_html(html)
        
        for phrase in self.key_phrases:
            if phrase.lower() in html_clean.lower():
                found_messages.append(phrase)
        
        if not found_messages:
            return "Нет изменений"
        
        return "Есть изменения: " + "; ".join(list(set(found_messages)))

    async def check_bankruptcy(self, inn: str, browser):
        # Эмуляция реального пользователя
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        try:
            # Поиск компании по ИНН
            url = f"https://fedresurs.ru/entities?searchString={inn}"
            # Ждем загрузки контента, а не всей сети (так надежнее при таймаутах)
            await page.goto(url, wait_until='domcontentloaded', timeout=90000)
            
            # Небольшая пауза, чтобы JS успел отрисовать таблицу
            await asyncio.sleep(5) 
            
            content = await page.content()
            
            # Проверка на блокировку (если страница пустая или содержит капчу)
            if "captcha" in content.lower() or len(content) < 1000:
                return "Ошибка: Блокировка или пустая страница"

            status = self._extract_bankruptcy_data(content)
            return status
        except Exception as e:
            return f"Ошибка таймаута: {str(e)[:30]}"
        finally:
            await page.close()
            await context.close()

    def read_companies(self):
        # Чтение вашего файла
        df = pd.read_excel(self.client_file, header=5)
        df.columns = [str(c).strip() for c in df.columns]
        inn_col = next((c for c in df.columns if 'ИНН' in c), None)
        name_col = next((c for c in df.columns if 'Наименование' in c), None)
        
        companies = df[[name_col, inn_col]].dropna().copy()
        companies.columns = ['name', 'inn']
        companies['inn'] = companies['inn'].astype(str).str.replace(r'\.0$', '', regex=True).strip()
        return companies

    async def run(self):
        companies = self.read_companies()
        print(f"Начинаем проверку {len(companies)} компаний...")

        async with async_playwright() as p:
            # КРИТИЧНО: Флаги маскировки для сервера
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox", 
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled"
                ]
            )
            
            for i, row in tqdm(companies.iterrows(), total=len(companies)):
                # Ваша логика 5 через 15
                if i > 0 and i % 5 == 0:
                    await asyncio.sleep(15)

                status = await self.check_bankruptcy(row['inn'], browser)
                self.results.append({
                    'Дата проверки': datetime.now().strftime('%d.%m.%Y'),
                    'Наименование': row['name'],
                    'ИНН': row['inn'],
                    'Статус': status
                })
                # Небольшая случайная пауза между запросами
                await asyncio.sleep(random.uniform(2, 4))

            await browser.close()
        
        # Сохранение
        df_res = pd.DataFrame(self.results)
        df_res.to_excel(self.output_file, index=False)
        
        # Простой HTML отчет
        df_res.to_html(self.html_file, index=False)
        print(f"Готово! Файл сохранен: {self.output_file}")

async def main():
    # Убедитесь, что имя файла совпадает с тем, что лежит в GitHub
    checker = FedresursBankruptcyChecker("Клиенты_страхование_ТЕСТ.xlsx", headless=True)
    await checker.run()

if __name__ == "__main__":
    asyncio.run(main())
