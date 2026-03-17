import asyncio
import pandas as pd
import numpy as np
from playwright.async_api import async_playwright
from datetime import datetime
import pytz
import time
import os

class FedresursBankruptcyChecker:
    def __init__(self, input_file):
        self.input_file = input_file
        self.moscow_tz = pytz.timezone('Europe/Moscow')
        self.now = datetime.now(self.moscow_tz)
        self.results = []

    async def get_bankruptcy_info(self, browser_context, inn):
        """Парсинг данных по конкретному ИНН через Fedresurs"""
        page = await browser_context.new_page()
        try:
            # Здесь должна быть логика навигации и поиска на сайте
            # Для примера имитируем задержку и поиск
            await page.goto(f"https://fedresurs.ru/search/entity?code={inn}", wait_until="networkidle", timeout=60000)
            
            # Логика извлечения данных (зависит от структуры сайта на 2026 год)
            # Вставьте сюда ваш селектор для проверки статуса
            status = "Нет данных" # Заглушка
            publications = "Публикаций не найдено" # Заглушка
            
            return status, publications
        except Exception as e:
            return f"Ошибка: {str(e)}", ""
        finally:
            await page.close()

    def format_bankruptcy_status(self, status_text: str) -> str:
        """Безопасная обработка текста для HTML (исправляет SyntaxError)"""
        if not status_text or "Ошибка" in status_text or "не найдена" in status_text:
            return status_text
        
        safe_text = status_text.replace("<", "&lt;").replace(">", "&gt;")
        html_ready_text = safe_text.replace("\n", "<br>")
        return f'<div class="formatted-status">{html_ready_text}</div>'

    def generate_html_table(self, df: pd.DataFrame) -> str:
        """Создание финального HTML-файла с отчетом"""
        rows_html = ""
        for idx, row in df.iterrows():
            status = str(row['Банкротство'])
            
            if status == "Нет данных" or "не найдена" in status:
                badge_cls, badge_txt, display_st = "badge-success", "Нет данных", ""
            elif "Ошибка" in status:
                badge_cls, badge_txt, display_st = "badge-error", "Ошибка", status
            else:
                badge_cls, badge_txt, display_st = "badge-warning", "Есть признаки", status

            formatted_status = self.format_bankruptcy_status(display_st)
            formatted_pubs = str(row['Публикации']).replace('\n', '<br>')

            rows_html += f"""
            <tr>
                <td>{idx + 1}</td>
                <td class="inn-cell">{row['ИНН']}</td>
                <td class="name-cell"><strong>{row['Наименование']}</strong></td>
                <td>
                    <span class="status-badge {badge_cls}">{badge_txt}</span>
                    {formatted_status}
                </td>
                <td class="publications-cell">{formatted_pubs}</td>
            </tr>
            """

        return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Мониторинг Федресурс {self.now.strftime('%d.%m.%Y')}</title>
    <style>
        body {{ font-family: -apple-system, system-ui, sans-serif; background: #f0f2f5; padding: 20px; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }}
        h1 {{ color: #1c1e21; font-size: 22px; border-bottom: 2px solid #eee; padding-bottom: 10px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th {{ background: #f8f9fa; padding: 12px; text-align: left; font-size: 12px; color: #65676b; border-bottom: 2px solid #dee2e6; }}
        td {{ padding: 12px; border-bottom: 1px solid #eee; vertical-align: top; font-size: 13px; }}
        .status-badge {{ padding: 4px 10px; border-radius: 10px; font-size: 11px; font-weight: bold; }}
        .badge-success {{ background: #e7f3ff; color: #1877f2; }}
        .badge-warning {{ background: #fff3cd; color: #856404; }}
        .badge-error {{ background: #f8d7da; color: #721c24; }}
        .inn-cell {{ font-family: monospace; font-weight: bold; }}
        .formatted-status {{ margin-top: 8px; font-size: 11px; color: #606770; line-height: 1.4; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Отчет по банкротствам (Федресурс)</h1>
        <p>Дата формирования: {self.now.strftime('%d.%m.%Y %H:%M')} (МСК)</p>
        <table>
            <thead>
                <tr>
                    <th style="width:30px">№</th><th style="width:120px">ИНН</th>
                    <th style="width:250px">Компания</th><th>Статус</th><th>Публикации</th>
                </tr>
            </thead>
            <tbody>{rows_html}</tbody>
        </table>
    </div>
</body>
</html>
"""

    async def run(self):
        # Загрузка данных
        df_input = pd.read_excel(self.input_file)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(viewport={'width': 1920, 'height': 1080})

            # Логика обработки батчами по 5 штук
            for i in range(0, len(df_input), 5):
                batch = df_input.iloc[i:i+5]
                print(f"Обработка группы {i//5 + 1}...")

                for idx, row in batch.iterrows():
                    inn = str(row['ИНН']).strip()
                    name = row['Наименование']
                    
                    status, pubs = await self.get_bankruptcy_info(context, inn)
                    
                    self.results.append({
                        'ИНН': inn,
                        'Наименование': name,
                        'Банкротство': status,
                        'Публикации': pubs,
                        'Дата проверки': self.now.strftime('%d.%m.%Y %H:%M')
                    })

                # Таймаут между батчами
                if i + 5 < len(df_input):
                    print("Ожидание 5 секунд перед следующим батчем...")
                    await asyncio.sleep(5)

            await browser.close()

        # Сохранение результатов
        df_res = pd.DataFrame(self.results)
        
        # 1. Экспорт в Excel
        df_res.to_excel("results_parsing.xlsx", index=False)
        
        # 2. Экспорт в HTML
        html_content = self.generate_html_table(df_res)
        with open("index.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        
        print("Парсинг завершен. Файлы results_parsing.xlsx и index.html созданы.")

if __name__ == "__main__":
    # Убедитесь, что файл Клиенты_страхование_ТЕСТ.xlsx лежит в корне
    checker = FedresursBankruptcyChecker("Клиенты_страхование_ТЕСТ.xlsx")
    asyncio.run(checker.run())
