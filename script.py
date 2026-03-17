import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
import nest_asyncio
import os
import re
import json
import pytz
from tqdm import tqdm
from typing import List, Dict, Tuple

# Разрешаем вложенные циклы событий для Jupyter/Облака
nest_asyncio.apply()

class FedresursBankruptcyChecker:
    def __init__(self, client_file: str, headless: bool = True, delay: int = 3):
        self.client_file = client_file
        self.headless = headless
        self.delay = delay
        
        # Настройка временной зоны Москвы
        self.msk_tz = pytz.timezone('Europe/Moscow')
        self.now = datetime.now(self.msk_tz)
        self.today_str = self.now.strftime('%Y-%m-%d')
        
        # Пути к файлам
        self.output_file = f"reports/fedresurs_{self.today_str}.xlsx"
        self.history_file = "history.json"
        self.results = []

        # Твои ключевые фразы
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
        messages, case_number, status, has_data = [], "", "", False
        company_status = self._extract_company_status(html)
        if company_status and "несостоятельным" in company_status.lower():
            status, has_data = company_status, True

        for section_name in self.config['sections']['bankruptcy']:
            pos = html.find(section_name)
            if pos != -1:
                end_pos = len(html)
                for next_sec in self.config['sections']['next_sections']:
                    next_pos = html.find(next_sec, pos + len(section_name))
                    if next_pos != -1 and next_pos < end_pos: end_pos = next_pos
                
                section_html = html[pos:end_pos]
                if 'нет данных' in self._clean_html(section_html).lower() and not has_data:
                    return "Нет данных", False, []

                case_match = re.search(self.config['patterns']['case_number'], section_html)
                if case_match: case_number, has_data = case_match.group(1), True

                if not status:
                    for p in [r'конкурсное производство', r'введено наблюдение', r'внешнее управление']:
                        if re.search(p, section_html, re.I):
                            st_m = re.search(r'([^<>\n]{0,70}(?:производство|наблюдение|управление)[^<>\n]{0,70})', section_html, re.I)
                            if st_m: status, has_data = self._clean_html(st_m.group(1)), True
                            break

                for match in re.finditer(self.config['patterns']['message'], section_html):
                    m_num, m_date = match.groups()
                    ctx = self._clean_html(section_html[max(0, match.start()-250):min(len(section_html), match.end()+250)])
                    for ph in self.key_phrases:
                        if ph.lower() in ctx.lower():
                            messages.append({'number': m_num, 'date': m_date, 'title': ph, 'is_intent': self._is_intent(ph)})
                            has_data = True
                            break
                break

        if not has_data: return "Нет данных", False, []
        
        res = ["Сведения о банкротстве:"]
        g1 = [case_number, status] + [f"{m['number']} от {m['date']} {m['title']}" for m in messages if not m['is_intent']]
        if any(g1): res.append(f"1) {' '.join(filter(None, g1))}")
        
        intents = [m for m in messages if m['is_intent']]
        if intents:
            g2 = ["Сообщения о намерении"] + [f"{m['number']} от {m['date']} {m['title']}" for m in intents]
            res.append(f"2) {' '.join(g2)}")
        return '\n'.join(res), True, messages

    async def _load_all_publications(self, page) -> None:
        for _ in range(10):
            try:
                btn = await page.wait_for_selector('div.more_btn:has-text("Загрузить еще")', timeout=2000)
                if btn and await btn.is_visible():
                    await btn.click()
                    await asyncio.sleep(1.5)
                else: break
            except: break

    async def _extract_publications_from_page(self, url: str, browser) -> List[Dict]:
        pubs = []
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until='networkidle', timeout=60000)
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
                        msg_info = self._extract_message_info(await link_el.inner_text())
                        if msg_info['number']:
                            pubs.append({'number': msg_info['number'], 'date': msg_info['date'], 'title': matched, 'is_intent': self._is_intent(matched)})
            pubs.sort(key=lambda x: datetime.strptime(x['date'], '%d.%m.%Y'), reverse=True)
        finally: await page.close()
        return pubs

    async def _extract_trades_data(self, url: str, browser) -> List[Dict]:
        trades = []
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until='networkidle', timeout=60000)
            if await page.query_selector('div.info-header:has-text("Торги")'):
                cards = await page.query_selector_all('entity-card-biddings-block-bidding-card')
                for card in cards:
                    num_link = await card.query_selector('a.number-link')
                    if num_link: trades.append({'number': await num_link.inner_text()})
        except: pass
        finally: await page.close()
        return trades

    async def find_company_id(self, inn: str, browser) -> str:
        page = await browser.new_page()
        try:
            await page.goto(f"https://fedresurs.ru/entities?searchString={inn}", wait_until='networkidle', timeout=60000)
            html = await page.content()
            for pattern in self.config['search']['id_patterns']:
                match = re.search(pattern, html)
                if match: return match.group(1)
        finally: await page.close()
        return None

    async def check_bankruptcy(self, inn: str, browser) -> Tuple[str, str]:
        try:
            c_id = await self.find_company_id(inn, browser)
            if not c_id: return "Компания не найдена", ""
            
            main_url = f"https://fedresurs.ru/companies/{c_id}"
            page = await browser.new_page()
            await page.goto(main_url, wait_until='networkidle', timeout=60000)
            main_html = await page.content()
            await page.close()

            b_status, has_d, b_msgs = self._extract_bankruptcy_data(main_html)
            trades = await self._extract_trades_data(main_url, browser)
            pubs = await self._extract_publications_from_page(f"{main_url}/publications", browser)

            pub_lines = [f"- {p['number']} от {p['date']} {p['title']}" for p in pubs]
            if (pubs or trades) and not has_d:
                st = ["Сведения о банкротстве:"]
                if trades: st.append("Торги присутствуют")
                if pubs: st.append("Публикации:"); st.extend(pub_lines)
                return "\n".join(st), ""

            return b_status if has_d or trades else "Нет данных", "\n".join(pub_lines)
        except Exception as e: return f"Ошибка: {str(e)[:50]}", ""

    def generate_dashboard(self, history: Dict):
        rows = ""
        for date in reversed(sorted(history.keys())):
            item = history[date]
            badge = "bg-danger" if "Есть изменения" in item['status'] else "bg-success"
            rows += f"""
            <tr>
                <td><strong>{date}</strong></td>
                <td><span class="badge {badge}">{item['status']}</span></td>
                <td><a href="{item['file']}" class="btn btn-sm btn-outline-light">📥 Excel</a></td>
            </tr>"""
        
        html = f"""
        <!DOCTYPE html><html><head><meta charset="UTF-8">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>body{{background:#121212;color:#e0e0e0;padding:50px 20px;}} .card{{background:#1e1e1e;border:1px solid #333;}} .table{{color:#e0e0e0;}}</style>
        <title>Консоль Мониторинга</title></head><body><div class="container" style="max-width:800px;">
        <h2 class="mb-4 text-center">🚜 Мониторинг Федресурс (Клиенты Х)</h2>
        <div class="card shadow"><table class="table table-hover mb-0"><thead class="table-dark">
        <tr><th>Дата</th><th>Статус изменений</th><th>Отчет</th></tr></thead><tbody>{rows}</tbody></table>
        </div></div></body></html>"""
        with open("index.html", "w", encoding="utf-8") as f: f.write(html)

    async def run(self):
        if not os.path.exists('reports'): os.makedirs('reports')
        df_input = pd.read_excel(self.client_file, header=5)
        
        name_col = next(c for c in df_input.columns if 'Наименование' in str(c))
        inn_col = next(c for c in df_input.columns if 'ИНН' in str(c))
        
        companies = df_input[[name_col, inn_col]].dropna()
        companies.columns = ['name', 'inn']

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            for _, row in tqdm(companies.iterrows(), total=len(companies)):
                status, pubs = await self.check_bankruptcy(str(row['inn']).strip(), browser)
                self.results.append({'ИНН': row['inn'], 'Наименование': row['name'], 'Банкротство': status, 'Публикации': pubs})
                await asyncio.sleep(self.delay)
            await browser.close()

        df_today = pd.DataFrame(self.results)
        df_today.to_excel(self.output_file, index=False)

        if os.path.exists(self.history_file):
            with open(self.history_file, 'r', encoding='utf-8') as f:
                history = json.load(f)
        else:
            history = {}

        dates = sorted(history.keys())
        change_status = "Базовый"
        if dates:
            last_data = history[dates[-1]]['data']
            change_status = "Нет изменений" if [r['Банкротство'] for r in last_data] == [r['Банкротство'] for r in self.results] else "⚠️ Есть изменения"

        history[self.today_str] = {'status': change_status, 'file': self.output_file, 'data': self.results}
        with open(self.history_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False)
        
        self.generate_dashboard(history)
        print(f"Готово! Статус: {change_status}")

if __name__ == "__main__":
    checker = FedresursBankruptcyChecker(client_file="Клиенты_страхование_ТЕСТ.xlsx")
    asyncio.run(checker.run())
