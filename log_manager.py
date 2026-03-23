"""
log_manager.py — история парсингов в SQLite
"""
import sqlite3
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from config import DB_PATH, EXCEL_DIR, HTML_DIR, MOSCOW_TZ


class LogManager:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS parsing_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_date TEXT NOT NULL,
                    run_timestamp TEXT NOT NULL,
                    excel_path TEXT,
                    html_path TEXT,
                    total_companies INTEGER,
                    no_data_count INTEGER,
                    has_signs_count INTEGER,
                    errors_count INTEGER,
                    status TEXT,
                    companies_json TEXT
                )
            """)
            conn.commit()
    
    def save_run(self, excel_path: str, html_path: str, df: pd.DataFrame, 
                 status: str = "success") -> int:
        stats = {
            "no_data": sum(s in ["Нет данных", "Компания не найдена"] for s in df['Банкротство']),
            "has_signs": sum(s not in ["Нет данных", "Компания не найдена"] and not s.startswith("Ошибка") for s in df['Банкротство']),
            "errors": sum(s.startswith("Ошибка") for s in df['Банкротство'])
        }
        companies_data = df[['ИНН', 'Наименование', 'Банкротство']].to_dict('records')
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO parsing_logs 
                (run_date, run_timestamp, excel_path, html_path, total_companies,
                 no_data_count, has_signs_count, errors_count, status, companies_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d'),
                datetime.now(MOSCOW_TZ).isoformat(),
                os.path.basename(excel_path),
                os.path.basename(html_path),
                len(df),
                stats["no_data"],
                stats["has_signs"],
                stats["errors"],
                status,
                json.dumps(companies_data, ensure_ascii=False)
            ))
            conn.commit()
            return cursor.lastrowid
    
    def get_history(self, days: int = 30) -> pd.DataFrame:
        cutoff = (datetime.now(MOSCOW_TZ) - timedelta(days=days)).strftime('%Y-%m-%d')
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query("""
                SELECT id, run_date, run_timestamp, excel_path, html_path,
                       total_companies, no_data_count, has_signs_count, errors_count, status
                FROM parsing_logs 
                WHERE run_date >= ? 
                ORDER BY run_date DESC, run_timestamp DESC
            """, conn, params=(cutoff,))
        return df
    
    def get_run_by_date(self, date: str) -> Optional[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM parsing_logs 
                WHERE run_date = ? AND status = 'success'
                ORDER BY run_timestamp DESC LIMIT 1
            """, (date,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None
    
    def get_run_by_id(self, run_id: int) -> Optional[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM parsing_logs WHERE id = ?", (run_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None
    
    def get_companies_data(self, run_id: int) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT companies_json FROM parsing_logs WHERE id = ?", (run_id,))
            row = cursor.fetchone()
            if row and row[0]:
                return json.loads(row[0])
        return []
    
    def compare_runs(self, today_id: int, yesterday_id: int) -> List[Dict]:
        """Сравнивает два запуска и возвращает изменения"""
        today_data = {c['ИНН']: c for c in self.get_companies_data(today_id)}
        yesterday_data = {c['ИНН']: c for c in self.get_companies_data(yesterday_id)}
        changes = []
        
        for inn, data in today_data.items():
            if inn not in yesterday_data:
                changes.append({
                    'type': 'new_company',
                    'inn': inn,
                    'name': data['Наименование'],
                    'message': f"➕ Новая компания: {data['Наименование']}"
                })
            elif data['Банкротство'] != yesterday_data[inn]['Банкротство']:
                if yesterday_data[inn]['Банкротство'] == "Нет данных" and data['Банкротство'] != "Нет данных":
                    changes.append({
                        'type': 'new_bankruptcy',
                        'inn': inn,
                        'name': data['Наименование'],
                        'old_status': yesterday_data[inn]['Банкротство'],
                        'new_status': data['Банкротство'],
                        'message': f"⚠️ НОВЫЕ ПРИЗНАКИ: {data['Наименование']}"
                    })
                else:
                    changes.append({
                        'type': 'status_changed',
                        'inn': inn,
                        'name': data['Наименование'],
                        'message': f"🔄 Изменился статус: {data['Наименование']}"
                    })
        return changes
    
    def get_status_badge(self, run_id: int) -> str:
        """Возвращает статус сравнения с предыдущим запуском"""
        current = self.get_run_by_id(run_id)
        if not current:
            return "ℹ️ Нет данных"
        
        # Ищем предыдущий успешный запуск
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT id FROM parsing_logs 
                WHERE id < ? AND status = 'success'
                ORDER BY id DESC LIMIT 1
            """, (run_id,))
            prev = cursor.fetchone()
        
        if not prev:
            return "✅ Первый запуск"
        
        changes = self.compare_runs(run_id, prev['id'])
        bankruptcy_changes = [c for c in changes if c['type'] == 'new_bankruptcy']
        
        if bankruptcy_changes:
            return f"⚠️ Изменений: {len(bankruptcy_changes)}"
        elif changes:
            return f"🔄 Изменений: {len(changes)}"
        else:
            return "✅ Без изменений"