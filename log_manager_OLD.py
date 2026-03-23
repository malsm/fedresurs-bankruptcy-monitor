"""
Менеджер логов - хранит историю запусков в SQLite
"""
import sqlite3
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from typing import Optional, Dict
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
                    error_message TEXT
                )
            """)
            conn.commit()
    
    def save_run(self, excel_path: str, html_path: str, df: pd.DataFrame, 
                 status: str = "success", error_msg: str = None) -> int:
        stats = {
            "no_data": sum(s in ["Нет данных", "Компания не найдена"] for s in df['Банкротство']),
            "has_signs": sum(s not in ["Нет данных", "Компания не найдена"] and not s.startswith("Ошибка") for s in df['Банкротство']),
            "errors": sum(s.startswith("Ошибка") for s in df['Банкротство'])
        }
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO parsing_logs 
                (run_date, run_timestamp, excel_path, html_path, total_companies,
                 no_data_count, has_signs_count, errors_count, status, error_message)
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
                error_msg
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
    
    def get_run_details(self, run_id: int) -> Optional[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT * FROM parsing_logs WHERE id = ?", (run_id,))
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
        return None
    
    def get_file_paths(self, run_id: int) -> Dict[str, str]:
        details = self.get_run_details(run_id)
        if not details:
            return {}
        return {
            'excel': os.path.join(EXCEL_DIR, details['excel_path']),
            'html': os.path.join(HTML_DIR, details['html_path'])
        }
    
    def cleanup_old_logs(self, keep_days: int = 90):
        cutoff = (datetime.now(MOSCOW_TZ) - timedelta(days=keep_days)).strftime('%Y-%m-%d')
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM parsing_logs WHERE run_date < ?", (cutoff,))
            conn.commit()
    
    def cleanup_old_files(self, keep_days: int = 90):
        cutoff = (datetime.now(MOSCOW_TZ) - timedelta(days=keep_days)).strftime('%Y-%m-%d')
        for folder in [EXCEL_DIR, HTML_DIR]:
            if os.path.exists(folder):
                for file in os.listdir(folder):
                    if file < cutoff:
                        os.remove(os.path.join(folder, file))
        self.cleanup_old_logs(keep_days)
