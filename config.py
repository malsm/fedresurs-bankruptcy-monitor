"""
Настройки проекта
"""
from datetime import timezone, timedelta
import os

# Часовой пояс Москвы
MOSCOW_TZ = timezone(timedelta(hours=3))

# Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()
LOGS_DIR = os.path.join(BASE_DIR, "logs")
EXCEL_DIR = os.path.join(LOGS_DIR, "excel")
HTML_DIR = os.path.join(LOGS_DIR, "html")
DB_PATH = os.path.join(LOGS_DIR, "database.db")

# Создаем папки
for d in [LOGS_DIR, EXCEL_DIR, HTML_DIR]:
    os.makedirs(d, exist_ok=True)

# Настройки парсинга
PARSER_CONFIG = {
    "client_file": "Клиенты_страхование_ТЕСТ.xlsx",
    "headless": True,
    "delay": 3,
    "batch_size": 5,
    "batch_delay": 30,
}

# Настройки дашборда
DASHBOARD_CONFIG = {
    "title": " Мониторинг банкротств (Федресурс)",
    "timezone": "Europe/Moscow",
    "max_history_days": 90,
}
