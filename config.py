"""
config.py — настройки проекта
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

# Создаём папки
for d in [LOGS_DIR, EXCEL_DIR, HTML_DIR]:
    os.makedirs(d, exist_ok=True)

# GitHub репозиторий
REPO_URL = "https://github.com/malsm/fedresurs-bankruptcy-monitor"

# Расписание: ПН(0), СР(2), ПТ(4) до 18:00
SCHEDULER_CONFIG = {
    "allowed_days": [0, 2, 4],      # ПН, СР, ПТ
    "max_hour": 18,                  # До 18:00
    "require_user_active": True,     # Только если ПК активен
    "idle_timeout_minutes": 30,      # Простой более 30 мин = неактивен
}