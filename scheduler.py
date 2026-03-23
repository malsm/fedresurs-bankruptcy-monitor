"""
scheduler.py — запуск парсера + отправка в GitHub
Совместим с parser.py: метод run() возвращает 2 значения
"""
import asyncio
import logging
import subprocess
import sys
import os
from datetime import datetime, timedelta
from parser import FedresursBankruptcyChecker
from log_manager import LogManager
from config import MOSCOW_TZ, SCHEDULER_CONFIG, REPO_URL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler('logs/scheduler.log', encoding='utf-8')]
)
logger = logging.getLogger(__name__)


def is_allowed_time() -> bool:
    now = datetime.now(MOSCOW_TZ)
    if now.weekday() not in SCHEDULER_CONFIG['allowed_days']:
        logger.info(f"Не ПН/СР/ПТ ({now.strftime('%A')}), пропускаем")
        return False
    if now.hour >= SCHEDULER_CONFIG['max_hour']:
        logger.info(f"После {SCHEDULER_CONFIG['max_hour']}:00, пропускаем")
        return False
    return True


def is_user_active() -> bool:
    if not SCHEDULER_CONFIG.get('require_user_active', True):
        return True
    try:
        result = subprocess.run(
            ['powershell', '-Command', '(Get-LastInputInfo).TimeSinceLastInput.TotalMinutes'],
            capture_output=True, text=True, timeout=10
        )
        idle_minutes = float(result.stdout.strip())
        timeout = SCHEDULER_CONFIG.get('idle_timeout_minutes', 30)
        if idle_minutes < timeout:
            logger.info(f"ПК активен (простой: {idle_minutes:.1f} мин)")
            return True
        else:
            logger.info(f"ПК неактивен (простой: {idle_minutes:.1f} мин)")
            return False
    except Exception as e:
        logger.warning(f"Не удалось проверить активность: {e}")
        return True


def push_to_github() -> bool:
    try:
        subprocess.run(['git', 'config', 'user.email', 'action@github.com'], check=True, capture_output=True)
        subprocess.run(['git', 'config', 'user.name', 'GitHub Action'], check=True, capture_output=True)
        subprocess.run(['git', 'add', 'logs/'], check=True, capture_output=True)
        result = subprocess.run(['git', 'diff', '--cached', '--quiet'], capture_output=True)
        if result.returncode != 0:
            timestamp = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M MSK')
            subprocess.run(['git', 'commit', '-m', f'Отчёт {timestamp}'], check=True, capture_output=True)
            subprocess.run(['git', 'push'], check=True, capture_output=True, timeout=60)
            logger.info("✅ Файлы отправлены в GitHub")
            return True
        logger.info("ℹ️ Нет новых изменений")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка push: {e}")
        return False


async def run_parsing() -> bool:
    logger.info(f"🚀 Запуск {datetime.now(MOSCOW_TZ)}")
    
    if not is_allowed_time():
        return True
    if not is_user_active():
        return True
    
    try:
        checker = FedresursBankruptcyChecker(
            client_file="Клиенты_страхование_ТЕСТ.xlsx",
            headless=True,  # Скрытый браузер для автозапуска
            delay=3
        )
        
        # ✅ ВАШЕ: run() возвращает 2 значения: df, html_path
        df, html_path = await checker.run()
        
        logger.info(f"✅ Завершено: {len(df)} компаний")
        
        # Сохраняем в историю (если есть log_manager)
        try:
            from log_manager import LogManager
            log_manager = LogManager()
            excel_path = checker.output_file
            log_manager.save_run(excel_path, html_path, df)
        except:
            pass
        
        # Отправляем в GitHub
        push_to_github()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    if '--service' in sys.argv:
        asyncio.run(run_parsing())
    else:
        asyncio.run(run_parsing())