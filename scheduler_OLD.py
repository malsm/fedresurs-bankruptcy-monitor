"""
scheduler.py — точка входа для локального запуска
Запускается на вашем ПК, отправляет отчёты в GitHub
Совместим с parser.py (параметры: client_file, headless, delay)
"""
import asyncio
import logging
import subprocess
from datetime import datetime, timezone, timedelta
from parser import FedresursBankruptcyChecker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

MOSCOW_TZ = timezone(timedelta(hours=3))


def push_to_github():
    """Отправка отчётов в репозиторий"""
    try:
        subprocess.run(['git', 'add', 'logs/'], check=True, capture_output=True)
        result = subprocess.run(['git', 'diff', '--cached', '--quiet'], capture_output=True)
        if result.returncode != 0:
            timestamp = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M MSK')
            subprocess.run(['git', 'config', 'user.email', 'action@github.com'], check=True, capture_output=True)
            subprocess.run(['git', 'config', 'user.name', 'GitHub Action'], check=True, capture_output=True)
            subprocess.run(['git', 'commit', '-m', f'Отчёт {timestamp}'], check=True, capture_output=True)
            subprocess.run(['git', 'push'], check=True, capture_output=True)
            logger.info("Файлы отправлены в GitHub")
        else:
            logger.info("Нет новых изменений")
    except Exception as e:
        logger.error(f"Ошибка push: {e}")


async def run_daily_parsing():
    logger.info(f"Запуск парсинга {datetime.now(MOSCOW_TZ)}")
    
    try:
        checker = FedresursBankruptcyChecker(
            client_file="Клиенты_страхование_ТЕСТ.xlsx",
            headless=False,
            delay=3
        )
        
        df, html_path = await checker.run()
        
        logger.info(f"Парсинг завершён: {len(df)} компаний")
        
        push_to_github()
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    asyncio.run(run_daily_parsing())