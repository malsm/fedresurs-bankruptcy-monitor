"""
Точка входа для GitHub Actions - запускает парсинг
"""
import asyncio
import logging
import subprocess
import os
from datetime import datetime
from config import PARSER_CONFIG, MOSCOW_TZ
from parser import FedresursBankruptcyChecker
from log_manager import LogManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def push_to_github():
    """Загрузка отчётов в GitHub"""
    try:
        subprocess.run(['git', 'add', 'logs/'], check=True, capture_output=True)
        result = subprocess.run(['git', 'diff', '--cached', '--quiet'], capture_output=True)
        if result.returncode != 0:
            timestamp = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M')
            subprocess.run(['git', 'config', 'user.email', 'action@github.com'], check=True, capture_output=True)
            subprocess.run(['git', 'config', 'user.name', 'GitHub Action'], check=True, capture_output=True)
            subprocess.run(['git', 'commit', '-m', f'📊 Отчёт {timestamp}'], check=True, capture_output=True)
            subprocess.run(['git', 'push'], check=True, capture_output=True)
            logger.info(" Файлы загружены в GitHub")
        else:
            logger.info(" Нет новых изменений")
    except Exception as e:
        logger.error(f" Ошибка загрузки: {e}")


async def run_daily_parsing():
    logger.info(f" Запуск парсинга {datetime.now(MOSCOW_TZ)}")
    
    try:
        checker = FedresursBankruptcyChecker(
            client_file=PARSER_CONFIG["client_file"],
            headless=PARSER_CONFIG["headless"],
            delay=PARSER_CONFIG["delay"],
            batch_size=PARSER_CONFIG["batch_size"],
            batch_delay=PARSER_CONFIG["batch_delay"]
        )
        
        df, excel_path, html_path = await checker.run_with_batches()
        
        log_manager = LogManager()
        run_id = log_manager.save_run(excel_path, html_path, df)
        log_manager.cleanup_old_files(keep_days=90)
        
        push_to_github()
        
        logger.info(f" Парсинг завершён. ID: {run_id}")
        return True
        
    except Exception as e:
        logger.error(f" Ошибка: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    asyncio.run(run_daily_parsing())
