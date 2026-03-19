"""
Точка входа для GitHub Actions
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from parser import FedresursBankruptcyChecker
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

moscow_tz = timezone(timedelta(hours=3))


async def run_daily_parsing():
    logger.info(f"🚀 Запуск парсинга {datetime.now(moscow_tz)}")
    
    try:
        checker = FedresursBankruptcyChecker(
            client_file="Клиенты_страхование_ТЕСТ.xlsx",
            headless=True,
            delay=15,
            batch_size=1,
            batch_delay=180,
            max_retries=5,
            use_proxy=False
        )
        
        df, excel_path, html_path = await checker.run_with_batches()
        
        logger.info(f"✅ Парсинг завершён. Обработано: {len(df)} компаний")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    asyncio.run(run_daily_parsing())
