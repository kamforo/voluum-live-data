#!/usr/bin/env python3
"""
Voluum Sync Runner for Cron
Runs a full sync cycle and logs results
"""

import asyncio
import os
import sys
from datetime import datetime
import logging

# Change to script directory for relative imports
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from data_collector_v2 import VoluumLiveCollector

# Set up logging to file
log_file = os.path.join(os.path.dirname(__file__), 'sync.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


async def run_sync():
    """Run a full sync cycle"""
    start_time = datetime.utcnow()
    logger.info("=" * 50)
    logger.info(f"Starting sync at {start_time.isoformat()}")

    try:
        collector = VoluumLiveCollector()
        results = await collector.run_full_sync()

        elapsed = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            f"Sync completed in {elapsed:.1f}s - "
            f"Visits: {results['visits']}, "
            f"Clicks: {results['clicks']}, "
            f"Conversions: {results['conversions']}"
        )
        return True

    except Exception as e:
        logger.error(f"Sync failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = asyncio.run(run_sync())
    sys.exit(0 if success else 1)
