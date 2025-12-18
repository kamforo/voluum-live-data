#!/usr/bin/env python3
"""
Voluum Sync Worker for DigitalOcean App Platform
Runs continuous sync at specified interval
"""

import asyncio
import os
import sys
import signal
from datetime import datetime
import logging

from dotenv import load_dotenv
load_dotenv()

from data_collector_v2 import VoluumLiveCollector

# Configuration
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "120"))  # 2 minutes default

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Graceful shutdown
shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


async def run_sync_cycle():
    """Run a single sync cycle"""
    start_time = datetime.utcnow()
    logger.info(f"Starting sync cycle at {start_time.isoformat()}")

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


async def main():
    """Main worker loop"""
    logger.info(f"Voluum Sync Worker starting (interval: {SYNC_INTERVAL_SECONDS}s)")

    while not shutdown_event.is_set():
        await run_sync_cycle()

        # Wait for interval or shutdown signal
        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=SYNC_INTERVAL_SECONDS
            )
        except asyncio.TimeoutError:
            pass  # Expected, continue loop

    logger.info("Worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
