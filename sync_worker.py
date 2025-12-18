#!/usr/bin/env python3
"""
Voluum Sync Worker
Runs continuous sync of Voluum data to Supabase

Can be run as:
- One-time sync: python sync_worker.py --once
- Continuous: python sync_worker.py (runs every 5 minutes)
- Backfill: python sync_worker.py --backfill 30 (sync last 30 days)
"""

import os
import asyncio
import argparse
import signal
import sys
from datetime import datetime, timedelta
import logging

from dotenv import load_dotenv
load_dotenv()

from data_collector import DataCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Graceful shutdown
shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received, stopping...")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


async def run_sync_cycle(collector: DataCollector, days_back: int = 1) -> bool:
    """
    Run a single sync cycle

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Starting sync cycle (days_back={days_back})")
        start_time = datetime.utcnow()

        results = await collector.run_full_sync(days_back=days_back)

        elapsed = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            f"Sync completed in {elapsed:.1f}s - "
            f"Visits: {results['visits_synced']}, "
            f"Conversions: {results['conversions_synced']}"
        )

        return True

    except Exception as e:
        logger.error(f"Sync cycle failed: {e}", exc_info=True)
        return False


async def run_continuous(
    collector: DataCollector,
    interval_minutes: int = 5,
    days_back: int = 1
):
    """
    Run continuous sync at specified interval
    """
    logger.info(f"Starting continuous sync (interval={interval_minutes}m)")

    while not shutdown_event.is_set():
        await run_sync_cycle(collector, days_back=days_back)

        # Wait for interval or shutdown
        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=interval_minutes * 60
            )
        except asyncio.TimeoutError:
            pass  # Timeout is expected, continue loop

    logger.info("Continuous sync stopped")


async def run_backfill(collector: DataCollector, days: int):
    """
    Backfill historical data
    """
    logger.info(f"Starting backfill for {days} days")

    # Process in chunks to avoid timeouts
    chunk_size = 7  # days per chunk
    total_visits = 0
    total_conversions = 0

    for start_day in range(0, days, chunk_size):
        end_day = min(start_day + chunk_size, days)

        from_date = datetime.utcnow() - timedelta(days=end_day)
        to_date = datetime.utcnow() - timedelta(days=start_day)

        logger.info(f"Backfilling {from_date.date()} to {to_date.date()}")

        try:
            visits = await collector.sync_visits(from_date=from_date, to_date=to_date)
            conversions = await collector.sync_conversions(from_date=from_date, to_date=to_date)

            total_visits += visits
            total_conversions += conversions

            logger.info(f"Chunk complete: {visits} visits, {conversions} conversions")

        except Exception as e:
            logger.error(f"Backfill chunk failed: {e}")
            continue

        # Small delay between chunks
        await asyncio.sleep(2)

    logger.info(f"Backfill complete: {total_visits} visits, {total_conversions} conversions")


async def main():
    parser = argparse.ArgumentParser(description="Voluum Data Sync Worker")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run sync once and exit"
    )
    parser.add_argument(
        "--backfill",
        type=int,
        metavar="DAYS",
        help="Backfill historical data for N days"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Sync interval in minutes (default: 5)"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=1,
        help="Days of recent data to sync each cycle (default: 1)"
    )
    parser.add_argument(
        "--retention",
        type=int,
        default=90,
        help="Data retention in days (default: 90)"
    )

    args = parser.parse_args()

    # Validate environment
    required_vars = ["SUPABASE_URL", "SUPABASE_SERVICE_KEY", "VOLUUM_ACCESS_ID", "VOLUUM_ACCESS_KEY"]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        logger.error(f"Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

    # Create collector
    collector = DataCollector(retention_days=args.retention)

    if args.backfill:
        await run_backfill(collector, args.backfill)
    elif args.once:
        success = await run_sync_cycle(collector, days_back=args.days_back)
        sys.exit(0 if success else 1)
    else:
        await run_continuous(
            collector,
            interval_minutes=args.interval,
            days_back=args.days_back
        )


if __name__ == "__main__":
    asyncio.run(main())
