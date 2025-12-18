#!/usr/bin/env python3
"""
Voluum Sync Worker for DigitalOcean App Platform
Runs continuous sync at specified interval + daily email reports
"""

import asyncio
import os
import sys
import signal
from datetime import datetime, timedelta
import logging

from dotenv import load_dotenv
load_dotenv()

from data_collector_v2 import VoluumLiveCollector

# Configuration
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "120"))  # 2 minutes default
DAILY_REPORT_HOUR = int(os.getenv("DAILY_REPORT_HOUR", "8"))  # 8 AM UTC default

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

# Track last email sent date
last_email_date = None


def should_send_daily_email() -> bool:
    """Check if daily email should be sent now"""
    global last_email_date
    now = datetime.utcnow()
    today = now.date()

    # Check if email is paused
    if os.getenv('PAUSE_EMAIL', 'false').lower() == 'true':
        return False

    # Check if we have SendGrid configured
    if not os.getenv('SENDGRID_API_KEY'):
        return False

    # Check if we already sent today
    if last_email_date == today:
        return False

    # Check if it's the right hour
    if now.hour == DAILY_REPORT_HOUR:
        return True

    return False


def send_daily_email():
    """Send the daily pattern report email"""
    global last_email_date

    try:
        from email_report import load_conversions, generate_html_report, send_email

        logger.info("Generating daily email report...")
        df = load_conversions(days_back=30)
        html = generate_html_report(df)

        status = send_email(html)
        if status == 202:
            logger.info(f"Daily email sent successfully to {os.getenv('REPORT_EMAIL_TO')}")
            last_email_date = datetime.utcnow().date()
        else:
            logger.warning(f"Email sent with status: {status}")

    except Exception as e:
        logger.error(f"Failed to send daily email: {e}", exc_info=True)


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
    logger.info(f"Daily email configured for {DAILY_REPORT_HOUR}:00 UTC")

    while not shutdown_event.is_set():
        await run_sync_cycle()

        # Check if daily email should be sent
        if should_send_daily_email():
            send_daily_email()

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
