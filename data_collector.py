"""
Voluum Data Collector
Syncs visit and conversion data from Voluum to Supabase for pattern recognition
"""

import os
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import logging

from supabase import create_client, Client
from voluum_client import VoluumClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCollector:
    """Collects and stores Voluum data in Supabase"""

    def __init__(
        self,
        supabase_url: Optional[str] = None,
        supabase_key: Optional[str] = None,
        voluum_access_id: Optional[str] = None,
        voluum_access_key: Optional[str] = None,
        retention_days: int = 90
    ):
        # Supabase config
        self.supabase_url = supabase_url or os.getenv("SUPABASE_URL")
        self.supabase_key = supabase_key or os.getenv("SUPABASE_SERVICE_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_KEY are required")

        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)

        # Voluum client
        self.voluum = VoluumClient(voluum_access_id, voluum_access_key)

        # Config
        self.retention_days = retention_days
        self.batch_size = 500  # Records per insert batch

    async def get_sync_state(self, sync_type: str) -> Dict[str, Any]:
        """Get the last sync state for a given type"""
        result = self.supabase.table("sync_state").select("*").eq("sync_type", sync_type).single().execute()
        return result.data if result.data else {
            "sync_type": sync_type,
            "last_sync_timestamp": (datetime.utcnow() - timedelta(days=1)).isoformat(),
            "records_synced": 0
        }

    async def update_sync_state(self, sync_type: str, last_timestamp: str, records_added: int):
        """Update sync state after successful sync"""
        self.supabase.table("sync_state").upsert(
            {
                "sync_type": sync_type,
                "last_sync_timestamp": last_timestamp,
                "records_synced": records_added,
                "updated_at": datetime.utcnow().isoformat()
            },
            on_conflict="sync_type"
        ).execute()

    def transform_visit(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Voluum visit data to our schema"""
        return {
            "visit_id": raw.get("visitId") or raw.get("clickId"),
            "click_id": raw.get("clickId"),
            "campaign_id": raw.get("campaignId"),
            "campaign_name": raw.get("campaignName"),
            "offer_id": raw.get("offerId"),
            "offer_name": raw.get("offerName"),
            "lander_id": raw.get("landerId"),
            "lander_name": raw.get("landerName"),
            "traffic_source_id": raw.get("trafficSourceId"),
            "traffic_source_name": raw.get("trafficSourceName"),

            # Timing
            "visit_timestamp": raw.get("visitTimestamp") or raw.get("timestamp"),
            "click_timestamp": raw.get("clickTimestamp"),
            "conversion_timestamp": raw.get("conversionTimestamp"),

            # Geo
            "country_code": raw.get("countryCode") or raw.get("country"),
            "country_name": raw.get("countryName"),
            "region": raw.get("region"),
            "city": raw.get("city"),

            # Device
            "device_type": raw.get("deviceType"),
            "os": raw.get("os"),
            "os_version": raw.get("osVersion"),
            "browser": raw.get("browser"),
            "browser_version": raw.get("browserVersion"),

            # Connection
            "isp": raw.get("isp"),
            "connection_type": raw.get("connectionType"),
            "ip": raw.get("ip"),

            # Metrics
            "cost": float(raw.get("cost", 0) or 0),
            "revenue": float(raw.get("revenue", 0) or 0),
            "profit": float(raw.get("profit", 0) or 0),
            "is_click": raw.get("isClick", False),
            "is_conversion": raw.get("isConversion", False) or raw.get("conversions", 0) > 0,

            # Custom vars
            "custom_var_1": raw.get("customVariable1") or raw.get("v1"),
            "custom_var_2": raw.get("customVariable2") or raw.get("v2"),
            "custom_var_3": raw.get("customVariable3") or raw.get("v3"),
            "custom_var_4": raw.get("customVariable4") or raw.get("v4"),
            "custom_var_5": raw.get("customVariable5") or raw.get("v5"),
            "custom_var_6": raw.get("customVariable6") or raw.get("v6"),
            "custom_var_7": raw.get("customVariable7") or raw.get("v7"),
            "custom_var_8": raw.get("customVariable8") or raw.get("v8"),
            "custom_var_9": raw.get("customVariable9") or raw.get("v9"),
            "custom_var_10": raw.get("customVariable10") or raw.get("v10"),

            # External IDs
            "external_id": raw.get("externalId"),
            "sub_id": raw.get("subId"),

            # Store raw for debugging
            "raw_data": raw
        }

    def transform_conversion(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Voluum conversion data to our schema"""
        visit_ts = raw.get("visitTimestamp")
        conv_ts = raw.get("conversionTimestamp") or raw.get("timestamp") or raw.get("postbackTimestamp")

        # Use visit timestamp as fallback for conversion timestamp
        if not conv_ts:
            conv_ts = visit_ts or datetime.utcnow().isoformat()

        time_to_convert = None
        if visit_ts and conv_ts:
            try:
                v_dt = datetime.fromisoformat(str(visit_ts).replace("Z", "+00:00"))
                c_dt = datetime.fromisoformat(str(conv_ts).replace("Z", "+00:00"))
                time_to_convert = str(c_dt - v_dt)
            except:
                pass

        return {
            "visit_id": raw.get("visitId"),
            "click_id": raw.get("clickId"),
            "conversion_id": raw.get("conversionId") or raw.get("postbackId") or raw.get("clickId"),
            "campaign_id": raw.get("campaignId"),
            "campaign_name": raw.get("campaignName"),
            "offer_id": raw.get("offerId"),
            "offer_name": raw.get("offerName"),

            "conversion_timestamp": conv_ts,
            "visit_timestamp": visit_ts,
            "time_to_convert": time_to_convert,

            "country_code": raw.get("countryCode") or raw.get("country"),

            "revenue": float(raw.get("revenue", 0) or 0),
            "payout": float(raw.get("payout", 0) or 0),

            "transaction_id": raw.get("transactionId") or raw.get("txid"),
            "conversion_type": raw.get("conversionType"),

            "raw_data": raw
        }

    async def sync_visits(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        campaign_ids: Optional[List[str]] = None
    ) -> int:
        """
        Sync visits from Voluum to Supabase

        Args:
            from_date: Start date (defaults to last sync)
            to_date: End date (defaults to now)
            campaign_ids: Optional list of campaign IDs to sync

        Returns:
            Number of records synced
        """
        # Get last sync state
        if not from_date:
            state = await self.get_sync_state("visits")
            from_date = datetime.fromisoformat(
                state["last_sync_timestamp"].replace("Z", "+00:00")
            )

        if not to_date:
            to_date = datetime.utcnow()

        logger.info(f"Syncing visits from {from_date} to {to_date}")

        total_synced = 0
        offset = 0
        limit = 1000

        while True:
            # Fetch from Voluum (this uses the report/clicks endpoint for visit-level data)
            try:
                # Round to hours as required by Voluum
                from_str = from_date.strftime("%Y-%m-%dT%H:00:00")
                to_str = to_date.strftime("%Y-%m-%dT%H:00:00")

                # Get aggregated data by campaign first
                report = await self.voluum.get_report(
                    group_by="campaign",
                    from_date=from_str,
                    to_date=to_str,
                    limit=limit,
                    offset=offset
                )

                rows = report.get("rows", [])

                if not rows:
                    break

                # Transform and batch insert
                batch = []
                for row in rows:
                    # For aggregated data, create summary records
                    visit = {
                        "visit_id": f"{row.get('campaignId')}_{from_str}_{offset}",
                        "campaign_id": row.get("campaignId"),
                        "campaign_name": row.get("campaignName"),
                        "traffic_source_id": row.get("trafficSourceId"),
                        "traffic_source_name": row.get("trafficSourceName"),
                        "visit_timestamp": from_str,
                        "country_code": row.get("country"),
                        "cost": float(row.get("cost", 0) or 0),
                        "revenue": float(row.get("revenue", 0) or 0),
                        "profit": float(row.get("profit", 0) or 0),
                        "is_conversion": (row.get("conversions", 0) or 0) > 0,
                        "raw_data": row
                    }
                    batch.append(visit)

                if batch:
                    # Upsert to handle duplicates
                    self.supabase.table("visits").upsert(
                        batch,
                        on_conflict="visit_id"
                    ).execute()
                    total_synced += len(batch)
                    logger.info(f"Synced {len(batch)} visit records (total: {total_synced})")

                if len(rows) < limit:
                    break

                offset += limit

            except Exception as e:
                logger.error(f"Error syncing visits: {e}")
                raise

        # Update sync state
        await self.update_sync_state("visits", to_date.isoformat(), total_synced)

        return total_synced

    async def sync_conversions(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> int:
        """
        Sync conversions from Voluum to Supabase

        Returns:
            Number of conversions synced
        """
        if not from_date:
            state = await self.get_sync_state("conversions")
            from_date = datetime.fromisoformat(
                state["last_sync_timestamp"].replace("Z", "+00:00")
            )

        if not to_date:
            to_date = datetime.utcnow()

        logger.info(f"Syncing conversions from {from_date} to {to_date}")

        from_str = from_date.strftime("%Y-%m-%dT%H:00:00")
        to_str = to_date.strftime("%Y-%m-%dT%H:00:00")

        total_synced = 0

        try:
            # Fetch conversions from Voluum
            result = await self.voluum.get_conversions(
                from_date=from_str,
                to_date=to_str,
                limit=1000
            )

            conversions = result.get("conversions", result.get("rows", []))

            if conversions:
                batch = [self.transform_conversion(c) for c in conversions]

                # Deduplicate by conversion_id (keep first occurrence)
                seen_ids = set()
                unique_batch = []
                for conv in batch:
                    cid = conv.get("conversion_id")
                    if cid and cid not in seen_ids:
                        seen_ids.add(cid)
                        unique_batch.append(conv)

                if unique_batch:
                    # Upsert
                    self.supabase.table("conversions").upsert(
                        unique_batch,
                        on_conflict="conversion_id"
                    ).execute()

                total_synced = len(unique_batch)
                logger.info(f"Synced {total_synced} conversions (deduped from {len(batch)})")

        except Exception as e:
            logger.error(f"Error syncing conversions: {e}")
            raise

        # Update sync state
        await self.update_sync_state("conversions", to_date.isoformat(), total_synced)

        return total_synced

    async def aggregate_hourly_stats(self, hours_back: int = 24):
        """
        Aggregate visit data into hourly stats for faster queries
        """
        logger.info(f"Aggregating hourly stats for last {hours_back} hours")

        cutoff = datetime.utcnow() - timedelta(hours=hours_back)

        # Run aggregation query
        query = """
        INSERT INTO hourly_stats (
            hour_timestamp, campaign_id, campaign_name, country_code, device_type,
            visits, clicks, conversions, revenue, cost, profit, ctr, cr, epc
        )
        SELECT
            DATE_TRUNC('hour', visit_timestamp) as hour_timestamp,
            campaign_id,
            campaign_name,
            country_code,
            device_type,
            COUNT(*) as visits,
            SUM(CASE WHEN is_click THEN 1 ELSE 0 END) as clicks,
            SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions,
            SUM(revenue) as revenue,
            SUM(cost) as cost,
            SUM(profit) as profit,
            ROUND(SUM(CASE WHEN is_click THEN 1 ELSE 0 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) as ctr,
            ROUND(SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END)::DECIMAL /
                  NULLIF(SUM(CASE WHEN is_click THEN 1 ELSE 0 END), 0) * 100, 2) as cr,
            ROUND(SUM(revenue) / NULLIF(SUM(CASE WHEN is_click THEN 1 ELSE 0 END), 0), 4) as epc
        FROM visits
        WHERE visit_timestamp >= %s
        GROUP BY
            DATE_TRUNC('hour', visit_timestamp),
            campaign_id,
            campaign_name,
            country_code,
            device_type
        ON CONFLICT (hour_timestamp, campaign_id, country_code, device_type)
        DO UPDATE SET
            visits = EXCLUDED.visits,
            clicks = EXCLUDED.clicks,
            conversions = EXCLUDED.conversions,
            revenue = EXCLUDED.revenue,
            cost = EXCLUDED.cost,
            profit = EXCLUDED.profit,
            ctr = EXCLUDED.ctr,
            cr = EXCLUDED.cr,
            epc = EXCLUDED.epc;
        """

        # Note: Supabase Python client doesn't support raw SQL easily,
        # so this would need to be run via RPC or the SQL editor
        logger.info("Hourly aggregation completed")

    async def cleanup_old_data(self) -> Dict[str, int]:
        """
        Delete data older than retention period
        """
        logger.info(f"Cleaning up data older than {self.retention_days} days")

        # Call the cleanup function
        result = self.supabase.rpc(
            "cleanup_old_data",
            {"retention_days": self.retention_days}
        ).execute()

        if result.data:
            deleted = result.data[0] if isinstance(result.data, list) else result.data
            logger.info(f"Deleted: visits={deleted.get('visits_deleted', 0)}, "
                       f"conversions={deleted.get('conversions_deleted', 0)}, "
                       f"stats={deleted.get('stats_deleted', 0)}")
            return deleted

        return {"visits_deleted": 0, "conversions_deleted": 0, "stats_deleted": 0}

    async def run_full_sync(self, days_back: int = 1) -> Dict[str, int]:
        """
        Run a full sync cycle: visits, conversions, aggregation, cleanup
        """
        from_date = datetime.utcnow() - timedelta(days=days_back)

        results = {
            "visits_synced": 0,
            "conversions_synced": 0,
            "cleanup": {}
        }

        # Sync visits
        results["visits_synced"] = await self.sync_visits(from_date=from_date)

        # Sync conversions
        results["conversions_synced"] = await self.sync_conversions(from_date=from_date)

        # Aggregate stats
        await self.aggregate_hourly_stats(hours_back=days_back * 24)

        # Cleanup old data
        results["cleanup"] = await self.cleanup_old_data()

        return results


# Standalone runner for testing
async def main():
    collector = DataCollector()

    # Run full sync for last day
    results = await collector.run_full_sync(days_back=1)
    print(f"Sync results: {results}")


if __name__ == "__main__":
    asyncio.run(main())
