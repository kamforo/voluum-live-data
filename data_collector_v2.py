"""
Voluum Data Collector v2
Syncs live visit/click data and conversions from Voluum to Supabase
Uses /report/live/visits and /report/live/clicks endpoints
"""

import os
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import logging
import httpx

from supabase import create_client, Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VoluumLiveCollector:
    """Collects live visit/click data and conversions from Voluum"""

    BASE_URL = "https://api.voluum.com"

    def __init__(
        self,
        supabase_url: Optional[str] = None,
        supabase_key: Optional[str] = None,
        voluum_access_id: Optional[str] = None,
        voluum_access_key: Optional[str] = None,
        campaign_filter: str = "Voluum MB"
    ):
        # Supabase
        self.supabase_url = supabase_url or os.getenv("SUPABASE_URL")
        self.supabase_key = supabase_key or os.getenv("SUPABASE_SERVICE_KEY")
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_KEY required")
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)

        # Voluum
        self.access_id = voluum_access_id or os.getenv("VOLUUM_ACCESS_ID")
        self.access_key = voluum_access_key or os.getenv("VOLUUM_ACCESS_KEY")
        if not self.access_id or not self.access_key:
            raise ValueError("VOLUUM_ACCESS_ID and VOLUUM_ACCESS_KEY required")

        self.token: Optional[str] = None
        self.token_expires: Optional[datetime] = None
        self.campaign_filter = campaign_filter

    async def _ensure_auth(self) -> str:
        """Get valid auth token"""
        if self.token and self.token_expires and datetime.now() < self.token_expires:
            return self.token

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.BASE_URL}/auth/access/session",
                json={"accessId": self.access_id, "accessKey": self.access_key},
                headers={"Content-Type": "application/json", "Accept": "application/json"}
            )
            resp.raise_for_status()
            self.token = resp.json().get("token")
            self.token_expires = datetime.now() + timedelta(hours=3)
            return self.token

    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make authenticated request"""
        token = await self._ensure_auth()
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.request(
                method,
                f"{self.BASE_URL}{endpoint}",
                params=params,
                headers={"cwauth-token": token, "Accept": "application/json"}
            )
            resp.raise_for_status()
            return resp.json()

    async def get_tracked_campaigns(self) -> List[Dict]:
        """Get list of campaigns matching filter with recent traffic"""
        data = await self._request("GET", "/report", params={
            "from": (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00"),
            "to": datetime.utcnow().strftime("%Y-%m-%dT23:00:00"),
            "tz": "UTC",
            "groupBy": "campaign",
            "limit": 500
        })

        campaigns = []
        for row in data.get("rows", []):
            name = row.get("campaignName", "")
            if self.campaign_filter in name and row.get("visits", 0) > 0:
                campaigns.append({
                    "campaign_id": row.get("campaignId"),
                    "campaign_name": name,
                    "visits": row.get("visits", 0)
                })

        logger.info(f"Found {len(campaigns)} {self.campaign_filter} campaigns with traffic")
        return campaigns

    def _parse_timestamp(self, ts_str: str) -> Optional[str]:
        """Parse Voluum timestamp string and return ISO format"""
        if not ts_str:
            return None
        try:
            # Format: "2025-12-18 12:52:23 AM"
            dt = datetime.strptime(ts_str, "%Y-%m-%d %I:%M:%S %p")
            return dt.isoformat()
        except:
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                return dt.isoformat()
            except:
                return ts_str  # Return original if can't parse

    def transform_visit(self, raw: Dict) -> Dict:
        """Transform raw visit to schema"""
        return {
            "click_id": raw.get("clickId"),
            "external_id": raw.get("externalId"),
            "campaign_id": raw.get("campaignId"),
            "campaign_name": raw.get("campaignName"),
            "traffic_source_id": raw.get("trafficSourceId"),
            "traffic_source_name": raw.get("trafficSourceName"),
            "offer_id": raw.get("offerId"),
            "offer_name": raw.get("offerName"),
            "affiliate_network_id": raw.get("affiliateNetworkId"),
            "affiliate_network_name": raw.get("affiliateNetworkName"),
            "lander_id": raw.get("landerId"),
            "lander_name": raw.get("landerName"),
            "visit_timestamp": self._parse_timestamp(raw.get("timestamp")),
            "country_code": raw.get("countryCode"),
            "country_name": raw.get("countryName"),
            "region": raw.get("region"),
            "city": raw.get("city"),
            "device": raw.get("device"),
            "device_name": raw.get("deviceName"),
            "brand": raw.get("brand"),
            "model": raw.get("model"),
            "os": raw.get("os"),
            "os_version": raw.get("osVersion"),
            "browser": raw.get("browser"),
            "browser_version": raw.get("browserVersion"),
            "connection_type": raw.get("connectionType"),
            "isp": raw.get("isp"),
            "mobile_carrier": raw.get("mobileCarrier"),
            "ip": raw.get("ip"),
            "custom_var_1": raw.get("customVariable1"),
            "custom_var_2": raw.get("customVariable2"),
            "custom_var_3": raw.get("customVariable3"),
            "custom_var_4": raw.get("customVariable4"),
            "custom_var_5": raw.get("customVariable5"),
            "custom_var_6": raw.get("customVariable6"),
            "custom_var_7": raw.get("customVariable7"),
            "custom_var_8": raw.get("customVariable8"),
            "custom_var_9": raw.get("customVariable9"),
            "custom_var_10": raw.get("customVariable10"),
            "referrer": raw.get("referrer"),
            "user_agent": raw.get("userAgent"),
            "raw_data": raw
        }

    def transform_click(self, raw: Dict) -> Dict:
        """Transform raw click to schema"""
        return {
            "click_id": raw.get("clickId"),
            "external_id": raw.get("externalId"),
            "campaign_id": raw.get("campaignId"),
            "campaign_name": raw.get("campaignName"),
            "offer_id": raw.get("offerId"),
            "offer_name": raw.get("offerName"),
            "lander_id": raw.get("landerId"),
            "lander_name": raw.get("landerName"),
            "click_timestamp": self._parse_timestamp(raw.get("timestamp")),
            "country_code": raw.get("countryCode"),
            "country_name": raw.get("countryName"),
            "device": raw.get("device"),
            "os": raw.get("os"),
            "browser": raw.get("browser"),
            "ip": raw.get("ip"),
            "raw_data": raw
        }

    def transform_conversion(self, raw: Dict) -> Dict:
        """Transform raw conversion to schema"""
        return {
            "click_id": raw.get("clickId"),
            "external_id": raw.get("externalId"),
            "transaction_id": raw.get("transactionId"),
            "campaign_id": raw.get("campaignId"),
            "campaign_name": raw.get("campaignName"),
            "offer_id": raw.get("offerId"),
            "offer_name": raw.get("offerName"),
            "affiliate_network_id": raw.get("affiliateNetworkId"),
            "affiliate_network_name": raw.get("affiliateNetworkName"),
            "postback_timestamp": self._parse_timestamp(raw.get("postbackTimestamp")),
            "visit_timestamp": self._parse_timestamp(raw.get("visitTimestamp")),
            "country_code": raw.get("countryCode"),
            "country_name": raw.get("countryName"),
            "revenue": float(raw.get("revenue") or 0),
            "payout": float(raw.get("payout") or 0),
            "cost": float(raw.get("cost") or 0),
            "profit": float(raw.get("profit") or 0),
            "device": raw.get("device"),
            "os": raw.get("os"),
            "browser": raw.get("browser"),
            "connection_type": raw.get("connectionType"),
            "isp": raw.get("isp"),
            "ip": raw.get("ip"),
            "custom_var_1": raw.get("customVariable1"),
            "custom_var_2": raw.get("customVariable2"),
            "custom_var_3": raw.get("customVariable3"),
            "custom_var_4": raw.get("customVariable4"),
            "custom_var_5": raw.get("customVariable5"),
            "raw_data": raw
        }

    async def sync_live_visits(self, campaign_ids: Optional[List[str]] = None) -> int:
        """Sync live visits for campaigns"""
        if not campaign_ids:
            campaigns = await self.get_tracked_campaigns()
            campaign_ids = [c["campaign_id"] for c in campaigns]

        total_synced = 0
        seen_click_ids = set()

        for i, cid in enumerate(campaign_ids):
            # Rate limiting - pause every 10 campaigns
            if i > 0 and i % 10 == 0:
                await asyncio.sleep(1)
            try:
                data = await self._request("GET", f"/report/live/visits/{cid}", params={"limit": 100})
                rows = data.get("rows", [])

                if not rows:
                    continue

                batch = []
                for row in rows:
                    click_id = row.get("clickId")
                    if click_id and click_id not in seen_click_ids:
                        seen_click_ids.add(click_id)
                        visit = self.transform_visit(row)
                        if visit["click_id"]:
                            batch.append(visit)

                if batch:
                    self.supabase.table("live_visits").upsert(
                        batch,
                        on_conflict="click_id"
                    ).execute()
                    total_synced += len(batch)
                    logger.info(f"Synced {len(batch)} visits from campaign {cid[:8]}...")

            except Exception as e:
                logger.error(f"Error syncing visits for {cid}: {e}")
                continue

        logger.info(f"Total visits synced: {total_synced}")
        return total_synced

    async def sync_live_clicks(self, campaign_ids: Optional[List[str]] = None) -> int:
        """Sync live clicks for campaigns"""
        if not campaign_ids:
            campaigns = await self.get_tracked_campaigns()
            campaign_ids = [c["campaign_id"] for c in campaigns]

        total_synced = 0
        seen_click_ids = set()

        for i, cid in enumerate(campaign_ids):
            # Rate limiting - pause every 10 campaigns
            if i > 0 and i % 10 == 0:
                await asyncio.sleep(1)
            try:
                data = await self._request("GET", f"/report/live/clicks/{cid}", params={"limit": 100})
                rows = data.get("rows", [])

                if not rows:
                    continue

                batch = []
                for row in rows:
                    click_id = row.get("clickId")
                    if click_id and click_id not in seen_click_ids:
                        seen_click_ids.add(click_id)
                        click = self.transform_click(row)
                        if click["click_id"]:
                            batch.append(click)

                if batch:
                    self.supabase.table("live_clicks").upsert(
                        batch,
                        on_conflict="click_id"
                    ).execute()
                    total_synced += len(batch)
                    logger.info(f"Synced {len(batch)} clicks from campaign {cid[:8]}...")

            except Exception as e:
                logger.error(f"Error syncing clicks for {cid}: {e}")
                continue

        logger.info(f"Total clicks synced: {total_synced}")
        return total_synced

    async def sync_conversions(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        days_back: int = 1
    ) -> int:
        """Sync conversions (historical data available)"""
        if not from_date:
            from_date = datetime.utcnow() - timedelta(days=days_back)
        if not to_date:
            to_date = datetime.utcnow()

        from_str = from_date.strftime("%Y-%m-%dT%H:00:00")
        to_str = to_date.strftime("%Y-%m-%dT%H:00:00")

        logger.info(f"Syncing conversions from {from_str} to {to_str}")

        total_synced = 0
        offset = 0
        limit = 1000

        while True:
            try:
                data = await self._request("GET", "/report/conversions", params={
                    "from": from_str,
                    "to": to_str,
                    "tz": "UTC",
                    "limit": limit,
                    "offset": offset
                })

                rows = data.get("rows", [])
                if not rows:
                    break

                # Filter to campaign_filter campaigns and dedupe
                batch = []
                seen = set()
                for row in rows:
                    name = row.get("campaignName", "")
                    if self.campaign_filter not in name:
                        continue

                    click_id = row.get("clickId")
                    postback_ts = row.get("postbackTimestamp")
                    key = f"{click_id}_{postback_ts}"

                    if key not in seen:
                        seen.add(key)
                        conv = self.transform_conversion(row)
                        if conv["click_id"]:
                            batch.append(conv)

                if batch:
                    self.supabase.table("conversions").upsert(
                        batch,
                        on_conflict="click_id,postback_timestamp"
                    ).execute()
                    total_synced += len(batch)
                    logger.info(f"Synced {len(batch)} conversions (offset {offset})")

                if len(rows) < limit:
                    break
                offset += limit

            except Exception as e:
                logger.error(f"Error syncing conversions: {e}")
                break

        logger.info(f"Total conversions synced: {total_synced}")
        return total_synced

    async def run_full_sync(self) -> Dict[str, int]:
        """Run complete sync cycle"""
        results = {
            "visits": 0,
            "clicks": 0,
            "conversions": 0
        }

        # Get campaign IDs once
        campaigns = await self.get_tracked_campaigns()
        campaign_ids = [c["campaign_id"] for c in campaigns]

        logger.info(f"Syncing {len(campaign_ids)} campaigns")

        # Sync visits
        results["visits"] = await self.sync_live_visits(campaign_ids)

        # Sync clicks
        results["clicks"] = await self.sync_live_clicks(campaign_ids)

        # Sync conversions (last 24h)
        results["conversions"] = await self.sync_conversions(days_back=1)

        return results


async def main():
    from dotenv import load_dotenv
    load_dotenv()

    collector = VoluumLiveCollector()
    results = await collector.run_full_sync()
    print(f"\nSync Results: {results}")


if __name__ == "__main__":
    asyncio.run(main())
