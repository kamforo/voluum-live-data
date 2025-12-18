"""
Voluum API Client
Handles authentication and API requests to Voluum
"""

import os
import httpx
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List


class VoluumClient:
    """Client for interacting with Voluum API"""

    BASE_URL = "https://api.voluum.com"

    def __init__(self, access_id: Optional[str] = None, access_key: Optional[str] = None):
        self.access_id = access_id or os.getenv("VOLUUM_ACCESS_ID")
        self.access_key = access_key or os.getenv("VOLUUM_ACCESS_KEY")
        self.token: Optional[str] = None
        self.token_expires: Optional[datetime] = None

        if not self.access_id or not self.access_key:
            raise ValueError("VOLUUM_ACCESS_ID and VOLUUM_ACCESS_KEY are required")

    async def _ensure_authenticated(self) -> None:
        """Ensure we have a valid authentication token"""
        if self.token and self.token_expires and datetime.now() < self.token_expires:
            return

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.BASE_URL}/auth/access/session",
                json={
                    "accessId": self.access_id,
                    "accessKey": self.access_key
                },
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "Accept": "application/json"
                }
            )
            response.raise_for_status()
            data = response.json()

            self.token = data.get("token")
            # Token expires in 4 hours, refresh 30 min early
            self.token_expires = datetime.now() + timedelta(hours=3, minutes=30)

    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, json_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Make an authenticated request to the API"""
        await self._ensure_authenticated()

        async with httpx.AsyncClient() as client:
            response = await client.request(
                method,
                f"{self.BASE_URL}{endpoint}",
                params=params,
                json=json_data,
                headers={
                    "cwauth-token": self.token,
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                },
                timeout=60.0
            )
            response.raise_for_status()
            return response.json()

    async def get_report(
        self,
        group_by: str = "campaign",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        timezone: str = "America/Los_Angeles",
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, str]] = None,
        limit: int = 1000,
        offset: int = 0,
        sort: Optional[str] = None,
        direction: str = "DESC"
    ) -> Dict[str, Any]:
        """
        Get report data from Voluum

        Args:
            group_by: Grouping dimension (campaign, offer, lander, traffic-source, country, etc.)
            from_date: Start date (ISO format, defaults to today)
            to_date: End date (ISO format, defaults to today)
            timezone: Timezone for the report
            columns: List of columns to include (visits, clicks, conversions, revenue, cost, profit, etc.)
            filters: Filter parameters (e.g., {"campaignId": "abc123"})
            limit: Max rows to return
            offset: Offset for pagination
            sort: Column to sort by
            direction: Sort direction (ASC or DESC)
        """
        # Default to today if no dates provided
        if not from_date:
            from_date = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")

        params = {
            "from": from_date,
            "to": to_date,
            "tz": timezone,
            "groupBy": group_by,
            "limit": limit,
            "offset": offset,
            "direction": direction
        }

        if columns:
            params["columns"] = ",".join(columns)

        if sort:
            params["sort"] = sort

        if filters:
            params.update(filters)

        return await self._request("GET", "/report", params=params)

    async def get_campaigns(self, limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Get list of campaigns"""
        return await self._request("GET", "/campaign", params={
            "limit": limit,
            "offset": offset
        })

    async def get_campaign(self, campaign_id: str) -> Dict[str, Any]:
        """Get a specific campaign by ID"""
        return await self._request("GET", f"/campaign/{campaign_id}")

    async def get_offers(self, limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Get list of offers"""
        return await self._request("GET", "/offer", params={
            "limit": limit,
            "offset": offset
        })

    async def get_offer(self, offer_id: str) -> Dict[str, Any]:
        """Get a specific offer by ID"""
        return await self._request("GET", f"/offer/{offer_id}")

    async def get_traffic_sources(self, limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Get list of traffic sources"""
        return await self._request("GET", "/traffic-source", params={
            "limit": limit,
            "offset": offset
        })

    async def get_landers(self, limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Get list of landers"""
        return await self._request("GET", "/lander", params={
            "limit": limit,
            "offset": offset
        })

    async def get_affiliate_networks(self, limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Get list of affiliate networks"""
        return await self._request("GET", "/affiliate-network", params={
            "limit": limit,
            "offset": offset
        })

    async def get_conversions(
        self,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        timezone: str = "America/Los_Angeles",
        campaign_id: Optional[str] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """
        Get conversion data

        Args:
            from_date: Start date (ISO format)
            to_date: End date (ISO format)
            timezone: Timezone
            campaign_id: Filter by campaign
            limit: Max conversions to return
        """
        if not from_date:
            from_date = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")

        params = {
            "from": from_date,
            "to": to_date,
            "tz": timezone,
            "limit": limit
        }

        if campaign_id:
            params["campaignId"] = campaign_id

        return await self._request("GET", "/report/conversions", params=params)

    async def get_clicks(
        self,
        campaign_id: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Get click-level data for a campaign"""
        if not from_date:
            from_date = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")

        return await self._request("GET", f"/report/clicks", params={
            "campaignId": campaign_id,
            "from": from_date,
            "to": to_date,
            "limit": limit
        })
