#!/usr/bin/env python3
"""
Voluum MCP Server
Model Context Protocol server for Voluum ad tracking platform
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Tool,
    TextContent,
    CallToolResult,
)

from voluum_client import VoluumClient

# Initialize MCP server
server = Server("voluum")

# Global client instance (initialized on first use)
_client: VoluumClient | None = None


def get_client() -> VoluumClient:
    """Get or create Voluum client instance"""
    global _client
    if _client is None:
        _client = VoluumClient()
    return _client


def format_currency(value: float) -> str:
    """Format a value as currency"""
    return f"${value:,.2f}"


def format_report_row(row: dict) -> str:
    """Format a report row for display"""
    parts = []

    # Name/identifier
    if "name" in row:
        parts.append(f"**{row['name']}**")
    elif "campaignName" in row:
        parts.append(f"**{row['campaignName']}**")
    elif "offerName" in row:
        parts.append(f"**{row['offerName']}**")

    # Key metrics
    metrics = []
    if "visits" in row:
        metrics.append(f"Visits: {row['visits']:,}")
    if "clicks" in row:
        metrics.append(f"Clicks: {row['clicks']:,}")
    if "conversions" in row:
        metrics.append(f"Conv: {row['conversions']:,}")
    if "revenue" in row:
        metrics.append(f"Rev: {format_currency(row['revenue'])}")
    if "cost" in row:
        metrics.append(f"Cost: {format_currency(row['cost'])}")
    if "profit" in row:
        metrics.append(f"Profit: {format_currency(row['profit'])}")
    if "roi" in row:
        metrics.append(f"ROI: {row['roi']:.1f}%")
    if "cr" in row:
        metrics.append(f"CR: {row['cr']:.2f}%")
    if "ctr" in row:
        metrics.append(f"CTR: {row['ctr']:.2f}%")

    if metrics:
        parts.append(" | ".join(metrics))

    return "\n".join(parts)


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available Voluum tools"""
    return [
        Tool(
            name="voluum_report",
            description="Get performance report from Voluum. Supports grouping by campaign, offer, lander, traffic-source, country, device-type, os, browser, connection-type, isp, day, hour, etc. Returns metrics like visits, clicks, conversions, revenue, cost, profit, ROI.",
            inputSchema={
                "type": "object",
                "properties": {
                    "group_by": {
                        "type": "string",
                        "description": "Grouping dimension: campaign, offer, lander, traffic-source, country, device-type, os, browser, connection-type, isp, day, hour, day-of-week",
                        "default": "campaign"
                    },
                    "from_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (defaults to today)"
                    },
                    "to_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (defaults to today)"
                    },
                    "days_back": {
                        "type": "integer",
                        "description": "Alternative: get data for last N days (overrides from_date/to_date)"
                    },
                    "campaign_id": {
                        "type": "string",
                        "description": "Filter by specific campaign ID"
                    },
                    "offer_id": {
                        "type": "string",
                        "description": "Filter by specific offer ID"
                    },
                    "traffic_source_id": {
                        "type": "string",
                        "description": "Filter by specific traffic source ID"
                    },
                    "sort_by": {
                        "type": "string",
                        "description": "Column to sort by: visits, clicks, conversions, revenue, cost, profit, roi",
                        "default": "profit"
                    },
                    "sort_direction": {
                        "type": "string",
                        "enum": ["ASC", "DESC"],
                        "default": "DESC"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max rows to return",
                        "default": 50
                    }
                }
            }
        ),
        Tool(
            name="voluum_campaigns",
            description="List all campaigns in Voluum with their configuration details",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Max campaigns to return",
                        "default": 100
                    }
                }
            }
        ),
        Tool(
            name="voluum_offers",
            description="List all offers in Voluum",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Max offers to return",
                        "default": 100
                    }
                }
            }
        ),
        Tool(
            name="voluum_traffic_sources",
            description="List all traffic sources in Voluum",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Max traffic sources to return",
                        "default": 100
                    }
                }
            }
        ),
        Tool(
            name="voluum_landers",
            description="List all landers (landing pages) in Voluum",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Max landers to return",
                        "default": 100
                    }
                }
            }
        ),
        Tool(
            name="voluum_conversions",
            description="Get recent conversions from Voluum",
            inputSchema={
                "type": "object",
                "properties": {
                    "from_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format"
                    },
                    "to_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format"
                    },
                    "days_back": {
                        "type": "integer",
                        "description": "Get conversions for last N days"
                    },
                    "campaign_id": {
                        "type": "string",
                        "description": "Filter by campaign ID"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 100
                    }
                }
            }
        ),
        Tool(
            name="voluum_summary",
            description="Get a quick summary of today's or recent performance across all campaigns",
            inputSchema={
                "type": "object",
                "properties": {
                    "days_back": {
                        "type": "integer",
                        "description": "Number of days to summarize (default: 1 for today)",
                        "default": 1
                    }
                }
            }
        ),
        Tool(
            name="voluum_top_performers",
            description="Get top performing campaigns, offers, or traffic sources by profit, revenue, or ROI",
            inputSchema={
                "type": "object",
                "properties": {
                    "entity_type": {
                        "type": "string",
                        "enum": ["campaign", "offer", "traffic-source", "country"],
                        "description": "What to rank",
                        "default": "campaign"
                    },
                    "metric": {
                        "type": "string",
                        "enum": ["profit", "revenue", "roi", "conversions"],
                        "description": "Metric to rank by",
                        "default": "profit"
                    },
                    "days_back": {
                        "type": "integer",
                        "description": "Time period in days",
                        "default": 7
                    },
                    "limit": {
                        "type": "integer",
                        "default": 10
                    }
                }
            }
        ),
        Tool(
            name="voluum_worst_performers",
            description="Get worst performing campaigns, offers, or traffic sources (losing money)",
            inputSchema={
                "type": "object",
                "properties": {
                    "entity_type": {
                        "type": "string",
                        "enum": ["campaign", "offer", "traffic-source", "country"],
                        "default": "campaign"
                    },
                    "days_back": {
                        "type": "integer",
                        "default": 7
                    },
                    "limit": {
                        "type": "integer",
                        "default": 10
                    }
                }
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> CallToolResult:
    """Handle tool calls"""
    try:
        client = get_client()

        if name == "voluum_report":
            # Handle date parameters
            from_date = arguments.get("from_date")
            to_date = arguments.get("to_date")
            days_back = arguments.get("days_back")

            if days_back:
                to_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")
                from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
            elif from_date:
                from_date = f"{from_date}T00:00:00Z"
                to_date = f"{to_date or from_date}T23:59:59Z" if to_date else f"{from_date.split('T')[0]}T23:59:59Z"

            # Build filters
            filters = {}
            if arguments.get("campaign_id"):
                filters["filter1"] = f"campaign:{arguments['campaign_id']}"
            if arguments.get("offer_id"):
                filters["filter2"] = f"offer:{arguments['offer_id']}"
            if arguments.get("traffic_source_id"):
                filters["filter3"] = f"traffic-source:{arguments['traffic_source_id']}"

            result = await client.get_report(
                group_by=arguments.get("group_by", "campaign"),
                from_date=from_date,
                to_date=to_date,
                filters=filters if filters else None,
                sort=arguments.get("sort_by", "profit"),
                direction=arguments.get("sort_direction", "DESC"),
                limit=arguments.get("limit", 50)
            )

            # Format output
            rows = result.get("rows", [])
            total = result.get("totals", {})

            output = [f"## Voluum Report - Group by {arguments.get('group_by', 'campaign')}\n"]

            if total:
                output.append("### Totals")
                output.append(format_report_row(total))
                output.append("")

            output.append(f"### Results ({len(rows)} rows)\n")
            for i, row in enumerate(rows, 1):
                output.append(f"{i}. {format_report_row(row)}\n")

            return CallToolResult(content=[TextContent(type="text", text="\n".join(output))])

        elif name == "voluum_campaigns":
            result = await client.get_campaigns(limit=arguments.get("limit", 100))
            campaigns = result.get("campaigns", [])

            output = [f"## Voluum Campaigns ({len(campaigns)} total)\n"]
            for c in campaigns:
                status = "🟢" if c.get("status") == "ACTIVE" else "🔴"
                output.append(f"- {status} **{c.get('name')}**")
                output.append(f"  ID: `{c.get('id')}`")
                if c.get("trafficSourceName"):
                    output.append(f"  Traffic Source: {c.get('trafficSourceName')}")
                output.append("")

            return CallToolResult(content=[TextContent(type="text", text="\n".join(output))])

        elif name == "voluum_offers":
            result = await client.get_offers(limit=arguments.get("limit", 100))
            offers = result.get("offers", [])

            output = [f"## Voluum Offers ({len(offers)} total)\n"]
            for o in offers:
                output.append(f"- **{o.get('name')}**")
                output.append(f"  ID: `{o.get('id')}`")
                if o.get("affiliateNetworkName"):
                    output.append(f"  Network: {o.get('affiliateNetworkName')}")
                if o.get("payout"):
                    output.append(f"  Payout: {format_currency(o.get('payout', 0))}")
                output.append("")

            return CallToolResult(content=[TextContent(type="text", text="\n".join(output))])

        elif name == "voluum_traffic_sources":
            result = await client.get_traffic_sources(limit=arguments.get("limit", 100))
            sources = result.get("trafficSources", [])

            output = [f"## Voluum Traffic Sources ({len(sources)} total)\n"]
            for ts in sources:
                output.append(f"- **{ts.get('name')}**")
                output.append(f"  ID: `{ts.get('id')}`")
                output.append("")

            return CallToolResult(content=[TextContent(type="text", text="\n".join(output))])

        elif name == "voluum_landers":
            result = await client.get_landers(limit=arguments.get("limit", 100))
            landers = result.get("landers", [])

            output = [f"## Voluum Landers ({len(landers)} total)\n"]
            for l in landers:
                output.append(f"- **{l.get('name')}**")
                output.append(f"  ID: `{l.get('id')}`")
                output.append("")

            return CallToolResult(content=[TextContent(type="text", text="\n".join(output))])

        elif name == "voluum_conversions":
            days_back = arguments.get("days_back")
            from_date = arguments.get("from_date")
            to_date = arguments.get("to_date")

            if days_back:
                to_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")
                from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
            elif from_date:
                from_date = f"{from_date}T00:00:00Z"
                to_date = f"{to_date}T23:59:59Z" if to_date else f"{from_date.split('T')[0]}T23:59:59Z"

            result = await client.get_conversions(
                from_date=from_date,
                to_date=to_date,
                campaign_id=arguments.get("campaign_id"),
                limit=arguments.get("limit", 100)
            )

            conversions = result.get("conversions", [])
            output = [f"## Recent Conversions ({len(conversions)} found)\n"]

            for conv in conversions:
                output.append(f"- {conv.get('visitTimestamp', 'N/A')}")
                output.append(f"  Campaign: {conv.get('campaignName', 'N/A')}")
                output.append(f"  Offer: {conv.get('offerName', 'N/A')}")
                output.append(f"  Revenue: {format_currency(conv.get('revenue', 0))}")
                output.append("")

            return CallToolResult(content=[TextContent(type="text", text="\n".join(output))])

        elif name == "voluum_summary":
            days = arguments.get("days_back", 1)
            to_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")

            result = await client.get_report(
                group_by="all",
                from_date=from_date,
                to_date=to_date
            )

            total = result.get("totals", {})
            period = "Today" if days == 1 else f"Last {days} days"

            output = [
                f"## Voluum Summary - {period}\n",
                f"**Visits:** {total.get('visits', 0):,}",
                f"**Clicks:** {total.get('clicks', 0):,}",
                f"**Conversions:** {total.get('conversions', 0):,}",
                f"**Revenue:** {format_currency(total.get('revenue', 0))}",
                f"**Cost:** {format_currency(total.get('cost', 0))}",
                f"**Profit:** {format_currency(total.get('profit', 0))}",
                f"**ROI:** {total.get('roi', 0):.1f}%",
                f"**CR:** {total.get('cr', 0):.2f}%"
            ]

            return CallToolResult(content=[TextContent(type="text", text="\n".join(output))])

        elif name == "voluum_top_performers":
            entity = arguments.get("entity_type", "campaign")
            metric = arguments.get("metric", "profit")
            days = arguments.get("days_back", 7)
            limit = arguments.get("limit", 10)

            to_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")

            result = await client.get_report(
                group_by=entity,
                from_date=from_date,
                to_date=to_date,
                sort=metric,
                direction="DESC",
                limit=limit
            )

            rows = result.get("rows", [])
            output = [f"## Top {limit} {entity.title()}s by {metric.title()} (Last {days} days)\n"]

            for i, row in enumerate(rows, 1):
                output.append(f"{i}. {format_report_row(row)}\n")

            return CallToolResult(content=[TextContent(type="text", text="\n".join(output))])

        elif name == "voluum_worst_performers":
            entity = arguments.get("entity_type", "campaign")
            days = arguments.get("days_back", 7)
            limit = arguments.get("limit", 10)

            to_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")

            result = await client.get_report(
                group_by=entity,
                from_date=from_date,
                to_date=to_date,
                sort="profit",
                direction="ASC",
                limit=limit
            )

            rows = result.get("rows", [])
            # Filter to only show negative profit
            losers = [r for r in rows if r.get("profit", 0) < 0]

            output = [f"## Worst {entity.title()}s (Losing Money) - Last {days} days\n"]

            if not losers:
                output.append("No losing campaigns found! 🎉")
            else:
                for i, row in enumerate(losers[:limit], 1):
                    output.append(f"{i}. {format_report_row(row)}\n")

            return CallToolResult(content=[TextContent(type="text", text="\n".join(output))])

        else:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Unknown tool: {name}")],
                isError=True
            )

    except Exception as e:
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error: {str(e)}")],
            isError=True
        )


async def main():
    """Run the MCP server"""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
