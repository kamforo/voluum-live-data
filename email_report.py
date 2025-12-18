#!/usr/bin/env python3
"""
Email Daily Pattern Report via SendGrid

Usage:
    python email_report.py                          # Send full report
    python email_report.py --var3 7933086          # Report for specific campaign
    python email_report.py --to other@email.com    # Send to different email

Environment variables required:
    SENDGRID_API_KEY - Your SendGrid API key
    REPORT_EMAIL_TO - Recipient email address
    REPORT_EMAIL_FROM - Sender email address (must be verified in SendGrid)
"""

import os
import argparse
from datetime import datetime, timedelta
from io import StringIO
import sys

from dotenv import load_dotenv
load_dotenv()

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

from supabase import create_client
import pandas as pd


def load_conversions(days_back: int = 30):
    """Load conversions from Supabase"""
    supabase = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_SERVICE_KEY')
    )

    cutoff = (datetime.utcnow() - timedelta(days=days_back)).isoformat()

    all_data = []
    offset = 0
    while True:
        batch = supabase.table('conversions').select(
            'revenue,custom_var_3,country_code,device,os,browser,isp,connection_type,postback_timestamp'
        ).gte('postback_timestamp', cutoff).range(offset, offset + 999).execute()

        if not batch.data:
            break
        all_data.extend(batch.data)
        offset += 1000
        if len(batch.data) < 1000:
            break

    df = pd.DataFrame(all_data)
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
    df['custom_var_3'] = df['custom_var_3'].fillna('unknown')

    return df


def analyze_feature_patterns(df, feature, var3=None, min_samples=20):
    """Analyze patterns for a single feature"""
    if var3:
        df = df[df['custom_var_3'] == var3]

    if len(df) < min_samples:
        return []

    median_rev = df['revenue'].median()

    patterns = []
    for value, group in df.groupby(feature):
        if len(group) >= min_samples and value and value != 'unknown':
            avg_rev = group['revenue'].mean()
            total_rev = group['revenue'].sum()
            deviation = (avg_rev - median_rev) / median_rev * 100 if median_rev > 0 else 0

            patterns.append({
                'value': value,
                'count': len(group),
                'avg_revenue': avg_rev,
                'total_revenue': total_rev,
                'deviation': deviation
            })

    return sorted(patterns, key=lambda x: x['avg_revenue'], reverse=True)


def generate_html_report(df, var3_filter=None):
    """Generate HTML email report"""
    total_rev = df['revenue'].sum()
    avg_rev = df['revenue'].mean()
    median_rev = df['revenue'].median()

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
            h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
            h2 {{ color: #555; margin-top: 30px; }}
            .stats {{ background: #f5f5f5; padding: 15px; border-radius: 5px; margin: 15px 0; }}
            .good {{ color: #2e7d32; }}
            .bad {{ color: #c62828; }}
            table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background: #4CAF50; color: white; }}
            tr:nth-child(even) {{ background: #f9f9f9; }}
            .scale {{ background: #e8f5e9; }}
            .block {{ background: #ffebee; }}
        </style>
    </head>
    <body>
        <h1>📊 Voluum Daily Pattern Report</h1>
        <p><strong>Generated:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>

        <div class="stats">
            <strong>Last 30 Days:</strong> {len(df):,} conversions |
            <strong>Revenue:</strong> ${total_rev:,.2f} |
            <strong>Avg:</strong> ${avg_rev:.2f} |
            <strong>Median:</strong> ${median_rev:.2f}
        </div>
    """

    # VAR3 Rankings
    if not var3_filter:
        var3_stats = df.groupby('custom_var_3').agg({
            'revenue': ['count', 'sum', 'mean']
        }).reset_index()
        var3_stats.columns = ['var3', 'conversions', 'total_rev', 'avg_rev']
        var3_stats = var3_stats[var3_stats['conversions'] >= 50]
        var3_stats = var3_stats.sort_values('avg_rev', ascending=False)

        html += """
        <h2>🏆 Top VAR3 Campaigns</h2>
        <table>
            <tr><th>VAR3</th><th>Conversions</th><th>Revenue</th><th>Avg Rev</th></tr>
        """
        for _, row in var3_stats.head(10).iterrows():
            html += f"<tr class='scale'><td>{row['var3']}</td><td>{int(row['conversions'])}</td><td>${row['total_rev']:,.2f}</td><td>${row['avg_rev']:.2f}</td></tr>"
        html += "</table>"

        html += """
        <h2>⚠️ Bottom VAR3 Campaigns</h2>
        <table>
            <tr><th>VAR3</th><th>Conversions</th><th>Revenue</th><th>Avg Rev</th></tr>
        """
        for _, row in var3_stats.tail(10).iterrows():
            html += f"<tr class='block'><td>{row['var3']}</td><td>{int(row['conversions'])}</td><td>${row['total_rev']:,.2f}</td><td>${row['avg_rev']:.2f}</td></tr>"
        html += "</table>"

    # Country patterns
    country_patterns = analyze_feature_patterns(df, 'country_code', var3_filter, min_samples=30)
    if country_patterns:
        best = [p for p in country_patterns if p['deviation'] > 20][:5]
        worst = [p for p in country_patterns if p['deviation'] < -20][-5:]

        if best:
            html += """
            <h2>🟢 Countries to SCALE</h2>
            <table>
                <tr><th>Country</th><th>Conversions</th><th>Avg Revenue</th><th>vs Median</th></tr>
            """
            for p in best:
                html += f"<tr class='scale'><td>{p['value']}</td><td>{p['count']}</td><td>${p['avg_revenue']:.2f}</td><td>+{p['deviation']:.0f}%</td></tr>"
            html += "</table>"

        if worst:
            html += """
            <h2>🔴 Countries to BLOCK</h2>
            <table>
                <tr><th>Country</th><th>Conversions</th><th>Avg Revenue</th><th>vs Median</th></tr>
            """
            for p in worst:
                html += f"<tr class='block'><td>{p['value']}</td><td>{p['count']}</td><td>${p['avg_revenue']:.2f}</td><td>{p['deviation']:.0f}%</td></tr>"
            html += "</table>"

    # ISP patterns
    isp_patterns = analyze_feature_patterns(df, 'isp', var3_filter, min_samples=20)
    if isp_patterns:
        html += """
        <h2>📡 Top ISPs</h2>
        <table>
            <tr><th>ISP</th><th>Conversions</th><th>Avg Revenue</th><th>vs Median</th></tr>
        """
        for p in isp_patterns[:10]:
            html += f"<tr class='scale'><td>{p['value'][:40]}</td><td>{p['count']}</td><td>${p['avg_revenue']:.2f}</td><td>+{p['deviation']:.0f}%</td></tr>"
        html += "</table>"

        html += """
        <h2>📡 Bottom ISPs</h2>
        <table>
            <tr><th>ISP</th><th>Conversions</th><th>Avg Revenue</th><th>vs Median</th></tr>
        """
        for p in isp_patterns[-10:]:
            html += f"<tr class='block'><td>{p['value'][:40]}</td><td>{p['count']}</td><td>${p['avg_revenue']:.2f}</td><td>{p['deviation']:.0f}%</td></tr>"
        html += "</table>"

    # Device/OS patterns
    device_patterns = analyze_feature_patterns(df, 'device', var3_filter, min_samples=30)
    os_patterns = analyze_feature_patterns(df, 'os', var3_filter, min_samples=30)

    html += "<h2>📱 Device & OS Performance</h2><table><tr><th>Dimension</th><th>Value</th><th>Conversions</th><th>Avg Rev</th><th>vs Median</th></tr>"

    for p in device_patterns:
        color = 'scale' if p['deviation'] > 20 else 'block' if p['deviation'] < -20 else ''
        html += f"<tr class='{color}'><td>Device</td><td>{p['value']}</td><td>{p['count']}</td><td>${p['avg_revenue']:.2f}</td><td>{p['deviation']:+.0f}%</td></tr>"

    for p in os_patterns[:8]:
        color = 'scale' if p['deviation'] > 20 else 'block' if p['deviation'] < -20 else ''
        html += f"<tr class='{color}'><td>OS</td><td>{p['value']}</td><td>{p['count']}</td><td>${p['avg_revenue']:.2f}</td><td>{p['deviation']:+.0f}%</td></tr>"

    html += "</table>"

    # Recommended actions
    all_patterns = []
    for feature in ['country_code', 'device', 'os', 'isp']:
        for p in analyze_feature_patterns(df, feature, var3_filter, min_samples=30):
            p['feature'] = feature
            all_patterns.append(p)

    best_opps = sorted([p for p in all_patterns if p['deviation'] > 30], key=lambda x: x['total_revenue'], reverse=True)[:5]
    worst_opps = sorted([p for p in all_patterns if p['deviation'] < -30], key=lambda x: x['total_revenue'], reverse=True)[:5]

    html += "<h2>💰 Recommended Actions</h2>"

    if best_opps:
        html += "<h3 class='good'>Scale These:</h3><ul>"
        for p in best_opps:
            html += f"<li><strong>{p['feature']}={p['value'][:25]}</strong>: ${p['total_revenue']:,.0f} revenue, ${p['avg_revenue']:.2f} avg (+{p['deviation']:.0f}%)</li>"
        html += "</ul>"

    if worst_opps:
        html += "<h3 class='bad'>Block These:</h3><ul>"
        for p in worst_opps:
            html += f"<li><strong>{p['feature']}={p['value'][:25]}</strong>: ${p['total_revenue']:,.0f} wasted, ${p['avg_revenue']:.2f} avg ({p['deviation']:.0f}%)</li>"
        html += "</ul>"

    html += """
        <hr>
        <p style="color: #888; font-size: 12px;">
            This report was automatically generated by Voluum Pattern Analyzer.<br>
            Data source: Last 30 days of conversion data.
        </p>
    </body>
    </html>
    """

    return html


def send_email(html_content: str, to_email: str = None, subject: str = None):
    """Send email via SendGrid"""
    api_key = os.getenv('SENDGRID_API_KEY')
    from_email = os.getenv('REPORT_EMAIL_FROM')
    to_email = to_email or os.getenv('REPORT_EMAIL_TO')

    if not api_key:
        raise ValueError("SENDGRID_API_KEY environment variable required")
    if not from_email:
        raise ValueError("REPORT_EMAIL_FROM environment variable required")
    if not to_email:
        raise ValueError("REPORT_EMAIL_TO environment variable required")

    if not subject:
        subject = f"📊 Voluum Daily Report - {datetime.utcnow().strftime('%Y-%m-%d')}"

    message = Mail(
        from_email=Email(from_email),
        to_emails=To(to_email),
        subject=subject,
        html_content=Content("text/html", html_content)
    )

    sg = SendGridAPIClient(api_key)
    response = sg.send(message)

    return response.status_code


def main():
    parser = argparse.ArgumentParser(description='Email Daily Pattern Report')
    parser.add_argument('--var3', type=str, help='Filter by specific VAR3 campaign')
    parser.add_argument('--to', type=str, help='Recipient email (overrides REPORT_EMAIL_TO)')
    parser.add_argument('--days', type=int, default=30, help='Days of data (default: 30)')
    parser.add_argument('--dry-run', action='store_true', help='Generate report without sending')
    args = parser.parse_args()

    print("Loading data...")
    df = load_conversions(days_back=args.days)
    print(f"Loaded {len(df):,} conversions")

    print("Generating report...")
    html = generate_html_report(df, var3_filter=args.var3)

    if args.dry_run:
        print("\n--- DRY RUN - Report Preview ---")
        # Save to file for preview
        with open('/tmp/report_preview.html', 'w') as f:
            f.write(html)
        print("Report saved to /tmp/report_preview.html")
        return

    print("Sending email...")
    try:
        status_code = send_email(html, to_email=args.to)
        if status_code == 202:
            print(f"✅ Email sent successfully to {args.to or os.getenv('REPORT_EMAIL_TO')}")
        else:
            print(f"⚠️ Email sent with status code: {status_code}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
