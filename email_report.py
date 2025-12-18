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
from sendgrid.helpers.mail import Mail, Email, To, Content, Attachment, FileContent, FileName, FileType, Disposition

from supabase import create_client
import pandas as pd
import base64
from io import BytesIO


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


def analyze_feature_patterns(df, feature, min_samples=20):
    """Analyze patterns for a single feature"""
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


def generate_excel_report(df, min_var3_conversions=50):
    """Generate Excel report with multiple sheets"""
    output = BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book

        # Formats
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#2196F3', 'font_color': 'white', 'border': 1})
        money_fmt = workbook.add_format({'num_format': '$#,##0.00', 'border': 1})
        pct_fmt = workbook.add_format({'num_format': '+0%;-0%', 'border': 1})
        good_fmt = workbook.add_format({'bg_color': '#e8f5e9', 'border': 1})
        bad_fmt = workbook.add_format({'bg_color': '#ffebee', 'border': 1})
        cell_fmt = workbook.add_format({'border': 1})

        # Sheet 1: VAR3 Overview
        var3_stats = df.groupby('custom_var_3').agg({
            'revenue': ['count', 'sum', 'mean']
        }).reset_index()
        var3_stats.columns = ['VAR3', 'Conversions', 'Total Revenue', 'Avg Revenue']
        var3_stats = var3_stats[var3_stats['Conversions'] >= min_var3_conversions]
        var3_stats = var3_stats.sort_values('Total Revenue', ascending=False)

        overall_avg = df['revenue'].mean()
        var3_stats['vs Overall Avg'] = (var3_stats['Avg Revenue'] - overall_avg) / overall_avg
        var3_stats['Status'] = var3_stats['Avg Revenue'].apply(
            lambda x: 'SCALE' if x > overall_avg * 1.2 else 'BLOCK' if x < overall_avg * 0.8 else 'OK'
        )

        var3_stats.to_excel(writer, sheet_name='VAR3 Overview', index=False, startrow=1)
        ws = writer.sheets['VAR3 Overview']
        for col, val in enumerate(var3_stats.columns):
            ws.write(0, col, val, header_fmt)
        ws.set_column('A:A', 15)
        ws.set_column('B:B', 12)
        ws.set_column('C:D', 14)
        ws.set_column('E:F', 12)

        # Sheet 2: Per-VAR3 Patterns (all in one sheet for easy filtering)
        patterns_data = []

        for var3 in var3_stats['VAR3'].tolist():
            subset = df[df['custom_var_3'] == var3]
            median_rev = subset['revenue'].median()

            for feature in ['country_code', 'device', 'os', 'isp']:
                min_samples = max(5, len(subset) // 20)
                patterns = analyze_feature_patterns(subset, feature, min_samples=min_samples)

                for p in patterns:
                    if abs(p['deviation']) > 25:  # Only significant patterns
                        patterns_data.append({
                            'VAR3': var3,
                            'Dimension': feature.replace('_', ' ').title(),
                            'Value': p['value'][:30],
                            'Conversions': p['count'],
                            'Avg Revenue': p['avg_revenue'],
                            'Total Revenue': p['total_revenue'],
                            'vs Campaign Median': p['deviation'] / 100,
                            'Action': 'SCALE' if p['deviation'] > 25 else 'BLOCK'
                        })

        if patterns_data:
            patterns_df = pd.DataFrame(patterns_data)
            patterns_df = patterns_df.sort_values(['VAR3', 'Dimension', 'vs Campaign Median'], ascending=[True, True, False])
            patterns_df.to_excel(writer, sheet_name='Patterns by VAR3', index=False, startrow=1)

            ws2 = writer.sheets['Patterns by VAR3']
            for col, val in enumerate(patterns_df.columns):
                ws2.write(0, col, val, header_fmt)
            ws2.set_column('A:A', 12)
            ws2.set_column('B:B', 12)
            ws2.set_column('C:C', 25)
            ws2.set_column('D:H', 14)
            ws2.autofilter(0, 0, len(patterns_df), len(patterns_df.columns) - 1)

        # Sheet 3: Top Scale Opportunities (sorted by total revenue impact)
        scale_patterns = [p for p in patterns_data if p['Action'] == 'SCALE']
        if scale_patterns:
            scale_df = pd.DataFrame(scale_patterns).sort_values('Total Revenue', ascending=False).head(50)
            scale_df.to_excel(writer, sheet_name='Top Scale Opps', index=False, startrow=1)
            ws3 = writer.sheets['Top Scale Opps']
            for col, val in enumerate(scale_df.columns):
                ws3.write(0, col, val, header_fmt)

        # Sheet 4: Top Block Targets (sorted by total revenue wasted)
        block_patterns = [p for p in patterns_data if p['Action'] == 'BLOCK']
        if block_patterns:
            block_df = pd.DataFrame(block_patterns).sort_values('Total Revenue', ascending=False).head(50)
            block_df.to_excel(writer, sheet_name='Top Block Targets', index=False, startrow=1)
            ws4 = writer.sheets['Top Block Targets']
            for col, val in enumerate(block_df.columns):
                ws4.write(0, col, val, header_fmt)

    output.seek(0)
    return output.getvalue()


def analyze_var3_patterns(df, var3, min_samples=10):
    """Analyze patterns within a specific VAR3 campaign"""
    subset = df[df['custom_var_3'] == var3]
    if len(subset) < min_samples:
        return None

    median_rev = subset['revenue'].median()
    results = {'var3': var3, 'conversions': len(subset), 'revenue': subset['revenue'].sum(),
               'avg_rev': subset['revenue'].mean(), 'median_rev': median_rev, 'patterns': {}}

    for feature in ['country_code', 'device', 'os', 'isp']:
        patterns = analyze_feature_patterns(subset, feature, min_samples=max(5, len(subset) // 20))
        if patterns:
            # Get patterns with significant deviation (>25% above or below)
            scale = [p for p in patterns if p['deviation'] > 25][:3]
            block = [p for p in patterns if p['deviation'] < -25][-3:]
            if scale or block:
                results['patterns'][feature] = {'scale': scale, 'block': block}

    return results


def generate_html_report(df, var3_filter=None, min_var3_conversions=50):
    """Generate HTML email report with per-VAR3 actionable insights"""

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; background: #fafafa; }}
            h1 {{ color: #333; border-bottom: 3px solid #2196F3; padding-bottom: 10px; }}
            h2 {{ color: #1976D2; margin-top: 30px; background: #e3f2fd; padding: 10px; border-radius: 5px; }}
            h3 {{ color: #555; margin: 15px 0 5px 0; }}
            .summary {{ background: #fff; padding: 15px; border-radius: 5px; margin: 15px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            .var3-card {{ background: #fff; padding: 15px; margin: 15px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); border-left: 4px solid #2196F3; }}
            .var3-header {{ display: flex; justify-content: space-between; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-bottom: 10px; }}
            .var3-title {{ font-size: 18px; font-weight: bold; color: #1976D2; }}
            .var3-stats {{ color: #666; font-size: 14px; }}
            .pattern-section {{ margin: 10px 0; }}
            .scale {{ color: #2e7d32; }}
            .block {{ color: #c62828; }}
            .pattern-item {{ padding: 4px 0; font-size: 14px; }}
            .pattern-item strong {{ min-width: 80px; display: inline-block; }}
            .no-patterns {{ color: #999; font-style: italic; }}
            table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; font-size: 13px; }}
            th {{ background: #f5f5f5; }}
            .top {{ background: #e8f5e9; }}
            .bottom {{ background: #ffebee; }}
        </style>
    </head>
    <body>
        <h1>📊 Voluum Pattern Report - Per Campaign Insights</h1>
        <p><strong>Generated:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
    """

    # Get VAR3 campaigns with enough data
    var3_stats = df.groupby('custom_var_3').agg({
        'revenue': ['count', 'sum', 'mean']
    }).reset_index()
    var3_stats.columns = ['var3', 'conversions', 'total_rev', 'avg_rev']
    var3_stats = var3_stats[var3_stats['conversions'] >= min_var3_conversions]
    var3_stats = var3_stats.sort_values('total_rev', ascending=False)

    html += f"""
        <div class="summary">
            <strong>Analyzing {len(var3_stats)} VAR3 campaigns</strong> with {min_var3_conversions}+ conversions<br>
            Total: {len(df):,} conversions | ${df['revenue'].sum():,.2f} revenue
        </div>
    """

    # Quick overview table
    html += """
        <h2>📈 VAR3 Campaign Overview</h2>
        <table>
            <tr><th>VAR3</th><th>Convs</th><th>Revenue</th><th>Avg Rev</th><th>Status</th></tr>
    """

    overall_avg = df['revenue'].mean()
    for _, row in var3_stats.head(20).iterrows():
        status_class = 'top' if row['avg_rev'] > overall_avg * 1.2 else 'bottom' if row['avg_rev'] < overall_avg * 0.8 else ''
        status = '🟢 Above avg' if row['avg_rev'] > overall_avg * 1.2 else '🔴 Below avg' if row['avg_rev'] < overall_avg * 0.8 else '➖ Average'
        html += f"<tr class='{status_class}'><td>{row['var3']}</td><td>{int(row['conversions'])}</td><td>${row['total_rev']:,.2f}</td><td>${row['avg_rev']:.2f}</td><td>{status}</td></tr>"
    html += "</table>"

    # Detailed per-VAR3 analysis
    html += "<h2>🎯 Actionable Insights Per Campaign</h2>"

    campaigns_with_insights = 0
    for _, row in var3_stats.iterrows():
        var3 = row['var3']
        analysis = analyze_var3_patterns(df, var3)

        if not analysis or not analysis['patterns']:
            continue

        campaigns_with_insights += 1

        html += f"""
        <div class="var3-card">
            <div class="var3-header">
                <span class="var3-title">VAR3: {var3}</span>
                <span class="var3-stats">{int(row['conversions'])} convs | ${row['total_rev']:,.2f} rev | ${row['avg_rev']:.2f} avg</span>
            </div>
        """

        for feature, patterns in analysis['patterns'].items():
            feature_label = {'country_code': '🌍 Country', 'device': '📱 Device', 'os': '💻 OS', 'isp': '📡 ISP'}.get(feature, feature)

            if patterns['scale'] or patterns['block']:
                html += f"<div class='pattern-section'><strong>{feature_label}:</strong><br>"

                for p in patterns['scale']:
                    html += f"<span class='pattern-item scale'>✅ <strong>{p['value'][:25]}</strong>: ${p['avg_revenue']:.2f} avg (+{p['deviation']:.0f}%) - {p['count']} convs</span><br>"

                for p in patterns['block']:
                    html += f"<span class='pattern-item block'>❌ <strong>{p['value'][:25]}</strong>: ${p['avg_revenue']:.2f} avg ({p['deviation']:.0f}%) - {p['count']} convs</span><br>"

                html += "</div>"

        html += "</div>"

    if campaigns_with_insights == 0:
        html += "<p class='no-patterns'>No significant patterns found. Need more conversion data for meaningful insights.</p>"

    html += f"""
        <hr>
        <p style="color: #888; font-size: 12px;">
            Report shows patterns with >25% deviation from campaign median.<br>
            Analyzed {len(var3_stats)} campaigns with {min_var3_conversions}+ conversions each.
        </p>
    </body>
    </html>
    """

    return html


def send_email_with_excel(excel_data: bytes, summary: dict, to_email: str = None, subject: str = None):
    """Send email with Excel attachment via SendGrid"""
    api_key = os.getenv('SENDGRID_API_KEY')
    from_email = os.getenv('REPORT_EMAIL_FROM')
    to_email = to_email or os.getenv('REPORT_EMAIL_TO')

    if not api_key:
        raise ValueError("SENDGRID_API_KEY environment variable required")
    if not from_email:
        raise ValueError("REPORT_EMAIL_FROM environment variable required")
    if not to_email:
        raise ValueError("REPORT_EMAIL_TO environment variable required")

    date_str = datetime.utcnow().strftime('%Y-%m-%d')
    if not subject:
        subject = f"Voluum Pattern Report - {date_str}"

    # Simple email body with summary
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2>Voluum Pattern Report - {date_str}</h2>
        <p>Your Excel report is attached with the following sheets:</p>
        <ul>
            <li><strong>VAR3 Overview</strong> - {summary['var3_count']} campaigns ranked by revenue</li>
            <li><strong>Patterns by VAR3</strong> - {summary['pattern_count']} actionable patterns (filterable)</li>
            <li><strong>Top Scale Opps</strong> - Best performing segments to scale</li>
            <li><strong>Top Block Targets</strong> - Worst performing segments to block</li>
        </ul>
        <p style="color: #666;">
            <strong>Data:</strong> {summary['total_conversions']:,} conversions | ${summary['total_revenue']:,.2f} revenue<br>
            <strong>Period:</strong> Last 30 days
        </p>
    </body>
    </html>
    """

    message = Mail(
        from_email=Email(from_email),
        to_emails=To(to_email),
        subject=subject,
        html_content=Content("text/html", html_body)
    )

    # Attach Excel file
    encoded_file = base64.b64encode(excel_data).decode()
    attachment = Attachment(
        FileContent(encoded_file),
        FileName(f'voluum_report_{date_str}.xlsx'),
        FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
        Disposition('attachment')
    )
    message.attachment = attachment

    sg = SendGridAPIClient(api_key)
    response = sg.send(message)

    return response.status_code


def main():
    parser = argparse.ArgumentParser(description='Email Daily Pattern Report')
    parser.add_argument('--to', type=str, help='Recipient email (overrides REPORT_EMAIL_TO)')
    parser.add_argument('--days', type=int, default=30, help='Days of data (default: 30)')
    parser.add_argument('--min-conversions', type=int, default=50, help='Min conversions per VAR3 (default: 50)')
    parser.add_argument('--dry-run', action='store_true', help='Generate report without sending')
    args = parser.parse_args()

    print("Loading data...")
    df = load_conversions(days_back=args.days)
    print(f"Loaded {len(df):,} conversions")

    print("Generating Excel report...")
    excel_data = generate_excel_report(df, min_var3_conversions=args.min_conversions)

    # Count patterns for summary
    var3_count = len(df[df['custom_var_3'] != 'unknown']['custom_var_3'].unique())
    pattern_count = 0
    for var3 in df['custom_var_3'].unique():
        subset = df[df['custom_var_3'] == var3]
        if len(subset) >= args.min_conversions:
            for feature in ['country_code', 'device', 'os', 'isp']:
                patterns = analyze_feature_patterns(subset, feature, min_samples=max(5, len(subset) // 20))
                pattern_count += len([p for p in patterns if abs(p['deviation']) > 25])

    summary = {
        'var3_count': var3_count,
        'pattern_count': pattern_count,
        'total_conversions': len(df),
        'total_revenue': df['revenue'].sum()
    }

    if args.dry_run:
        print("\n--- DRY RUN - Report Preview ---")
        filename = '/tmp/voluum_report.xlsx'
        with open(filename, 'wb') as f:
            f.write(excel_data)
        print(f"Excel saved to {filename}")
        print(f"Summary: {summary}")
        return

    print("Sending email with Excel attachment...")
    try:
        status_code = send_email_with_excel(excel_data, summary, to_email=args.to)
        if status_code == 202:
            print(f"✅ Email sent successfully to {args.to or os.getenv('REPORT_EMAIL_TO')}")
        else:
            print(f"⚠️ Email sent with status code: {status_code}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
