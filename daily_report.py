#!/usr/bin/env python3
"""
Daily Pattern Report for Voluum Conversions
Generates actionable insights for media buyers

Usage:
    python daily_report.py                  # Full report
    python daily_report.py --var3 7933086  # Specific campaign
"""

import os
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier

from supabase import create_client


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
    df['postback_timestamp'] = pd.to_datetime(df['postback_timestamp'])

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
            pct_high = (group['revenue'] > median_rev).mean() * 100

            # Calculate deviation from median
            deviation = (avg_rev - median_rev) / median_rev * 100 if median_rev > 0 else 0

            patterns.append({
                'value': value,
                'count': len(group),
                'avg_revenue': avg_rev,
                'total_revenue': total_rev,
                'pct_high': pct_high,
                'deviation': deviation
            })

    return sorted(patterns, key=lambda x: x['avg_revenue'], reverse=True)


def analyze_two_feature_patterns(df, feat1, feat2, var3=None, min_samples=15):
    """Analyze patterns for two features combined"""
    if var3:
        df = df[df['custom_var_3'] == var3]

    if len(df) < min_samples:
        return []

    median_rev = df['revenue'].median()
    mean_rev = df['revenue'].mean()

    patterns = []
    for (v1, v2), group in df.groupby([feat1, feat2]):
        if len(group) >= min_samples and v1 and v2 and v1 != 'unknown' and v2 != 'unknown':
            avg_rev = group['revenue'].mean()
            pct_high = (group['revenue'] > median_rev).mean() * 100

            # Only include if significantly different from mean
            if abs(avg_rev - mean_rev) / mean_rev > 0.2:  # 20% deviation threshold
                patterns.append({
                    'feat1': v1,
                    'feat2': v2,
                    'count': len(group),
                    'avg_revenue': avg_rev,
                    'pct_high': pct_high,
                    'deviation': (avg_rev - mean_rev) / mean_rev * 100
                })

    return sorted(patterns, key=lambda x: x['avg_revenue'], reverse=True)


def print_report(df, var3_filter=None):
    """Print the daily report"""
    print("=" * 80)
    print("VOLUUM DAILY PATTERN REPORT")
    print(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Data: Last 30 days | Total Conversions: {len(df):,}")
    print("=" * 80)

    # Overall stats
    total_rev = df['revenue'].sum()
    avg_rev = df['revenue'].mean()
    median_rev = df['revenue'].median()
    print(f"\nOVERALL: Revenue ${total_rev:,.2f} | Avg ${avg_rev:.2f} | Median ${median_rev:.2f}")

    # VAR3 Campaign Rankings
    if not var3_filter:
        print("\n" + "─" * 80)
        print("VAR3 CAMPAIGN RANKINGS")
        print("─" * 80)

        var3_stats = df.groupby('custom_var_3').agg({
            'revenue': ['count', 'sum', 'mean']
        }).reset_index()
        var3_stats.columns = ['var3', 'conversions', 'total_rev', 'avg_rev']
        var3_stats = var3_stats[var3_stats['conversions'] >= 50]
        var3_stats = var3_stats.sort_values('avg_rev', ascending=False)

        print("\n🟢 TOP 10 VAR3 (Highest Avg Revenue):")
        print(f"{'VAR3':<15} {'Convs':>8} {'Total Rev':>12} {'Avg Rev':>10}")
        for _, row in var3_stats.head(10).iterrows():
            print(f"{row['var3']:<15} {int(row['conversions']):>8} ${row['total_rev']:>10,.2f} ${row['avg_rev']:>9.2f}")

        print("\n🔴 BOTTOM 10 VAR3 (Lowest Avg Revenue):")
        print(f"{'VAR3':<15} {'Convs':>8} {'Total Rev':>12} {'Avg Rev':>10}")
        for _, row in var3_stats.tail(10).iterrows():
            print(f"{row['var3']:<15} {int(row['conversions']):>8} ${row['total_rev']:>10,.2f} ${row['avg_rev']:>9.2f}")

    # Analyze by single features
    features = ['country_code', 'device', 'os', 'isp', 'connection_type']

    for feature in features:
        patterns = analyze_feature_patterns(df, feature, var3_filter, min_samples=30)
        if not patterns:
            continue

        print(f"\n{'─' * 80}")
        print(f"PATTERNS BY {feature.upper()}" + (f" (VAR3: {var3_filter})" if var3_filter else ""))
        print("─" * 80)

        # Best patterns
        best = [p for p in patterns if p['deviation'] > 20][:5]
        if best:
            print("\n🟢 SCALE (>20% above average):")
            for p in best:
                print(f"   {p['value'][:30]:<30} | {p['count']:>5} convs | ${p['avg_revenue']:.2f} avg | +{p['deviation']:.0f}%")

        # Worst patterns
        worst = [p for p in patterns if p['deviation'] < -20][-5:]
        if worst:
            print("\n🔴 BLOCK (<20% below average):")
            for p in worst:
                print(f"   {p['value'][:30]:<30} | {p['count']:>5} convs | ${p['avg_revenue']:.2f} avg | {p['deviation']:.0f}%")

    # Two-feature combinations
    print(f"\n{'─' * 80}")
    print("MULTI-DIMENSIONAL PATTERNS" + (f" (VAR3: {var3_filter})" if var3_filter else ""))
    print("─" * 80)

    combos = [
        ('country_code', 'device'),
        ('country_code', 'os'),
        ('device', 'os'),
        ('country_code', 'connection_type'),
    ]

    for f1, f2 in combos:
        patterns = analyze_two_feature_patterns(df, f1, f2, var3_filter, min_samples=20)
        if not patterns:
            continue

        best = [p for p in patterns if p['deviation'] > 30][:3]
        worst = [p for p in patterns if p['deviation'] < -30][-3:]

        if best or worst:
            print(f"\n{f1.upper()} + {f2.upper()}:")

            if best:
                for p in best:
                    print(f"   🟢 {p['feat1']} + {p['feat2']}: ${p['avg_revenue']:.2f} avg ({p['count']} convs) → +{p['deviation']:.0f}%")

            if worst:
                for p in worst:
                    print(f"   🔴 {p['feat1']} + {p['feat2']}: ${p['avg_revenue']:.2f} avg ({p['count']} convs) → {p['deviation']:.0f}%")

    # ISP Deep Dive (usually most important)
    print(f"\n{'─' * 80}")
    print("ISP DEEP DIVE" + (f" (VAR3: {var3_filter})" if var3_filter else ""))
    print("─" * 80)

    isp_patterns = analyze_feature_patterns(df, 'isp', var3_filter, min_samples=20)
    if isp_patterns:
        print("\n🟢 TOP 10 ISPs by Revenue:")
        for p in isp_patterns[:10]:
            print(f"   {p['value'][:40]:<40} | {p['count']:>4} convs | ${p['avg_revenue']:.2f} | {p['deviation']:+.0f}%")

        print("\n🔴 BOTTOM 10 ISPs by Revenue:")
        for p in isp_patterns[-10:]:
            print(f"   {p['value'][:40]:<40} | {p['count']:>4} convs | ${p['avg_revenue']:.2f} | {p['deviation']:+.0f}%")

    # Summary Actions
    print(f"\n{'=' * 80}")
    print("RECOMMENDED ACTIONS")
    print("=" * 80)

    # Find biggest opportunities
    all_patterns = []
    for feature in features:
        for p in analyze_feature_patterns(df, feature, var3_filter, min_samples=30):
            p['feature'] = feature
            all_patterns.append(p)

    best_opps = sorted([p for p in all_patterns if p['deviation'] > 30], key=lambda x: x['total_revenue'], reverse=True)[:5]
    worst_opps = sorted([p for p in all_patterns if p['deviation'] < -30], key=lambda x: x['total_revenue'], reverse=True)[:5]

    if best_opps:
        print("\n💰 BIGGEST SCALE OPPORTUNITIES:")
        for p in best_opps:
            print(f"   [{p['feature']}={p['value'][:25]}] ${p['total_revenue']:.0f} revenue, ${p['avg_revenue']:.2f} avg (+{p['deviation']:.0f}%)")

    if worst_opps:
        print("\n🚫 BIGGEST COST SAVINGS (block these):")
        for p in worst_opps:
            print(f"   [{p['feature']}={p['value'][:25]}] ${p['total_revenue']:.0f} wasted, ${p['avg_revenue']:.2f} avg ({p['deviation']:.0f}%)")


def main():
    parser = argparse.ArgumentParser(description='Daily Pattern Report')
    parser.add_argument('--var3', type=str, help='Filter by specific VAR3 campaign')
    parser.add_argument('--days', type=int, default=30, help='Days of data (default: 30)')
    args = parser.parse_args()

    print("Loading data...")
    df = load_conversions(days_back=args.days)
    print(f"Loaded {len(df):,} conversions\n")

    print_report(df, var3_filter=args.var3)


if __name__ == "__main__":
    main()
