#!/usr/bin/env python3
"""
Voluum Pattern Analyzer
Detects good and bad performing patterns from conversion data

Usage:
    python pattern_analyzer.py                    # Full analysis
    python pattern_analyzer.py --var3 7933086    # Analyze specific campaign
    python pattern_analyzer.py --min-convs 10    # Minimum conversions threshold
"""

import os
import argparse
from collections import defaultdict
from typing import Dict, List, Tuple
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client


class PatternAnalyzer:
    def __init__(self):
        self.supabase = create_client(
            os.getenv('SUPABASE_URL'),
            os.getenv('SUPABASE_SERVICE_KEY')
        )
        self.conversions = []
        self.visits = []

    def load_data(self):
        """Load all conversions and visits from Supabase"""
        print("Loading conversions...")
        offset = 0
        while True:
            batch = self.supabase.table('conversions').select(
                'click_id,revenue,payout,custom_var_3,country_code,device,os,browser,isp'
            ).range(offset, offset + 999).execute()
            if not batch.data:
                break
            self.conversions.extend(batch.data)
            offset += 1000
            if len(batch.data) < 1000:
                break
        print(f"  Loaded {len(self.conversions)} conversions")

        print("Loading visits...")
        offset = 0
        while True:
            batch = self.supabase.table('live_visits').select(
                'click_id,custom_var_3,country_code,device,os,browser,isp'
            ).range(offset, offset + 999).execute()
            if not batch.data:
                break
            self.visits.extend(batch.data)
            offset += 1000
            if len(batch.data) < 1000:
                break
        print(f"  Loaded {len(self.visits)} visits")

    def analyze_patterns(
        self,
        dimensions: List[str],
        min_convs: int = 5,
        var3_filter: str = None
    ) -> Tuple[List, List]:
        """
        Analyze patterns by given dimensions
        Returns (best_patterns, worst_patterns)
        """
        pattern_stats = defaultdict(lambda: {'convs': 0, 'revenue': 0, 'payout': 0})

        for c in self.conversions:
            # Apply var3 filter if specified
            if var3_filter and c.get('custom_var_3') != var3_filter:
                continue

            # Build pattern key from dimensions
            key_parts = []
            for dim in dimensions:
                val = c.get(dim) or 'unknown'
                key_parts.append(str(val))
            key = '|'.join(key_parts)

            pattern_stats[key]['convs'] += 1
            pattern_stats[key]['revenue'] += float(c['revenue'] or 0)
            pattern_stats[key]['payout'] += float(c['payout'] or 0)

        # Calculate metrics and filter
        patterns = []
        for key, stats in pattern_stats.items():
            if stats['convs'] >= min_convs:
                stats['key'] = key
                stats['dimensions'] = dict(zip(dimensions, key.split('|')))
                stats['rev_per_conv'] = stats['revenue'] / stats['convs']
                stats['profit'] = stats['revenue'] - stats['payout']
                patterns.append(stats)

        # Sort
        best = sorted(patterns, key=lambda x: x['rev_per_conv'], reverse=True)
        worst = sorted(patterns, key=lambda x: x['rev_per_conv'])

        return best, worst

    def analyze_visits_without_conversions(self, min_visits: int = 20) -> List:
        """Find patterns with many visits but no/low conversions"""
        # Get conversion click_ids
        conv_clicks = set(c['click_id'] for c in self.conversions)

        # Analyze visits
        pattern_stats = defaultdict(lambda: {'visits': 0, 'converted': 0, 'revenue': 0})

        for v in self.visits:
            var3 = v.get('custom_var_3') or 'unknown'
            country = v.get('country_code') or 'unknown'
            device = v.get('device') or 'unknown'
            key = f"{var3}|{country}|{device}"

            pattern_stats[key]['visits'] += 1
            if v['click_id'] in conv_clicks:
                pattern_stats[key]['converted'] += 1

        # Find bad patterns (high visits, low conversions)
        bad_patterns = []
        for key, stats in pattern_stats.items():
            if stats['visits'] >= min_visits:
                stats['key'] = key
                var3, country, device = key.split('|')
                stats['dimensions'] = {'var3': var3, 'country': country, 'device': device}
                stats['conv_rate'] = (stats['converted'] / stats['visits']) * 100
                bad_patterns.append(stats)

        return sorted(bad_patterns, key=lambda x: x['conv_rate'])

    def print_report(self, best: List, worst: List, dimensions: List[str], limit: int = 15):
        """Print formatted report"""
        dim_header = ' | '.join(d.replace('custom_var_3', 'VAR3').replace('country_code', 'Country') for d in dimensions)

        print(f"\n{'='*70}")
        print(f"BEST PATTERNS ({dim_header})")
        print(f"{'='*70}")
        print(f"{'Pattern':<40} {'Convs':>6} {'Revenue':>10} {'Rev/Conv':>10}")
        print("-" * 70)
        for p in best[:limit]:
            pattern_str = ' | '.join(str(p['dimensions'].get(d, '?'))[:12] for d in dimensions)
            print(f"{pattern_str:<40} {p['convs']:>6} ${p['revenue']:>9.2f} ${p['rev_per_conv']:>9.2f}")

        print(f"\n{'='*70}")
        print(f"WORST PATTERNS ({dim_header})")
        print(f"{'='*70}")
        print(f"{'Pattern':<40} {'Convs':>6} {'Revenue':>10} {'Rev/Conv':>10}")
        print("-" * 70)
        for p in worst[:limit]:
            pattern_str = ' | '.join(str(p['dimensions'].get(d, '?'))[:12] for d in dimensions)
            print(f"{pattern_str:<40} {p['convs']:>6} ${p['revenue']:>9.2f} ${p['rev_per_conv']:>9.2f}")


def main():
    parser = argparse.ArgumentParser(description='Voluum Pattern Analyzer')
    parser.add_argument('--var3', type=str, help='Filter by specific VAR3 campaign ID')
    parser.add_argument('--min-convs', type=int, default=5, help='Minimum conversions (default: 5)')
    parser.add_argument('--limit', type=int, default=15, help='Number of patterns to show (default: 15)')
    args = parser.parse_args()

    analyzer = PatternAnalyzer()
    analyzer.load_data()

    # Analysis 1: VAR3 + Country
    print("\n" + "=" * 70)
    print("ANALYSIS: VAR3 + COUNTRY")
    print("=" * 70)
    best, worst = analyzer.analyze_patterns(
        ['custom_var_3', 'country_code'],
        min_convs=args.min_convs,
        var3_filter=args.var3
    )
    analyzer.print_report(best, worst, ['custom_var_3', 'country_code'], args.limit)

    # Analysis 2: VAR3 + Device + OS
    print("\n" + "=" * 70)
    print("ANALYSIS: VAR3 + DEVICE + OS")
    print("=" * 70)
    best, worst = analyzer.analyze_patterns(
        ['custom_var_3', 'device', 'os'],
        min_convs=args.min_convs,
        var3_filter=args.var3
    )
    analyzer.print_report(best, worst, ['custom_var_3', 'device', 'os'], args.limit)

    # Analysis 3: Country + Device + OS (across all VAR3)
    if not args.var3:
        print("\n" + "=" * 70)
        print("ANALYSIS: COUNTRY + DEVICE + OS (all campaigns)")
        print("=" * 70)
        best, worst = analyzer.analyze_patterns(
            ['country_code', 'device', 'os'],
            min_convs=args.min_convs
        )
        analyzer.print_report(best, worst, ['country_code', 'device', 'os'], args.limit)

    # Analysis 4: VAR3 only (campaign level)
    print("\n" + "=" * 70)
    print("ANALYSIS: VAR3 CAMPAIGN PERFORMANCE")
    print("=" * 70)
    best, worst = analyzer.analyze_patterns(
        ['custom_var_3'],
        min_convs=args.min_convs,
        var3_filter=args.var3
    )
    analyzer.print_report(best, worst, ['custom_var_3'], args.limit)

    # Summary stats
    total_revenue = sum(float(c['revenue'] or 0) for c in analyzer.conversions)
    total_convs = len(analyzer.conversions)
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Total Conversions: {total_convs:,}")
    print(f"Total Revenue: ${total_revenue:,.2f}")
    print(f"Average Rev/Conv: ${total_revenue/total_convs:.2f}")
    print(f"Total Visits Tracked: {len(analyzer.visits):,}")


if __name__ == "__main__":
    main()
