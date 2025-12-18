#!/usr/bin/env python3
"""
ML Pattern Detector for Voluum Conversions
Uses Decision Tree to find multi-dimensional patterns that predict high/low revenue

Usage:
    python ml_pattern_detector.py                     # Analyze all VAR3s with enough data
    python ml_pattern_detector.py --var3 7933086     # Analyze specific campaign
    python ml_pattern_detector.py --min-samples 50   # Minimum samples per VAR3
"""

import os
import argparse
from collections import defaultdict
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier, export_text
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score

from supabase import create_client


class MLPatternDetector:
    def __init__(self):
        self.supabase = create_client(
            os.getenv('SUPABASE_URL'),
            os.getenv('SUPABASE_SERVICE_KEY')
        )
        self.df = None
        self.features = ['country_code', 'device', 'os', 'browser', 'connection_type', 'isp']

    def load_data(self, days_back: int = 30):
        """Load conversions from Supabase"""
        print(f"Loading conversions (last {days_back} days)...")

        cutoff = (datetime.utcnow() - timedelta(days=days_back)).isoformat()

        all_data = []
        offset = 0
        while True:
            batch = self.supabase.table('conversions').select(
                'click_id,revenue,payout,custom_var_3,country_code,device,os,browser,isp,connection_type,postback_timestamp'
            ).gte('postback_timestamp', cutoff).range(offset, offset + 999).execute()

            if not batch.data:
                break
            all_data.extend(batch.data)
            offset += 1000
            if len(batch.data) < 1000:
                break

        self.df = pd.DataFrame(all_data)
        print(f"  Loaded {len(self.df)} conversions")

        # Clean data
        self.df['revenue'] = pd.to_numeric(self.df['revenue'], errors='coerce').fillna(0)
        self.df['custom_var_3'] = self.df['custom_var_3'].fillna('unknown')

        for col in self.features:
            self.df[col] = self.df[col].fillna('unknown').astype(str)

        return self.df

    def get_var3_with_enough_data(self, min_samples: int = 50) -> list:
        """Get VAR3 campaigns with enough conversions"""
        counts = self.df['custom_var_3'].value_counts()
        valid = counts[counts >= min_samples].index.tolist()
        print(f"  Found {len(valid)} VAR3 campaigns with >= {min_samples} conversions")
        return valid

    def analyze_var3(self, var3: str, min_samples_leaf: int = 10) -> dict:
        """Analyze patterns for a specific VAR3 campaign"""
        subset = self.df[self.df['custom_var_3'] == var3].copy()

        if len(subset) < 30:
            return None

        # Calculate median revenue for this VAR3
        median_rev = subset['revenue'].median()
        mean_rev = subset['revenue'].mean()

        # Create binary target: high (1) vs low (0) revenue
        subset['high_revenue'] = (subset['revenue'] > median_rev).astype(int)

        # Prepare features
        feature_dfs = []
        encoders = {}

        for col in self.features:
            le = LabelEncoder()
            encoded = le.fit_transform(subset[col])
            feature_dfs.append(pd.Series(encoded, name=col))
            encoders[col] = le

        X = pd.concat(feature_dfs, axis=1)
        y = subset['high_revenue']

        # Train Decision Tree
        dt = DecisionTreeClassifier(
            max_depth=4,
            min_samples_leaf=min_samples_leaf,
            min_samples_split=min_samples_leaf * 2,
            random_state=42
        )
        dt.fit(X, y)

        # Cross-validation score
        cv_scores = cross_val_score(dt, X, y, cv=min(5, len(subset) // 10 + 1))

        # Extract rules
        rules = self._extract_rules(dt, X.columns.tolist(), encoders, subset)

        # Feature importance
        importance = dict(zip(self.features, dt.feature_importances_))

        return {
            'var3': var3,
            'total_conversions': len(subset),
            'median_revenue': median_rev,
            'mean_revenue': mean_rev,
            'total_revenue': subset['revenue'].sum(),
            'cv_accuracy': cv_scores.mean(),
            'feature_importance': importance,
            'rules': rules
        }

    def _extract_rules(self, tree, feature_names, encoders, data) -> list:
        """Extract human-readable rules from decision tree"""
        rules = []

        def recurse(node, conditions, depth=0):
            if depth > 4:
                return

            feature = tree.tree_.feature[node]
            threshold = tree.tree_.threshold[node]

            # Leaf node
            if feature == -2:
                samples = tree.tree_.n_node_samples[node]
                value = tree.tree_.value[node][0]

                if samples >= 10:  # Minimum samples for a rule
                    high_count = int(value[1]) if len(value) > 1 else 0
                    low_count = int(value[0])
                    total = high_count + low_count

                    if total > 0:
                        high_pct = high_count / total * 100

                        # Determine if this is a "good" or "bad" pattern
                        pattern_type = 'GOOD' if high_pct > 60 else 'BAD' if high_pct < 40 else 'NEUTRAL'

                        if pattern_type != 'NEUTRAL' and conditions:
                            rules.append({
                                'conditions': conditions.copy(),
                                'type': pattern_type,
                                'high_pct': high_pct,
                                'samples': total,
                                'high_count': high_count,
                                'low_count': low_count
                            })
                return

            # Get feature name and decode threshold
            feat_name = feature_names[feature]
            encoder = encoders[feat_name]

            # Left branch (<=)
            left_node = tree.tree_.children_left[node]
            if threshold < len(encoder.classes_):
                # Get values that satisfy <= threshold
                left_values = [encoder.classes_[i] for i in range(int(threshold) + 1) if i < len(encoder.classes_)]
                if left_values:
                    left_cond = conditions + [f"{feat_name} IN ({', '.join(left_values[:3])}{'...' if len(left_values) > 3 else ''})"]
                    recurse(left_node, left_cond, depth + 1)

            # Right branch (>)
            right_node = tree.tree_.children_right[node]
            if threshold < len(encoder.classes_) - 1:
                right_values = [encoder.classes_[i] for i in range(int(threshold) + 1, len(encoder.classes_))]
                if right_values:
                    right_cond = conditions + [f"{feat_name} IN ({', '.join(right_values[:3])}{'...' if len(right_values) > 3 else ''})"]
                    recurse(right_node, right_cond, depth + 1)

        recurse(0, [])

        # Sort by confidence and samples
        rules.sort(key=lambda x: (x['type'] == 'BAD', -x['high_pct'] if x['type'] == 'GOOD' else x['high_pct'], -x['samples']))

        return rules

    def analyze_multi_var3(self, var3_list: list = None, min_samples: int = 50) -> list:
        """Analyze multiple VAR3 campaigns"""
        if var3_list is None:
            var3_list = self.get_var3_with_enough_data(min_samples)

        results = []
        for var3 in var3_list:
            print(f"  Analyzing VAR3: {var3}...")
            result = self.analyze_var3(var3)
            if result:
                results.append(result)

        return results

    def print_report(self, results: list):
        """Print formatted report"""
        print("\n" + "=" * 80)
        print("ML PATTERN DETECTION REPORT")
        print(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        print("=" * 80)

        for r in results:
            print(f"\n{'─' * 80}")
            print(f"VAR3: {r['var3']}")
            print(f"Conversions: {r['total_conversions']:,} | Revenue: ${r['total_revenue']:,.2f} | Avg: ${r['mean_revenue']:.2f} | Median: ${r['median_revenue']:.2f}")
            print(f"Model Accuracy: {r['cv_accuracy']*100:.1f}%")

            # Feature importance
            imp = r['feature_importance']
            top_features = sorted(imp.items(), key=lambda x: x[1], reverse=True)[:3]
            print(f"Top Features: {', '.join(f'{f}({v:.0%})' for f, v in top_features)}")

            # Good patterns
            good_rules = [rule for rule in r['rules'] if rule['type'] == 'GOOD'][:5]
            if good_rules:
                print(f"\n  🟢 PATTERNS TO SCALE:")
                for i, rule in enumerate(good_rules, 1):
                    conditions = ' AND '.join(rule['conditions'][:3])
                    print(f"     {i}. {conditions}")
                    print(f"        → {rule['high_pct']:.0f}% high revenue ({rule['samples']} conversions)")

            # Bad patterns
            bad_rules = [rule for rule in r['rules'] if rule['type'] == 'BAD'][:5]
            if bad_rules:
                print(f"\n  🔴 PATTERNS TO BLOCK:")
                for i, rule in enumerate(bad_rules, 1):
                    conditions = ' AND '.join(rule['conditions'][:3])
                    print(f"     {i}. {conditions}")
                    print(f"        → {100-rule['high_pct']:.0f}% low revenue ({rule['samples']} conversions)")

        # Summary
        print(f"\n{'=' * 80}")
        print("SUMMARY")
        print(f"{'=' * 80}")
        print(f"VAR3 Campaigns Analyzed: {len(results)}")
        total_convs = sum(r['total_conversions'] for r in results)
        total_rev = sum(r['total_revenue'] for r in results)
        print(f"Total Conversions: {total_convs:,}")
        print(f"Total Revenue: ${total_rev:,.2f}")

        # Top/Bottom VAR3
        by_avg = sorted(results, key=lambda x: x['mean_revenue'], reverse=True)
        print(f"\nTop 5 VAR3 by Avg Revenue:")
        for r in by_avg[:5]:
            print(f"  {r['var3']}: ${r['mean_revenue']:.2f} avg ({r['total_conversions']} convs)")

        print(f"\nBottom 5 VAR3 by Avg Revenue:")
        for r in by_avg[-5:]:
            print(f"  {r['var3']}: ${r['mean_revenue']:.2f} avg ({r['total_conversions']} convs)")


def main():
    parser = argparse.ArgumentParser(description='ML Pattern Detector')
    parser.add_argument('--var3', type=str, help='Analyze specific VAR3 campaign')
    parser.add_argument('--min-samples', type=int, default=50, help='Minimum conversions per VAR3 (default: 50)')
    parser.add_argument('--days', type=int, default=30, help='Days of data to analyze (default: 30)')
    args = parser.parse_args()

    detector = MLPatternDetector()
    detector.load_data(days_back=args.days)

    if args.var3:
        results = [detector.analyze_var3(args.var3)]
        results = [r for r in results if r]
    else:
        results = detector.analyze_multi_var3(min_samples=args.min_samples)

    if results:
        detector.print_report(results)
    else:
        print("No VAR3 campaigns with enough data found.")


if __name__ == "__main__":
    main()
