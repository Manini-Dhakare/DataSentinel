"""
CLI script to generate synthetic customers and orders Parquet batches for SchemaGuard.

Usage:
    python scripts/generate_sample_data.py --rows 1000
    python scripts/generate_sample_data.py --rows 100000 --seed 42
"""

import argparse
import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from generator.generate_data import SyntheticDataGenerator


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic Parquet batches for SchemaGuard.")
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of rows to generate per batch (default: 1000)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible dataset generation (default: 42)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/generated",
        help="Directory where generated Parquet files will be stored (default: data/generated)"
    )
    parser.add_argument(
        "--report-dir",
        type=str,
        default="data/reports",
        help="Directory where generation JSON report will be stored (default: data/reports)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print("=" * 60)
    print("SchemaGuard: Synthetic Parquet Data Generator")
    print(f"Target Rows per Batch : {args.rows}")
    print(f"Random Seed           : {args.seed}")
    print(f"Output Directory      : {args.output_dir}")
    print(f"Report Directory      : {args.report_dir}")
    print("=" * 60)

    generator = SyntheticDataGenerator(
        seed=args.seed,
        base_output_dir=args.output_dir,
        report_dir=args.report_dir
    )
    report = generator.generate_all(rows_per_batch=args.rows)

    print("\nGeneration Summary:")
    print(f"- Customers file: {report['customers']['file_name']} ({report['customers']['row_count']} rows)")
    for filename, meta in report["batches"].items():
        print(f"- {filename:<38} | Rows: {meta['row_count']:<6} | Invalid: {meta['intentional_invalid_records']:<5} | {meta['schema_change']}")

    print("\nGeneration report written successfully to data/reports/generation_report.json")
    print("=" * 60)


if __name__ == "__main__":
    main()
