#!/usr/bin/env python3
"""
Dashboard Launcher Script for SchemaGuard (Phase 8).

Launches the interactive Streamlit dashboard for real-time monitoring,
data reconciliation auditing, schema evolution history, and quarantine inspection.

Usage:
    python scripts/run_dashboard.py
    python scripts/run_dashboard.py --server.port 8501
"""

import sys
import os
import subprocess


def main() -> int:
    """Launches the Streamlit dashboard application."""
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    app_path = os.path.join(root_dir, "frontend", "app.py")

    if not os.path.isfile(app_path):
        print(f"Error: Dashboard application not found at '{app_path}'", file=sys.stderr)
        return 1

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        app_path,
        "--server.headless=true",
    ] + sys.argv[1:]

    print(f"Launching SchemaGuard Dashboard: {' '.join(cmd)}")
    return subprocess.call(cmd)


if __name__ == "__main__":
    sys.exit(main())
