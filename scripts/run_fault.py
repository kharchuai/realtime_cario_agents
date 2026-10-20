"""Chạy CSV như run_pipeline; pause consumer 10s ở giây nguồn thứ 300."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_pipeline import main as run_pipeline


def main():
    """Dùng CLI replay hiện có và bật lịch pause consumer cố định 300s/10s."""
    return run_pipeline(fault=True)


if __name__ == "__main__":
    raise SystemExit(main())
