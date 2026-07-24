"""Загрузка taxi_zone_lookup.csv → ClickHouse default.taxi_zones."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REQUIRED_ENV = ("CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD")
CSV_PATH = Path(__file__).resolve().parent / "taxi_zone_lookup.csv"
CONTAINER = os.environ.get("CLICKHOUSE_CONTAINER", "main_clickhouse")


def main() -> None:
    missing = [n for n in REQUIRED_ENV if not os.environ.get(n)]
    if missing:
        print(f"Задайте переменные: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    if not CSV_PATH.exists():
        print(f"Нет файла {CSV_PATH}", file=sys.stderr)
        sys.exit(1)

    user = os.environ["CLICKHOUSE_USER"]
    password = os.environ["CLICKHOUSE_PASSWORD"]

    def ch_query(sql: str, *, stdin=None) -> str:
        cmd = [
            "docker",
            "exec",
            "-i",
            CONTAINER,
            "clickhouse-client",
            "--user",
            user,
            "--password",
            password,
            "--query",
            sql,
        ]
        result = subprocess.run(cmd, input=stdin, capture_output=True, text=True)
        if result.returncode != 0:
            raise SystemExit(result.stderr or result.stdout or "clickhouse-client failed")
        return result.stdout.strip()

    ch_query("TRUNCATE TABLE IF EXISTS default.taxi_zones")
    with CSV_PATH.open(encoding="utf-8") as f:
        ch_query(
            "INSERT INTO default.taxi_zones FORMAT CSVWithNames",
            stdin=f.read(),
        )
    n = ch_query("SELECT count() FROM default.taxi_zones")
    print(f"Загружено в default.taxi_zones: {n}")


if __name__ == "__main__":
    main()
