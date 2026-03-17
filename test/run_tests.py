#!/usr/bin/env python3
"""Integration tests for duckdb-httpsql extension.

Usage:
    # Start the server first:
    #   ./httpsql-server --host "root:root@tcp(127.0.0.1:3306)/" --port 18080
    python test/run_tests.py

Environment variables:
    SERVER_URL  (default: http://localhost:18080)
    MYSQL_HOST  (default: 127.0.0.1)
    MYSQL_PORT  (default: 3306)
    MYSQL_USER  (default: root)
    MYSQL_PASS  (default: root)
"""

import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

# Use the mysql-sharding duckdb binary (same DuckDB version)
DUCKDB = PROJECT_DIR.parent / "duckdb-mysql-sharding" / "build" / "release" / "duckdb"
EXT    = PROJECT_DIR / "build" / "release" / "extension" / "httpsql" / "httpsql.duckdb_extension"

SERVER_URL = os.environ.get("SERVER_URL", "http://localhost:18080")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("MYSQL_PASS", "root")

GREEN = "\033[0;32m"
RED   = "\033[0;31m"
YELLOW = "\033[0;33m"
NC    = "\033[0m"

passed = 0
failed = 0


def check(desc: str, sql: str, expected: str):
    global passed, failed
    full_sql = (
        f"LOAD '{EXT}';\n"
        f"ATTACH '{SERVER_URL}' AS db1 (TYPE httpsql);\n"
        f"ATTACH '{SERVER_URL}' AS db2 (TYPE httpsql);\n"
        f"{sql}"
    )
    try:
        result = subprocess.run(
            [str(DUCKDB), "-unsigned", "-noheader", "-csv", "-c", full_sql],
            capture_output=True, text=True, timeout=30,
        )
        # Strip httpsql debug lines
        actual_lines = [l for l in result.stdout.splitlines() if not l.startswith("[httpsql]")]
        actual = "\n".join(actual_lines).strip().replace("\r", "")
    except Exception as e:
        actual = f"ERROR: {e}"

    if actual == expected:
        print(f"  {GREEN}PASS{NC}  {desc}")
        passed += 1
    else:
        print(f"  {RED}FAIL{NC}  {desc}")
        for label, val in [("expected", expected), ("actual", actual)]:
            preview = "\n".join(val.splitlines()[:5])
            print(f"        {label}: {preview!r}")
        failed += 1


def setup():
    print("==> Setting up test data...")
    setup_sql = PROJECT_DIR.parent / "duckdb-mysql-sharding" / "test" / "setup_test_data.sql"
    subprocess.run(
        ["mysql", "-h", MYSQL_HOST, "-P", MYSQL_PORT, "-u", MYSQL_USER, f"-p{MYSQL_PASS}",
         "-e", f"source {setup_sql}"],
        capture_output=True,
    )

    if not DUCKDB.exists():
        print(f"ERROR: DuckDB binary not found at {DUCKDB}", file=sys.stderr)
        sys.exit(1)
    if not EXT.exists():
        print(f"ERROR: Extension not found at {EXT}", file=sys.stderr)
        sys.exit(1)

    # Check server is alive
    import urllib.request
    try:
        urllib.request.urlopen(f"{SERVER_URL}/api/schemas", timeout=3)
    except Exception as e:
        print(f"ERROR: httpsql-server not reachable at {SERVER_URL}: {e}", file=sys.stderr)
        print("  Start with: ./httpsql-server --host 'root:root@tcp(127.0.0.1:3306)/' --port 18080")
        sys.exit(1)
    print(f"==> Server OK at {SERVER_URL}")


def main():
    setup()
    print("==> Running tests...\n")

    # ── 1. Basic row counts ────────────────────────────────────────────────
    print(f"{YELLOW}--- Shard Discovery ---{NC}")
    check("orders: 5 shards -> 15 rows",
          "SELECT COUNT(*) FROM db1.sharding_test_db1.orders;", "15")
    check("users: 3 shards -> 6 rows",
          "SELECT COUNT(*) FROM db1.sharding_test_db1.users;", "6")
    check("config: non-sharded -> 2 rows",
          "SELECT COUNT(*) FROM db1.sharding_test_db1.config;", "2")
    check("payments: 3 shards in db2 -> 6 rows",
          "SELECT COUNT(*) FROM db2.sharding_test_db2.payments;", "6")

    # ── 2. Multi-attach (same server, different catalog alias) ─────────────
    print(f"\n{YELLOW}--- Multi-Attach ---{NC}")
    check("cross-attach: both accessible in same session",
          "SELECT (SELECT COUNT(*) FROM db1.sharding_test_db1.orders) + "
          "(SELECT COUNT(*) FROM db2.sharding_test_db2.payments);",
          "21")

    # ── 3. LIMIT pushdown ──────────────────────────────────────────────────
    print(f"\n{YELLOW}--- LIMIT Pushdown ---{NC}")
    check("LIMIT 1",
          "SELECT COUNT(*) FROM (SELECT * FROM db1.sharding_test_db1.orders LIMIT 1);", "1")
    check("LIMIT 3",
          "SELECT COUNT(*) FROM (SELECT * FROM db1.sharding_test_db1.orders LIMIT 3);", "3")
    check("LIMIT larger than total",
          "SELECT COUNT(*) FROM (SELECT * FROM db1.sharding_test_db1.orders LIMIT 100);", "15")

    # ── 4. TopN pushdown (ORDER BY + LIMIT) ────────────────────────────────
    print(f"\n{YELLOW}--- TopN Pushdown ---{NC}")
    check("ORDER BY ASC LIMIT 3",
          "SELECT amount FROM db1.sharding_test_db1.orders ORDER BY amount ASC LIMIT 3;",
          "3.0\n5.0\n7.5")
    check("ORDER BY DESC LIMIT 3",
          "SELECT amount FROM db1.sharding_test_db1.orders ORDER BY amount DESC LIMIT 3;",
          "100.0\n50.0\n45.0")
    check("ORDER BY amount ASC LIMIT 5",
          "SELECT amount FROM db1.sharding_test_db1.orders ORDER BY amount ASC LIMIT 5;",
          "3.0\n5.0\n7.5\n8.0\n9.0")
    check("multi-col ORDER BY + LIMIT",
          "SELECT status, amount FROM db1.sharding_test_db1.orders ORDER BY status ASC, amount DESC LIMIT 3;",
          "cancelled,22.0\ncancelled,15.0\ncancelled,7.5")

    # ── 5. Aggregate pushdown (no GROUP BY) ────────────────────────────────
    print(f"\n{YELLOW}--- Aggregate Pushdown (Simple) ---{NC}")
    check("COUNT(*)",
          "SELECT COUNT(*) FROM db1.sharding_test_db1.orders;", "15")
    check("SUM(double)",
          "SELECT SUM(amount) FROM db1.sharding_test_db1.orders;", "362.5")
    check("SUM(int)",
          "SELECT SUM(quantity) FROM db1.sharding_test_db1.orders;", "65")
    check("MIN(amount)",
          "SELECT MIN(amount) FROM db1.sharding_test_db1.orders;", "3.0")
    check("MAX(amount)",
          "SELECT MAX(amount) FROM db1.sharding_test_db1.orders;", "100.0")
    check("multiple aggregates",
          "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM db1.sharding_test_db1.orders;",
          "15,362.5,3.0,100.0")

    # ── 6. Aggregate pushdown with GROUP BY ────────────────────────────────
    print(f"\n{YELLOW}--- Aggregate Pushdown (GROUP BY) ---{NC}")
    check("GROUP BY + COUNT(*)",
          "SELECT status, COUNT(*) FROM db1.sharding_test_db1.orders GROUP BY status ORDER BY status;",
          "cancelled,3\ncompleted,6\npending,6")
    check("GROUP BY + SUM",
          "SELECT status, SUM(amount) FROM db1.sharding_test_db1.orders GROUP BY status ORDER BY status;",
          "cancelled,44.5\ncompleted,270.0\npending,48.0")
    check("GROUP BY + MIN/MAX",
          "SELECT status, MIN(amount), MAX(amount) FROM db1.sharding_test_db1.orders GROUP BY status ORDER BY status;",
          "cancelled,7.5,22.0\ncompleted,20.0,100.0\npending,3.0,12.5")
    check("GROUP BY + all aggregates",
          "SELECT status, COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM db1.sharding_test_db1.orders GROUP BY status ORDER BY status;",
          "cancelled,3,44.5,7.5,22.0\ncompleted,6,270.0,20.0,100.0\npending,6,48.0,3.0,12.5")
    check("GROUP BY + SUM(int)",
          "SELECT status, SUM(quantity) FROM db1.sharding_test_db1.orders GROUP BY status ORDER BY status;",
          "cancelled,5\ncompleted,46\npending,14")
    check("GROUP BY payments by method",
          "SELECT method, COUNT(*) FROM db2.sharding_test_db2.payments GROUP BY method ORDER BY method;",
          "bank_transfer,1\ncredit_card,3\ndebit_card,2")

    # ── Summary ────────────────────────────────────────────────────────────
    total = passed + failed
    print(f"\n{'=' * 40}")
    print(f"  Total: {total}  {GREEN}Pass: {passed}{NC}  {RED}Fail: {failed}{NC}")
    print(f"{'=' * 40}")
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
