"""Run dbt with .env loaded and the profile pointing at ./dbt.

Usage (PowerShell):  python -m scripts.dbt_cli build
                     python -m scripts.dbt_cli source freshness
                     python -m scripts.dbt_cli docs generate
Any arguments are passed straight to dbt.
"""
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt"


def main() -> int:
    load_dotenv(ROOT / ".env")
    if not os.environ.get("DATABASE_URL"):
        print("DATABASE_URL is not set (put it in .env)", file=sys.stderr)
        return 1
    env = {**os.environ, "DBT_PROFILES_DIR": str(DBT_DIR)}
    dbt = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
    return subprocess.call([str(dbt), *sys.argv[1:]], cwd=DBT_DIR, env=env)


if __name__ == "__main__":
    sys.exit(main())
