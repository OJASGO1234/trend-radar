"""Print what every Grafana panel shows, as numbers.

Reads infra/grafana/dashboards/trend-radar.json, runs each panel's queries through
Grafana's own datasource proxy (so it also proves the provisioned datasource works),
over the last N minutes, and prints last / min / max per series.

Run:  python -m scripts.panel_report [--minutes 5]
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import requests

DASH = Path(__file__).resolve().parents[1] / "infra" / "grafana" / "dashboards" / "trend-radar.json"
GRAFANA = os.getenv("GRAFANA_URL", "http://localhost:3000")
AUTH = ("admin", os.getenv("GRAFANA_ADMIN_PASSWORD", "admin"))


def fmt(v: float, unit: str) -> str:
    if unit == "percentunit":
        return f"{v * 100:.3f}%"
    if unit == "s":
        return f"{v:.2f}s"
    if unit == "ops":
        return f"{v:.3f}/s"
    return f"{v:.2f}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=5)
    args = ap.parse_args()
    now = int(time.time())
    dash = json.loads(DASH.read_text(encoding="utf-8"))
    for p in dash["panels"]:
        unit = p["fieldConfig"]["defaults"].get("unit", "short")
        print(f"\n[{p['id']:>2}] {p['title']}  ({p['type']}, unit={unit})")
        for t in p["targets"]:
            expr = t["expr"].replace("$__range", f"{args.minutes}m")
            body = {"from": str((now - args.minutes * 60) * 1000), "to": str(now * 1000), "queries": [{
                "refId": "A", "datasource": {"type": "prometheus", "uid": "prometheus"},
                "expr": expr, "range": True, "instant": False, "interval": "15s", "maxDataPoints": 200}]}
            r = requests.post(f"{GRAFANA}/api/ds/query", json=body, auth=AUTH, timeout=30)
            r.raise_for_status()
            frames = r.json()["results"]["A"].get("frames", [])
            if not frames:
                print(f"     {t['legendFormat'] or t['expr']}: (no data)")
            for fr in frames:
                cols = fr["data"]["values"]
                vals = [v for v in cols[1] if v is not None] if len(cols) > 1 else []
                if not vals:
                    print(f"     {t['legendFormat'] or t['expr']}: no samples in range (e.g. nothing has happened yet)")
                    continue
                name = fr["schema"]["fields"][1].get("config", {}).get("displayNameFromDS") or t["legendFormat"]
                print(f"     {name:<28} last={fmt(vals[-1], unit):<12} min={fmt(min(vals), unit):<12} max={fmt(max(vals), unit)}")


if __name__ == "__main__":
    main()
