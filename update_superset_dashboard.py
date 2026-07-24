"""Обновление метрик датасета и дашборда NYC Taxi 2025 в Superset."""

from __future__ import annotations

import json
import os
import uuid
import urllib.error
import urllib.request
import http.cookiejar

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
USER = os.environ["SUPERSET_ADMIN_USERNAME"]
PASSWORD = os.environ["SUPERSET_ADMIN_PASSWORD"]
DASHBOARD_ID = int(os.environ.get("SUPERSET_DASHBOARD_ID", "1"))
DATASET_ID = int(os.environ.get("SUPERSET_METRICS_DATASET_ID", "1"))

NEW_CHARTS = [
    ("Daily Driver Revenue", "sum_driver_revenue"),
    ("Daily Median Speed (mph)", "avg_median_speed_mph"),
]


class Client:
    def __init__(self) -> None:
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.jar)
        )
        self.token = ""
        self.csrf = ""

    def request(self, method: str, path: str, payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.csrf:
            headers["X-CSRFToken"] = self.csrf
            headers["Referer"] = BASE
        req = urllib.request.Request(
            f"{BASE}{path}", data=data, headers=headers, method=method
        )
        try:
            with self.opener.open(req) as resp:
                body = resp.read().decode()
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as e:
            raise SystemExit(f"{method} {path} -> {e.code}: {e.read().decode()}") from e


def ensure_charts_in_layout(c: Client, chart_ids_by_name: dict[str, int]) -> None:
    dash = c.request("GET", f"/api/v1/dashboard/{DASHBOARD_ID}")["result"]
    pos = json.loads(dash.get("position_json") or "{}")
    present = {
        v.get("meta", {}).get("sliceName")
        for v in pos.values()
        if v.get("type") == "CHART"
    }
    missing = [n for n, _ in NEW_CHARTS if n not in present]
    if not missing:
        return

    def new_id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:20]}"

    row_id = new_id("ROW")
    chart_keys = []
    for name in missing:
        key = new_id("CHART")
        chart_keys.append(key)
        pos[key] = {
            "type": "CHART",
            "id": key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {
                "chartId": chart_ids_by_name[name],
                "sliceName": name,
                "height": 50,
                "width": 6,
                "uuid": str(uuid.uuid4()),
            },
        }
    pos[row_id] = {
        "type": "ROW",
        "id": row_id,
        "children": chart_keys,
        "parents": ["ROOT_ID", "GRID_ID"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    children = pos.setdefault("GRID_ID", {"children": []}).setdefault("children", [])
    children.insert(min(1, len(children)), row_id)

    meta = json.loads(dash.get("json_metadata") or "{}")
    c.request(
        "PUT",
        f"/api/v1/dashboard/{DASHBOARD_ID}",
        {
            "dashboard_title": "NYC Taxi 2025",
            "slug": dash.get("slug"),
            "owners": [o["id"] for o in dash.get("owners") or []],
            "position_json": json.dumps(pos),
            "json_metadata": json.dumps(meta),
            "css": dash.get("css") or "",
            "published": True,
        },
    )
    print(f"layout: added {missing}")


def main() -> None:
    c = Client()
    c.token = c.request(
        "POST",
        "/api/v1/security/login",
        {
            "username": USER,
            "password": PASSWORD,
            "provider": "db",
            "refresh": True,
        },
    )["access_token"]
    c.csrf = c.request("GET", "/api/v1/security/csrf_token/")["result"]

    result = c.request("GET", f"/api/v1/dataset/{DATASET_ID}")["result"]
    existing_metrics = {m["metric_name"] for m in result.get("metrics") or []}
    extras = [
        ("sum_driver_revenue", "SUM(driver_revenue)"),
        ("avg_median_speed_mph", "AVG(median_speed_mph)"),
        ("avg_revenue_per_mile", "AVG(revenue_per_mile)"),
        ("sum_total_revenue", "SUM(total_revenue)"),
        ("sum_total_trips", "SUM(total_trips)"),
    ]

    metric_payload = []
    for m in result.get("metrics") or []:
        metric_payload.append(
            {
                "id": m["id"],
                "metric_name": m["metric_name"],
                "expression": m["expression"],
                "verbose_name": m.get("verbose_name") or m["metric_name"],
            }
        )
    for name, expr in extras:
        if name in existing_metrics:
            continue
        metric_payload.append(
            {"metric_name": name, "expression": expr, "verbose_name": name}
        )

    db_id = (
        result["database"]["id"]
        if isinstance(result["database"], dict)
        else result["database"]
    )
    c.request(
        "PUT",
        f"/api/v1/dataset/{DATASET_ID}",
        {
            "database_id": db_id,
            "table_name": result["table_name"],
            "metrics": metric_payload,
            "owners": [o["id"] for o in result.get("owners") or []],
        },
    )

    dash = c.request("GET", f"/api/v1/dashboard/{DASHBOARD_ID}")["result"]
    c.request(
        "PUT",
        f"/api/v1/dashboard/{DASHBOARD_ID}",
        {
            "dashboard_title": "NYC Taxi 2025",
            "slug": dash.get("slug"),
            "owners": [o["id"] for o in dash.get("owners") or []],
            "json_metadata": dash.get("json_metadata") or "{}",
            "css": dash.get("css") or "",
            "published": True,
        },
    )

    charts = c.request("GET", "/api/v1/chart/?q=(page_size:100)").get("result") or []
    existing: dict[str, int] = {}
    for ch in charts:
        existing.setdefault(ch["slice_name"], ch["id"])

    for name, metric in NEW_CHARTS:
        params = {
            "datasource": f"{DATASET_ID}__table",
            "viz_type": "echarts_timeseries_line",
            "metrics": [metric],
            "groupby": [],
            "x_axis": "report_date",
            "time_grain_sqla": "P1D",
            "row_limit": 400,
            "adhoc_filters": [],
        }
        if name in existing:
            c.request(
                "PUT",
                f"/api/v1/chart/{existing[name]}",
                {
                    "dashboards": [DASHBOARD_ID],
                    "slice_name": name,
                    "params": json.dumps(params),
                },
            )
            print(f"updated chart: {name}")
            continue
        created = c.request(
            "POST",
            "/api/v1/chart/",
            {
                "slice_name": name,
                "datasource_id": DATASET_ID,
                "datasource_type": "table",
                "viz_type": "echarts_timeseries_line",
                "params": json.dumps(params),
                "dashboards": [DASHBOARD_ID],
            },
        )
        chart_id = created.get("id")
        if chart_id:
            existing[name] = chart_id
        print(f"created chart {name}: {chart_id}")

    ensure_charts_in_layout(c, existing)
    print("done: dashboard NYC Taxi 2025")


if __name__ == "__main__":
    main()
