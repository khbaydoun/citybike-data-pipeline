"""
End-to-end acceptance check: compare the live API with values computed
independently by DuckDB from the same CSV file(s) that were published.

Checks the busiest station, then every endpoint for the top stations and a
random sample of others.

Usage (after publishing the files and waiting for consumer lag 0):
    .venv/bin/python helpers/acceptance_check.py --file data/202606-citibike-tripdata_*.csv
"""

import argparse
import json
import random
import sys
import urllib.error
import urllib.parse
import urllib.request

import duckdb


def expected(files: list[str]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    paths = ", ".join(f"'{f}'" for f in files)
    # Same filter as the generator: drop rows missing ride or station ids.
    con.sql(f"""
        CREATE TABLE rides AS
        SELECT trim(start_station_id) AS s, trim(end_station_id) AS e,
               started_at::TIMESTAMP AS st, ended_at::TIMESTAMP AS et,
               date_diff('millisecond', started_at::TIMESTAMP, ended_at::TIMESTAMP) AS d
        FROM read_csv([{paths}], header = true, all_varchar = true)
        WHERE coalesce(trim(ride_id), '') <> '' AND coalesce(trim(start_station_id), '') <> ''
          AND coalesce(trim(end_station_id), '') <> ''
    """)
    con.sql("""
        CREATE TABLE events AS
        SELECT s AS id, 'trip_start' AS t, st AS ts FROM rides
        UNION ALL SELECT e, 'trip_end', et FROM rides
    """)
    con.sql("""
        CREATE TABLE stations AS
        WITH ev AS (
            SELECT id, count(*) AS n,
                   max(ts) AS last_ts,
                   -- on equal timestamps trip_end wins (same rule as the pipeline)
                   arg_max(t, (ts, t = 'trip_end')) AS last_type
            FROM events GROUP BY id
        ),
        dep AS (SELECT s AS id, count(*) AS departures,
                       count(*) FILTER (WHERE d > 0) AS dep_trips,
                       coalesce(sum(d) FILTER (WHERE d > 0), 0) AS dep_sum FROM rides GROUP BY s),
        arr AS (SELECT e AS id, count(*) AS arrivals,
                       count(*) FILTER (WHERE d > 0) AS arr_trips,
                       coalesce(sum(d) FILTER (WHERE d > 0), 0) AS arr_sum FROM rides GROUP BY e)
        SELECT ev.*, coalesce(departures, 0) AS departures, coalesce(arrivals, 0) AS arrivals,
               coalesce(dep_trips, 0) AS dep_trips, coalesce(dep_sum, 0) AS dep_sum,
               coalesce(arr_trips, 0) AS arr_trips, coalesce(arr_sum, 0) AS arr_sum
        FROM ev LEFT JOIN dep USING (id) LEFT JOIN arr USING (id)
    """)
    return con


def avg(sum_ms: int, count: int) -> float | None:
    return round(sum_ms / 1000 / count, 1) if count else None  # same formula as the API


def get(api: str, path: str) -> dict:
    try:
        with urllib.request.urlopen(f"{api}{path}", timeout=5) as res:
            return json.load(res)
    except urllib.error.HTTPError as e:
        return {"http_status": e.code}


class Checker:
    def __init__(self):
        self.passed = self.failed = 0

    def eq(self, label: str, got, want):
        if got == want:
            self.passed += 1
        else:
            self.failed += 1
            print(f"  ❌ {label}: got {got!r}, expected {want!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", nargs="+", required=True, help="CSV file(s) that were published")
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--top", type=int, default=5, help="top stations to check")
    parser.add_argument("--sample", type=int, default=5, help="random other stations to check")
    args = parser.parse_args()

    print(f"Computing expected values with DuckDB from {len(args.file)} file(s)...")
    con = expected(args.file)
    rows = con.sql("SELECT * FROM stations ORDER BY n DESC, id DESC").fetchall()
    cols = [c[0] for c in con.sql("SELECT * FROM stations").description]
    stations = [dict(zip(cols, r)) for r in rows]
    total = con.sql("SELECT count(*) FROM events").fetchone()[0]
    print(f"Expected: {total:,} events over {len(stations):,} stations\n")

    c = Checker()
    top = stations[0]
    busiest = get(args.api, "/stations/busiest")
    c.eq("busiest.station_id", busiest.get("station_id"), top["id"])
    c.eq("busiest.event_count", busiest.get("event_count"), top["n"])
    print(f"busiest: {top['id']} with {top['n']:,} events")

    random.seed(42)
    chosen = stations[: args.top] + random.sample(stations[args.top:], min(args.sample, len(stations) - args.top))
    for s in chosen:
        sid, q = s["id"], urllib.parse.quote(s["id"], safe="")
        last = get(args.api, f"/stations/{q}/last-activity")
        c.eq(f"{sid} last_activity.event_type", last.get("event_type"), s["last_type"])
        c.eq(f"{sid} last_activity.timestamp", last.get("timestamp"),
             s["last_ts"].isoformat(timespec="milliseconds") + "Z")
        bal = get(args.api, f"/stations/{q}/bike-balance")
        c.eq(f"{sid} arrivals", bal.get("arrivals"), s["arrivals"])
        c.eq(f"{sid} departures", bal.get("departures"), s["departures"])
        c.eq(f"{sid} bike_balance", bal.get("bike_balance"), s["arrivals"] - s["departures"])
        ts = get(args.api, f"/stations/{q}/trip-stats")
        c.eq(f"{sid} departing", ts.get("departing"),
             {"trip_count": s["dep_trips"], "avg_duration_seconds": avg(s["dep_sum"], s["dep_trips"])})
        c.eq(f"{sid} arriving", ts.get("arriving"),
             {"trip_count": s["arr_trips"], "avg_duration_seconds": avg(s["arr_sum"], s["arr_trips"])})
        print(f"checked {sid:<12} balance {s['arrivals'] - s['departures']:+6d}  "
              f"dep {s['dep_trips']:>6} / {avg(s['dep_sum'], s['dep_trips'])} s  "
              f"arr {s['arr_trips']:>6} / {avg(s['arr_sum'], s['arr_trips'])} s")

    print(f"\n{c.passed} checks passed, {c.failed} failed "
          f"({len(chosen)} stations: top {args.top} + {len(chosen) - args.top} random)")
    sys.exit(1 if c.failed else 0)


if __name__ == "__main__":
    main()
