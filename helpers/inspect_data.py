"""
Data inspection for the Citibike trip CSVs.

Answers the questions that drive pipeline design *before* we build it:
volume, data quality, duplicates, bad durations, station ID format,
and how the load is spread across stations (== Kafka partitions,
since the generator keys messages by station_id).

Usage:
    .venv/bin/python helpers/inspect_data.py                      # all of data/*.csv
    .venv/bin/python helpers/inspect_data.py --glob 'data/*_1.csv'
"""

import argparse

import duckdb


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def show(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    con.sql(sql).show(max_rows=50, max_width=200)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--glob", default="data/*.csv", help="CSV file(s) to inspect")
    args = parser.parse_args()

    con = duckdb.connect()

    # all_varchar: read everything as text, like the generator does.
    # Station IDs such as "7962.10" must stay strings, never floats.
    con.sql(f"""
        CREATE VIEW raw AS
        SELECT * FROM read_csv('{args.glob}', header = true, all_varchar = true, filename = true)
    """)
    con.sql("""
        CREATE VIEW rides AS
        SELECT
            filename,
            trim(ride_id)                      AS ride_id,
            trim(rideable_type)                AS rideable_type,
            trim(member_casual)                AS member_casual,
            trim(start_station_id)             AS start_station_id,
            trim(end_station_id)               AS end_station_id,
            trim(start_station_name)           AS start_station_name,
            trim(end_station_name)             AS end_station_name,
            started_at                         AS started_at_raw,
            try_cast(started_at AS TIMESTAMP)  AS started_at,
            try_cast(ended_at   AS TIMESTAMP)  AS ended_at,
            date_diff('millisecond', try_cast(started_at AS TIMESTAMP),
                                     try_cast(ended_at   AS TIMESTAMP)) / 1000.0 AS duration_s
        FROM raw
    """)
    # Rows the generator actually turns into events (it skips empty ride/station ids).
    con.sql("""
        CREATE VIEW valid AS
        SELECT * FROM rides
        WHERE coalesce(ride_id, '') <> ''
          AND coalesce(start_station_id, '') <> ''
          AND coalesce(end_station_id, '') <> ''
    """)
    # One row per event, as the generator will publish them.
    con.sql("""
        CREATE VIEW events AS
        SELECT ride_id, 'trip_start' AS event_type, start_station_id AS station_id, started_at AS ts FROM valid
        UNION ALL
        SELECT ride_id, 'trip_end',                 end_station_id,                 ended_at             FROM valid
    """)

    section("1. Volume")
    show(con, "SELECT regexp_extract(filename, '[^/]+$') AS file, count(*) AS rows FROM raw GROUP BY 1 ORDER BY 1")
    show(con, """
        SELECT (SELECT count(*) FROM rides)     AS total_rows,
               (SELECT count(*) FROM valid)     AS rows_kept_by_generator,
               (SELECT count(*) FROM valid) * 2 AS events_to_publish
    """)

    section("2. Missing / empty values (rows)")
    show(con, """
        SELECT
            count(*) FILTER (WHERE coalesce(ride_id, '') = '')            AS ride_id,
            count(*) FILTER (WHERE coalesce(start_station_id, '') = '')   AS start_station_id,
            count(*) FILTER (WHERE coalesce(end_station_id, '') = '')     AS end_station_id,
            count(*) FILTER (WHERE coalesce(start_station_name, '') = '') AS start_station_name,
            count(*) FILTER (WHERE coalesce(end_station_name, '') = '')   AS end_station_name,
            count(*) FILTER (WHERE started_at IS NULL)                    AS unparseable_started_at,
            count(*) FILTER (WHERE ended_at IS NULL)                      AS unparseable_ended_at
        FROM rides
    """)

    section("3. Duplicate ride_ids in the source")
    show(con, """
        SELECT count(*) AS rows, count(DISTINCT ride_id) AS distinct_ride_ids,
               count(*) - count(DISTINCT ride_id) AS duplicate_rows
        FROM valid
    """)

    section("4. Categorical values (spec: classic_bike|electric_bike|docked_bike, member|casual)")
    show(con, "SELECT rideable_type, count(*) AS rows FROM valid GROUP BY 1 ORDER BY 2 DESC")
    show(con, "SELECT member_casual, count(*) AS rows FROM valid GROUP BY 1 ORDER BY 2 DESC")

    section("5. Timestamps")
    show(con, """
        SELECT min(started_at) AS min_start, max(started_at) AS max_start,
               min(ended_at)   AS min_end,   max(ended_at)   AS max_end,
               count(*) FILTER (WHERE started_at_raw NOT LIKE '%.%') AS without_millis
        FROM valid
    """)
    show(con, """
        SELECT count(*) FILTER (WHERE started_at < date_trunc('month', (SELECT median(started_at) FROM valid)))
                   AS started_before_month,
               count(*) FILTER (WHERE ended_at >= date_trunc('month', (SELECT median(started_at) FROM valid))
                                                  + INTERVAL 1 MONTH)
                   AS ended_after_month
        FROM valid
    """)

    section("6. Trip durations (seconds) -> trip-stats edge cases")
    show(con, """
        SELECT count(*) FILTER (WHERE duration_s < 0)              AS negative_end_before_start,
               count(*) FILTER (WHERE duration_s = 0)              AS zero,
               count(*) FILTER (WHERE duration_s BETWEEN 0.001 AND 59.999) AS under_1_min,
               count(*) FILTER (WHERE duration_s > 86400)          AS over_24h,
               round(min(duration_s), 1)                           AS min_s,
               round(quantile_cont(duration_s, 0.5), 1)            AS p50_s,
               round(quantile_cont(duration_s, 0.9), 1)            AS p90_s,
               round(quantile_cont(duration_s, 0.99), 1)           AS p99_s,
               round(max(duration_s), 1)                           AS max_s
        FROM valid
    """)
    show(con, """
        SELECT ride_id, start_station_id, end_station_id, started_at, ended_at, duration_s
        FROM valid WHERE duration_s < 0 ORDER BY duration_s LIMIT 5
    """)

    section("7. Stations")
    show(con, """
        SELECT count(DISTINCT start_station_id) AS distinct_start_ids,
               count(DISTINCT end_station_id)   AS distinct_end_ids,
               (SELECT count(DISTINCT station_id) FROM events) AS distinct_ids_total,
               count(*) FILTER (WHERE start_station_id = end_station_id) AS round_trips
        FROM valid
    """)
    print("Station IDs that don't look like '1234.56' (format surprises):")
    show(con, """
        SELECT station_id, count(*) AS events FROM events
        WHERE NOT regexp_full_match(station_id, '\\d+\\.\\d+')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)
    print("Station IDs with a trailing zero (would break if parsed as float):")
    show(con, """
        SELECT count(DISTINCT station_id) AS ids_with_trailing_zero FROM events
        WHERE regexp_full_match(station_id, '\\d+\\.\\d*0')
    """)
    print("Station IDs mapped to more than one name (which name does the API return?):")
    show(con, """
        WITH names AS (
            SELECT start_station_id AS id, start_station_name AS name FROM valid
            UNION SELECT end_station_id, end_station_name FROM valid
        )
        SELECT id, count(DISTINCT name) AS names, string_agg(DISTINCT name, ' | ') AS examples
        FROM names GROUP BY 1 HAVING count(DISTINCT name) > 1 ORDER BY 2 DESC LIMIT 10
    """)

    section("8. Load shape: skew per station (= per Kafka partition key)")
    show(con, """
        WITH per_station AS (SELECT station_id, count(*) AS events FROM events GROUP BY 1)
        SELECT station_id, events,
               round(100.0 * events / sum(events) OVER (), 3) AS pct_of_all
        FROM per_station ORDER BY events DESC LIMIT 10
    """)
    show(con, """
        WITH per_station AS (SELECT count(*) AS events FROM events GROUP BY station_id)
        SELECT round(avg(events), 1) AS avg_events_per_station,
               quantile_cont(events, 0.5) AS median, max(events) AS max
        FROM per_station
    """)

    section("9. Load shape: event-time rate (real-world, before generator pacing)")
    show(con, """
        WITH per_min AS (SELECT date_trunc('minute', ts) AS m, count(*) AS n FROM events GROUP BY 1)
        SELECT round(avg(n) / 60, 2) AS avg_events_per_s,
               round(max(n) / 60, 2) AS peak_minute_events_per_s,
               max(n)                AS peak_minute_events
        FROM per_min
    """)


if __name__ == "__main__":
    main()
