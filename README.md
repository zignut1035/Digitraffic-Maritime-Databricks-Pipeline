# 🚢 Digitraffic Maritime Trajectory Pipeline — Multi-Cadence AIS Medallion Architecture

> A Databricks Lakeflow pipeline that ingests live vessel AIS positions, port calls, and sea-state data from Finland's Digitraffic API, processes them through a bronze/silver/gold medallion architecture, and publishes both a live vessel-tracking snapshot **and** a full historical trajectory table — each data source refreshed at its own natural update cadence.

![Python](https://img.shields.io/badge/Python-3.x-blue?style=flat-square&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-Structured%20Streaming-orange?style=flat-square&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion%20Architecture-teal?style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-Lakeflow%20Jobs-red?style=flat-square&logo=databricks)
![Azure](https://img.shields.io/badge/Azure-Data%20Lake%20Storage-0089D6?style=flat-square&logo=microsoftazure)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

---
<img width="594" height="600" alt="Screenshot_2-9-2026_21433_adb-7405614385684070 10 azuredatabricks net" src="https://github.com/user-attachments/assets/4f67109f-3d20-4e5a-8c55-f7bee96953d4" />
## Overview

The [Digitraffic Marine API](https://www.digitraffic.fi/en/marine-traffic/) publishes live AIS vessel positions, port call schedules, and sea-state buoy readings for Finnish waters. This project builds a fully automated ingestion-to-analytics pipeline that:

1. **Polls three independent REST endpoints** (AIS locations, port calls, sea state) and lands raw JSON snapshots in Azure Data Lake Storage
2. **Ingests incrementally into Bronze** via Databricks Auto Loader, with schema evolution and full lineage (`ingested_at`, `source_file`) on every row
3. **Cleans and upserts into Silver** using Structured Streaming `foreachBatch` merge logic, keyed per entity **and per event** (`mmsi + timestamp`, `port_call_id`, `site_number + last_update`) — so history is preserved rather than each entity being collapsed to a single row
4. **Deduplicates within each microbatch** before merging, so a batch containing multiple rows for the same merge key never fails Delta's `MERGE` with a multiple-match error
5. **Builds two Gold tables**: a live current-state snapshot (`current_vessel_status`) and a full historical trajectory (`vessel_trajectory`), joined against vessel identity from port calls
6. **Runs each data source on its own schedule**, matched to how often the underlying source actually changes, rather than forcing everything onto one refresh cycle

Each of the three sources is fetched, bronzed, and silvered completely independently — a slow-changing source (sea state) doesn't hold back a fast-changing one (AIS), and a stalled job on one source doesn't break the others.

---

## System Architecture

    ┌──────────────────────────────────────────────────────────────────────┐
    │                         LANDING (raw JSON)                           │
    │                                                                        │
    │  01_fetch_to_landing.py                                               │
    │    ├─► GET /ais/v1/locations      ──► landing/ais_locations/*.json   │
    │    ├─► GET /port-call/v1/port-calls ─► landing/port_calls/*.json     │
    │    └─► GET /sse/v1/measurements   ──► landing/sea_state/*.json       │
    │  (parameterized via a `datasets` widget — each job run fetches       │
    │   only the source(s) it owns)                                        │
    └──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
    ┌──────────────────────────────────────────────────────────────────────┐
    │                     BRONZE (Auto Loader, append-only)                │
    │                                                                        │
    │  02_bronze_autoloader.py                                              │
    │    cloudFiles JSON stream ──► + ingested_at, source_file             │
    │    ──► maritime_ais.maritime_bronze.{ais_locations,port_calls,       │
    │                                        sea_state}                     │
    └──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
    ┌──────────────────────────────────────────────────────────────────────┐
    │              SILVER (streaming merge/upsert, composite key)          │
    │                                                                        │
    │  03_silver_ais.py         ──► merge on mmsi + timestamp              │
    │  03_silver_port_calls.py  ──► merge on port_call_id                  │
    │  03_silver_sea_state.py   ──► merge on site_number + last_update     │
    │                                (null-safe)                            │
    │                                                                        │
    │  Each explodes its bronze GeoJSON/array payload, casts types,        │
    │  deduplicates the microbatch on its merge key (row_number/window),   │
    │  and upserts into maritime_ais.maritime_silver.*                     │
    │  Uses `microBatchDF.sparkSession` (not the global `spark`) inside    │
    │  foreachBatch for Spark Connect compatibility.                       │
    └──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
    ┌──────────────────────────────────────────────────────────────────────┐
    │                  GOLD (batch overwrite, two tables)                  │
    │                                                                        │
    │  04_silver_to_gold.py                                                 │
    │    ├─► ais_locations (latest per mmsi) ⋈ latest port_calls           │
    │    │   ──► maritime_ais.maritime_gold.current_vessel_status          │
    │    │       (1 row per vessel — current state)                        │
    │    │                                                                  │
    │    └─► ais_locations (full history) ⋈ vessel_name (from port_calls)  │
    │        ── drop rows where lat/lon/speed unchanged vs. previous ping  │
    │        ──► maritime_ais.maritime_gold.vessel_trajectory              │
    │            (many rows per vessel — full movement history)            │
    └──────────────────────────────────────────────────────────────────────┘

---

## Job Schedules

Each source is fetched and processed on a cadence matched to how often
Digitraffic actually updates it — polling faster than the source changes
just burns compute for no benefit, and polling slower than it changes
introduces avoidable staleness.

| Job | Sources | Schedule | Tasks | Writes Gold? |
|---|---|---|---|---|
| `ais_fast_refresh` | `ais_locations` | Every 2 min | fetch → bronze → silver → **gold** | ✅ (sole writer) |
| `port_calls_refresh` | `port_calls` | Every 15 min | fetch → bronze → silver | ❌ |
| `sea_state_refresh` | `sea_state` | Every 30 min | fetch → bronze → silver | ❌ |

**Only one job ever writes Gold.** Both `current_vessel_status` and
`vessel_trajectory` are rebuilt via `mode("overwrite")` on every
`ais_fast_refresh` run, reading whatever Silver `ais_locations` and
`port_calls` data is currently committed — so vessel names, ports, and
ETAs are picked up automatically on the very next 2-minute cycle after
`port_calls_refresh` updates them. No coordination between jobs is
needed, and no two jobs ever race to overwrite the same table.

> **Note on "real-time":** this pipeline is near-live, not streaming to a
> live map. Each source updates on its own polling cadence (2–30 min), and
> any downstream dashboard reflects data as of the last completed run —
> not sub-second position updates. This matches how commercial AIS
> providers (MarineTraffic, VesselFinder) work as well, since vessels
> themselves only broadcast AIS pings every few seconds to minutes.

`sea_state` currently feeds Silver only; it isn't joined into Gold yet
(see Future Work).

---

## Repository Structure

    Digitraffic-Maritime-Pipeline/
    └── Notebooks/
        ├── 01_fetch_to_landing.py     # Polls Digitraffic REST APIs → raw JSON in landing zone
        ├── 02_bronze_autoloader.py    # Auto Loader: landing JSON → Bronze Delta (append-only)
        ├── 03_silver_ais.py           # Bronze → Silver: AIS locations (merge on mmsi + timestamp)
        ├── 03_silver_port_calls.py    # Bronze → Silver: port calls (merge on port_call_id)
        ├── 03_silver_sea_state.py     # Bronze → Silver: sea state (merge on site_number + last_update)
        └── 04_silver_to_gold.py       # Silver → Gold: current_vessel_status + vessel_trajectory

Both `01_fetch_to_landing` and `02_bronze_autoloader` accept a `datasets`
widget parameter (comma-separated), so the same notebook code is reused
across all three jobs — each job just passes the dataset(s) it owns.

---

## Data Model

| Layer | Table | Grain | Write mode |
|---|---|---|---|
| Bronze | `ais_locations` | 1 row per AIS ping | Append (streaming) |
| Bronze | `port_calls` | 1 row per port call event | Append (streaming) |
| Bronze | `sea_state` | 1 row per buoy reading | Append (streaming) |
| Silver | `ais_locations` | 1 row per vessel **per timestamp** (mmsi + timestamp) | Merge/upsert |
| Silver | `port_calls` | 1 row per port call (port_call_id) | Merge/upsert |
| Silver | `sea_state` | 1 row per buoy **per reading** (site_number + last_update) | Merge/upsert |
| Gold | `current_vessel_status` | 1 row per vessel, current state | Overwrite (batch) |
| Gold | `vessel_trajectory` | Many rows per vessel — one per **changed** position/speed | Overwrite (batch) |

**Design change from earlier version:** Silver `ais_locations` and
`sea_state` are now keyed on a **composite key** (entity ID + event
timestamp) instead of entity ID alone. This means each new ping is
inserted as a new row rather than overwriting the previous one, which is
what makes historical trajectory possible in Gold. Because a single
ingestion microbatch can occasionally contain more than one row sharing
the same composite key (e.g. duplicate AIS broadcasts within one poll),
each merge function first deduplicates the microbatch per key
(`row_number()` over a window) before calling `MERGE`, avoiding Delta's
`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` error.

`vessel_trajectory` additionally filters out consecutive duplicate
readings — a new ping is only kept if `latitude`, `longitude`, or
`speed_over_ground` differs from that vessel's previous recorded row —
so the table reflects genuine movement/state changes rather than
unchanged repeated pings.

---

## Tech Stack

| Tool | Role |
|---|---|
| PySpark Structured Streaming | Bronze ingestion, Silver merge/upsert |
| Databricks Auto Loader (`cloudFiles`) | Incremental, schema-evolving JSON ingestion |
| Delta Lake | ACID storage layer across all three medallion tiers |
| Unity Catalog | Catalog/schema management, table lineage, access grants |
| Databricks Lakeflow Jobs | Multi-cadence orchestration, DAG dependencies |
| Databricks SQL Warehouse | Serverless SQL compute for querying Gold tables and powering dashboards |
| Azure Data Lake Storage (ABFSS) | Underlying object storage for landing/bronze/silver/gold |
| Digitraffic Marine API | Live AIS, port call, and sea-state data source |
| folium | Interactive Leaflet-based vessel map (ad hoc / notebook use) |

---

## Installation & Usage

This pipeline is designed to run entirely inside Databricks via Lakeflow
Jobs — there's no local execution path, since it depends on Unity Catalog
tables, `dbutils`, and `spark` session objects available only in a
Databricks runtime.

    # 1. Clone into Databricks Repos (Workspace → Repos → Add Repo)
    https://github.com/<your-username>/Digitraffic-Maritime-Pipeline.git

    # 2. Create the Unity Catalog schemas (run once)
    # — see the CREATE SCHEMA statements referenced in 02_bronze_autoloader

    # 3. Create three Lakeflow Jobs, each wiring up the notebooks above
    #    with the `datasets` parameter set per the Job Schedules table

    # 4. Create a Databricks SQL Warehouse (SQL → SQL Warehouses → Create)
    #    for querying Gold and building dashboards, separate from the
    #    all-purpose/job cluster running the pipeline notebooks

### Querying the data

    -- Live fleet snapshot: one row per vessel, current position
    SELECT * FROM maritime_ais.maritime_gold.current_vessel_status;

    -- Full movement history for one vessel, chronologically
    SELECT mmsi, vessel_name,
           date_format(timestamp, 'yyyy-MM-dd HH:mm:ss') AS recorded_at,
           latitude, longitude, speed_over_ground
    FROM maritime_ais.maritime_gold.vessel_trajectory
    WHERE mmsi = '<mmsi>'
    ORDER BY timestamp;

    -- Only vessels currently underway, all history
    SELECT * FROM maritime_ais.maritime_gold.vessel_trajectory
    WHERE speed_over_ground > 0
    ORDER BY mmsi, timestamp;

### Table maintenance

`vessel_trajectory` is queried primarily by `mmsi`, and grows unbounded
over time. Rather than physically partitioning by `mmsi` (which, with
hundreds–thousands of vessels, would create too many small partitions),
file layout is optimized with Z-ORDER after each Gold rebuild:

    OPTIMIZE maritime_ais.maritime_gold.vessel_trajectory ZORDER BY (mmsi, timestamp);
    OPTIMIZE maritime_ais.maritime_gold.current_vessel_status ZORDER BY (mmsi);

If/when the table grows into the tens of millions of rows spanning
months, revisit with date-based partitioning (`event_date`) combined with
Z-ORDER.

### Visualizing results

In the Databricks SQL Editor, after running a query: click **+** above
the results grid → **Visualization** → choose **Map**, set Latitude /
Longitude to the `latitude` / `longitude` columns, and optionally group
or color by `mmsi`. Save, then **Add to dashboard** to pin it for
recurring viewing. Dashboards reflect data as of the last Gold rebuild —
refresh manually or on a schedule to see newer pings.

---

## Known Limitations

- Streaming checkpoints are tied to the underlying Delta table's internal
  ID; if a Bronze table is ever dropped and recreated, its dependent
  Silver stream fails with `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`
  until its checkpoint is manually cleared. A full pipeline reset requires
  dropping tables **and** clearing `checkpoints/` and `schemas/` paths for
  every affected dataset, not just the tables themselves.
- `sea_state` is ingested and merged into Silver but not yet joined into
  either Gold table.
- Job cluster startup time can cause the 2-minute `ais_fast_refresh`
  schedule to skip a cycle if the previous run's cluster hasn't finished
  spinning up in time.
- `vessel_trajectory` only accumulates meaningful history once the
  pipeline has run repeatedly over time — a single run produces one ping
  per vessel, which is not yet a "trajectory."
- Vessel name in `vessel_trajectory` is sourced from `port_calls`, so any
  `mmsi` with no port call history on record shows a null `vessel_name`.
  AIS locations alone don't carry vessel identity in this feed.

---

## Future Work

- Join `sea_state` into Gold (either into `current_vessel_status` or a
  separate table) so buoy conditions are queryable alongside vessel
  position
- Pull vessel static/metadata (name, type, dimensions) from a dedicated
  AIS metadata endpoint so `vessel_trajectory` doesn't depend on
  `port_calls` for identity
- Automated map regeneration (folium → Azure static hosting) on a
  schedule, decoupled from the fast AIS refresh cycle
- Data quality checks / alerting on stale or missing source data
  (e.g. Digitraffic API downtime detection)
- Evaluate date-based partitioning for `vessel_trajectory` once row
  volume grows significantly
