# 🚢 Digitraffic Maritime Pipeline — Multi-Cadence AIS Medallion Architecture

> A Databricks Lakeflow pipeline that ingests live vessel AIS positions, port calls, and sea-state data from Finland's Digitraffic API, processes them through a bronze/silver/gold medallion architecture, and publishes a live vessel-tracking snapshot — each data source refreshed at its own natural update cadence.

![Python](https://img.shields.io/badge/Python-3.x-blue?style=flat-square&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-Structured%20Streaming-orange?style=flat-square&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion%20Architecture-teal?style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-Lakeflow%20Jobs-red?style=flat-square&logo=databricks)
![Azure](https://img.shields.io/badge/Azure-Data%20Lake%20Storage-0089D6?style=flat-square&logo=microsoftazure)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

---

## Overview

The [Digitraffic Marine API](https://www.digitraffic.fi/en/marine-traffic/) publishes live AIS vessel positions, port call schedules, and sea-state buoy readings for Finnish waters. This project builds a fully automated ingestion-to-analytics pipeline that:

1. **Polls three independent REST endpoints** (AIS locations, port calls, sea state) and lands raw JSON snapshots in Azure Data Lake Storage
2. **Ingests incrementally into Bronze** via Databricks Auto Loader, with schema evolution and full lineage (`ingested_at`, `source_file`) on every row
3. **Cleans and upserts into Silver** using Structured Streaming `foreachBatch` merge logic, keyed per entity (`mmsi`, `port_call_id`, `site_number`) so each table always reflects current state
4. **Joins into a Gold snapshot table** (`current_vessel_status`) — every vessel's live position enriched with its latest destination port and ETA
5. **Runs each data source on its own schedule**, matched to how often the underlying source actually changes, rather than forcing everything onto one refresh cycle

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
    │                    SILVER (streaming merge/upsert)                   │
    │                                                                        │
    │  03_silver_ais.py         ──► merge on mmsi                          │
    │  03_silver_port_calls.py  ──► merge on port_call_id                  │
    │  03_silver_sea_state.py   ──► merge on site_number (null-safe)       │
    │                                                                        │
    │  Each explodes its bronze GeoJSON/array payload, casts types,        │
    │  and upserts into maritime_ais.maritime_silver.*                     │
    └──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
    ┌──────────────────────────────────────────────────────────────────────┐
    │                    GOLD (batch overwrite snapshot)                   │
    │                                                                        │
    │  04_silver_to_gold.py                                                 │
    │    ais_locations ⋈ latest port_calls (per-mmsi window, rn = 1)       │
    │    ──► maritime_ais.maritime_gold.current_vessel_status              │
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

**Only one job ever writes Gold.** `current_vessel_status` is rebuilt via
`mode("overwrite")` on every `ais_fast_refresh` run; since that run always
reads whatever silver `port_calls` data is currently committed, ports/ETAs
are picked up automatically on the very next 2-minute cycle after
`port_calls_refresh` updates them — no coordination between jobs is
needed, and no two jobs ever race to overwrite the same table.

`sea_state` currently feeds Silver only; it isn't joined into Gold yet
(see Future Work).

---

## Repository Structure

    Digitraffic-Maritime-Pipeline/
    └── Notebooks/
        ├── 01_fetch_to_landing.py     # Polls Digitraffic REST APIs → raw JSON in landing zone
        ├── 02_bronze_autoloader.py    # Auto Loader: landing JSON → Bronze Delta (append-only)
        ├── 03_silver_ais.py           # Bronze → Silver: AIS locations (merge on mmsi)
        ├── 03_silver_port_calls.py    # Bronze → Silver: port calls (merge on port_call_id)
        ├── 03_silver_sea_state.py     # Bronze → Silver: sea state (merge on site_number)
        └── 04_silver_to_gold.py       # Silver → Gold: current_vessel_status snapshot

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
| Silver | `ais_locations` | 1 row per vessel (mmsi) | Merge/upsert |
| Silver | `port_calls` | 1 row per port call (port_call_id) | Merge/upsert |
| Silver | `sea_state` | 1 row per buoy (site_number) | Merge/upsert |
| Gold | `current_vessel_status` | 1 row per vessel, current state | Overwrite (batch) |

**Design trade-off:** Silver AIS and Silver sea-state are keyed on entity
ID with `whenMatchedUpdateAll`, meaning each vessel/buoy keeps exactly one
row — the latest ping overwrites the previous one. This gives a live
snapshot but **no historical trajectory**. If historical vessel tracks are
ever needed, Silver AIS would need to become append-only (partitioned by
ingestion time) instead of upsert-keyed (see Future Work).

---

## Tech Stack

| Tool | Role |
|---|---|
| PySpark Structured Streaming | Bronze ingestion, Silver merge/upsert |
| Databricks Auto Loader (`cloudFiles`) | Incremental, schema-evolving JSON ingestion |
| Delta Lake | ACID storage layer across all three medallion tiers |
| Unity Catalog | Catalog/schema management, table lineage |
| Databricks Lakeflow Jobs | Multi-cadence orchestration, DAG dependencies |
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

### Querying the live snapshot

    SELECT * FROM maritime_ais.maritime_gold.current_vessel_status;

---

## Known Limitations

- Silver AIS and sea-state tables are upsert-keyed, so no historical
  vessel-track data is retained — only current state.
- Streaming checkpoints are tied to the underlying Delta table's internal
  ID; if a Bronze table is ever dropped and recreated, its dependent
  Silver stream fails with `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`
  until its checkpoint is manually cleared.
- `sea_state` is ingested and merged into Silver but not yet joined into
  the Gold vessel-status table.
- Job cluster startup time can cause the 2-minute `ais_fast_refresh`
  schedule to skip a cycle if the previous run's cluster hasn't finished
  spinning up in time.

---

## Future Work

- Join `sea_state` into `current_vessel_status` (or a separate Gold table)
  so buoy conditions are queryable alongside vessel position
- Append-only historical Silver AIS table (partitioned by ingestion time)
  to support vessel-track playback and speed/course analytics over time
- Automated map regeneration (folium → Azure static hosting) on a
  schedule, decoupled from the fast AIS refresh cycle
- Data quality checks / alerting on stale or missing source data
  (e.g. Digitraffic API downtime detection)
