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
