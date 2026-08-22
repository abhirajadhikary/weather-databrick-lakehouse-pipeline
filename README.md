# Weather Intelligence Lakehouse Platform

An end-to-end, batch-oriented data platform built on Databricks designed to ingest, validate, model, and serve weather observation data using Medallion Architecture and Delta Lake.

## 1. System Architecture
```text
[ External API: WeatherAPI ] 
              │
              ▼
    [ Ingestion Framework ] ──(Raw JSON & Metadata)──► [ BRONZE: brz_weather_observations ]
                                                                   │
                                                           (Clean, Flatten, Validate)
                                                                   ▼
    [ QUARANTINE: slv_invalid_records ] ◄──(Invalid)─── [ SILVER: slv_weather_observations ]
                                                                   │
                                                        (Delta Change Data Feed)
                                                                   ▼
                                                       [ GOLD DATA WAREHOUSE ]
                                                  ┌────────────────┼────────────────┐
                                                  ▼                ▼                ▼
                                            dim_location    dim_condition   fact_observations
                                                                                    │
                                                                           (Daily Aggregations)
                                                                                    ▼
                                                                        gld_daily_weather_metrics
```

---

## 2. Medallion Layer Design

* **Bronze (`weather_catalog.bronze`)**
* `brz_weather_observations`: Immutable raw landing layer preserving original API payloads alongside ingestion metadata (`batch_id`, `ingestion_timestamp`, `source_system`).


* **Silver (`weather_catalog.silver`)**
* `slv_weather_observations`: Cleaned, flattened, and type-casted schema. Idempotent upserts performed via `MERGE INTO` based on natural key (`name`, `last_updated_epoch`).
* `slv_invalid_records`: Quarantine table isolating records failing domain validation (e.g., negative visibility, invalid lat/lon coordinates, null keys).


* **Gold (`weather_catalog.gold`)**
* **Star Schema Dimensional Model**:
* `dim_location`: Dimension table containing spatial details keyed by MD5 location hashes.
* `dim_weather_condition`: Lookup dimension mapping condition codes to descriptive texts.
* `fact_weather_observations`: Central fact table capturing fine-grained numerical metrics.


* `gld_daily_weather_metrics`: Pre-calculated analytical summary product providing daily averages, totals, and thresholds.


* **Observability (`weather_catalog.observability`)**
* `data_quality_results`: Automated validation audit logs.
* `pipeline_runs`: Per-batch row movement and execution tracking logs.



---

## 3. Data Quality & Observability

* **Quarantine Enforcement**: Invalid records are filtered and routed to `slv_invalid_records` rather than dropped silently.
* **Automated Audit Checks**:
* Uniqueness validation on primary keys in Gold fact tables.
* Record contamination checks tracking quarantined batch counts.
* Batch lineage tracking comparing row volumes from Bronze through Gold.

---

## 4. Orchestration & Operations

The pipeline is fully automated using **Databricks Workflows** running on a sequential task DAG:

1. `01_ingest_bronze`: Parameterized REST API extraction into Bronze.
2. `02_transform_silver`: Flattening, quality routing, and Silver MERGE.
3. `03_transform_gold`: Star Schema dimension/fact population and daily metric rollups.
4. `04_quality_and_observability`: Automated DQ assertion execution and pipeline run logging.
5. `05_table_maintenance`: Operational file compaction (`OPTIMIZE`) and stale file cleanup (`VACUUM`).

---

## 5. Local Project Structure

```text
├── configs/
│   └── config.json
├── notebooks/
│   ├── 01_ingest_bronze
│   ├── 02_transform_silver
│   ├── 03_transform_gold
│   ├── 04_quality_and_observability
│   └── 05_table_maintenance
├── src/
│   ├── ingestion/
│   │   └── api_client.py
│   ├── transformations/
│   │   ├── silver_transformer.py
│   │   └── gold_transformer.py
│   ├── quality/
│   │   └── dq_rules.py
│   ├── observability/
│   │   └── pipeline_metrics.py
│   └── utils/
│       └── table_maintenance.py
└── README.md

```