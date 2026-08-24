<div align="center">
  <img width="4962" height="1942" alt="image" src="https://github.com/user-attachments/assets/912983a5-6ce9-4f5a-89f0-73d13a2030c7" />
</div>

# Weather Databricks Lakehouse Pipeline

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
                                                                                    │
                                                                       (Databricks SQL & Dashboards)
                                                                                    ▼
                                                                      [ Lakehouse Dashboards v3 ]

```

---

## 2. Medallion Layer Design

```text
Reading REST API JSON payloads 
   └─► Writing Delta/Parquet files to Bronze (`brz_weather_observations`)
         └─► Reading Delta/Parquet from Bronze 
               ├─► Writing Clean Delta/Parquet to Silver (`slv_weather_observations`)
               └─► Writing Invalid Delta/Parquet to Quarantine (`slv_invalid_records`)
                     └─► Reading Delta/Parquet from Silver
                           └─► Writing Star Schema Delta/Parquet to Gold (`fact_` & `dim_` tables)
```

* **Bronze (`weather_catalog.bronze`)**
  - `brz_weather_observations`: Immutable raw landing layer preserving original API payloads alongside ingestion metadata (`batch_id`, `ingestion_timestamp`, `source_system`).

* **Silver (`weather_catalog.silver`)**
  - `slv_weather_observations`: Cleaned, flattened, and type-casted schema. Idempotent upserts performed via `MERGE INTO` based on natural key (`name`, `last_updated_epoch`).
  - `slv_invalid_records`: Quarantine table isolating records failing domain validation (e.g., negative visibility, invalid lat/lon coordinates, null keys).

* **Gold (`weather_catalog.gold`)**
* **Star Schema Dimensional Model**:
  - `dim_location`: Dimension table containing spatial details keyed by MD5 location hashes.
  - `dim_weather_condition`: Lookup dimension mapping condition codes to descriptive texts.
  - `fact_weather_observations`: Central fact table capturing fine-grained numerical metrics.

* `gld_daily_weather_metrics`: Pre-calculated analytical summary product providing daily averages, totals, and thresholds.

* **Observability (`weather_catalog.observability`)**
  - `data_quality_results`: Automated validation audit logs.
  - `pipeline_runs`: Per-batch row movement and execution tracking logs.

---

## 3. SQL Analytics & Lakehouse Dashboards

The serving layer features analytical Databricks SQL queries feeding interactive **Lakehouse Dashboards (v3)**:

* **Daily Temperature Trends**: Time-series line chart tracking average and extreme daily temperatures across monitored cities (`Daily Temperature Trends by City`).
* **Rain & Extreme Weather Summary**: Grouped combo/bar charts visualizing cumulative rainfall (`cumulative_rain_mm`) alongside peak wind speed metrics (`peak_wind_kph`).
* **Pipeline Observability Health**: Operational monitoring visualizers evaluating real-time batch processing volumes, quarantine counts, and execution latency (`Pipeline Observability Health`).

---

## 4. Data Quality, Testing & CI/CD

* **Quarantine Enforcement**: Invalid records are filtered and routed to `slv_invalid_records` rather than dropped silently.
* **Automated Unit Testing**: PyTest test suite executing isolated local Spark session assertions against JSON schema parsing and quarantine domain filtering logic (`tests/test_transformations.py`).
* **CI/CD Automation**: GitHub Actions workflow (`.github/workflows/ci_cd.yml`) executing PyTest unit tests on pull requests and deploying code changes directly to Databricks upon merging into `main`.
* **Databricks Asset Bundles (DABs)**: Declarative multi-environment infrastructure-as-code management across `dev` and `prod` targets via `databricks.yml`.

---

## 5. Orchestration & Operations

The pipeline is fully automated using **Databricks Workflows** running on a sequential task DAG:

1. `01_ingest_bronze`: Parameterized REST API extraction into Bronze.
2. `02_transform_silver`: Flattening, quality routing, and Silver MERGE.
3. `03_transform_gold`: Star Schema dimension/fact population and daily metric rollups.
4. `04_quality_and_observability`: Automated DQ assertion execution and pipeline run logging.
5. `05_table_maintenance`: Operational file compaction (`OPTIMIZE`) and stale file cleanup (`VACUUM`).

---

## 6. Project Structure

```text
├── .github/
│   └── workflows/
│       └── ci_cd.yml
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
├── tests/
│   └── test_transformations.py
├── databricks.yml
└── README.md

```
