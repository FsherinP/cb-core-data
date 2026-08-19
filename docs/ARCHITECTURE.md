# cb-core-data — Architecture

> **Scope note:** this document describes the pipeline as it exists on
> `cbrelease-4.8.39.1-hotfix`. The current working branch adds a
> performance-profiling / phase-clocking layer (timers wrapped around each
> job's phases) on top of this same logic — that instrumentation is
> intentionally **not** described here; everything below is the underlying
> business logic it wraps.
>
> A companion interactive view of everything in this document —
> filterable/searchable by job — lives at
> [`docs/pipeline-overview.html`](pipeline-overview.html). Full per-job
> detail (every read, join and write) is in [`JOB_DETAILS.md`](JOB_DETAILS.md).
> Every named Parquet/warehouse table is catalogued in
> [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md).

## What this repository is

`cb-core-data` is the analytics pipeline behind **Karmayogi Bharat / iGOT**, a
learning platform for Indian government employees. It is a batch,
PySpark-based ETL system (with DuckDB used for the heaviest SQL joins) that:

1. pulls raw activity data out of the platform's operational stores
   (Cassandra, Elasticsearch, Redis, MongoDB, Druid, Postgres),
2. joins that raw data into a small set of reusable, denormalized tables, and
3. runs ~40 independent, requirement-specific jobs on top of those tables —
   each one producing a CSV report, a Postgres/BigQuery warehouse table, a
   Redis cache entry, a Kafka event, or some combination of those.

There is no single "main.py that runs everything." `jobs/main.py` exists and
orchestrates four of the Stage 2 jobs in sequence, but in practice each job
under `jobs/stage-0/`, `jobs/stage-1/` and `jobs/stage-2/` is its own
`spark-submit`-able script with its own `main()`. What ties them together is
not code — it's the shared set of Parquet table names in
`constants/ParquetFileConstants.py` that most of them read from or write to.

## The three stages

```
   Cassandra · Elasticsearch · Postgres(app) · MongoDB · Druid · REST APIs
                                  │
                                  ▼
   STAGE 0 — Extract        dataExhaust.py
   One job. ~29 reads, 4 joins. Writes 27 raw Parquet caches under
   data-res/pq_files/cache_pq/<name>/ — one folder per source table/query.
                                  │
                                  ▼
   STAGE 1 — Prejoin         prejoinData.py  (+ 7 dfutil library modules)
   One orchestrator running 18 steps. ~45 joins across those steps. Writes
   ~27 computed Parquet tables under output/computed/<name>/ — these are
   the tables almost every Stage 2 job reads from.
                                  │
                                  ▼
   STAGE 2 — Reports, warehouse & cache      ~40 independent jobs
   Each job reads Stage 1's computed tables (sometimes going back to
   Cassandra/Postgres/Redis/Druid directly), does its own joins, and
   produces ONE of: a per-org CSV report, a warehouse table, a Redis
   cache entry, a Kafka message, or a Cassandra write.
                                  │
                    ┌─────────────┼─────────────┬───────────────┐
                    ▼             ▼             ▼               ▼
             Postgres warehouse BigQuery   Redis (dashboard)  GCS (zipped,
             (dataWarehouse.py/  (bq-        (dashboardSync.py  password-
             *Bash.sh)           scripts.sh) + several others) protected CSVs,
                                                                 via zipUpload.py)
```

### Stage 0 — Extract (`jobs/stage-0/dataExhaust.py`)

The single point where every raw external system enters the pipeline. One
class, `DataExhaustModel`, with one `process_data()` method that runs ~29
reads in sequence:

- **Cassandra** — the bulk of the reads: enrolments, course batches,
  content/framework/questionset hierarchy, ratings, ACBP plans, user roles,
  org records, karma points, learner-leaderboard snapshot, extended user
  profiles, old (pre-migration) assessments, external/marketplace
  enrolments, unenrolment audit history, CAP access-control rules.
- **Elasticsearch** — 5 separate `compositesearch`/`fs-forms` queries: course
  catalog, events, final/standalone assessment metadata, course-completion
  survey submissions.
- **Postgres (app schema, `sunbird`)** — org hierarchy
  (`org_hierarchy_v4`), learner engagement stats (`learner_stats`),
  marketplace content (`cios_content_entity`).

Every read is written straight to Parquet under
`data-res/pq_files/cache_pq/<name>/`, with **zero transformation** beyond
column renaming and light JSON parsing — the goal of this stage is purely to
get a fast, disk-local snapshot of "what does the source system say right
now," so Stage 1 and Stage 2 never need a live connection to Cassandra,
Elasticsearch or the app database. The only real joins in this stage attach
Postgres's org hierarchy onto Cassandra's org table (3 joins) and attach
event metadata onto event enrolments (1 join).

### Stage 1 — Prejoin (`jobs/stage-1/prejoinData.py`)

A thin orchestrator (159 lines) that runs 18 steps, each a function call into
one of seven shared library modules under `dfutil/`:

| # | Step | Module |
|---|---|---|
| 1 | Clean/derive assessment pass-fail logic | `assessmentDFUtil.parse_raw_assessment_data` |
| 2 | Org + hierarchy | `userDFUtil.preComputeOrgWithHierarchy` |
| 3 | Content ratings & summary | `contentDFUtil.preComputeRatingAndSummaryDataFrame` |
| 4 | Course/program catalog (from ES) | `contentDFUtil.preComputeAllCourseProgramESDataFrame` |
| 5 | Content master table | `contentDFUtil.preComputeContentDataFrame` |
| 6 | Content hierarchy | `contentDFUtil.precomputeContentHierarchyDataFrame` |
| 7 | Assessment ES frame | `assessmentDFUtil.precomputeAssessmentEsDataframe` |
| 8 | External/marketplace content | `contentDFUtil.preComputeExternalContentDataFrame` |
| 9 | User profile master | `userDFUtil.preComputeUser` |
| 10 | Enrolment master table | `enrolmentDFUtil.preComputeEnrolment` |
| 11 | External enrolment | `enrolmentDFUtil.preComputeExternalEnrolment` |
| 12 | User × org master (`USER_ORG_COMPUTED_FILE`) | `userDFUtil.preComputeOrgHierarchyWithUser` |
| 13 | Enrolment-warehouse frame | `enrolmentDFUtil.preComputeUserEnrolmentWarehouseData` |
| 14 | User-warehouse frame | `userDFUtil.preComputeUserWarehouseData` |
| 15 | Content-warehouse frame | `contentDFUtil.preComputeContentWarehouseData` |
| 16 | Direct warehouse Parquet writes (org, events) | `contentDFUtil.writeWarehouseParquetFiles` |
| 17 | Legacy assessment data | `assessmentDFUtil.precomputeOldAssessmentDataframe` |
| 18 | ACBP allocation engine | `acbpDFUtil_v3.preComputeACBPData` |

Two tables produced here matter more than the rest, because nearly
everything downstream reads them:

- **`USER_ORG_COMPUTED_FILE`** — every user, joined to their organization,
  ministry and department. The single most-read table in the repository.
- **`CONTENT_COMPUTED_PARQUET_FILE`** — every course/program, joined to its
  rating and org.
- **`ENROLMENT_COMPUTED_PARQUET_FILE`** and **`ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE`**
  — the enrolment/completion facts, in two shapes (an internal "computed"
  shape most report jobs use, and a warehouse-column-named shape that
  `dashboardSync.py`'s whole SQL library is built on).

The **ACBP allocation engine** (step 18) deserves a special note: unlike
every other step, it runs in **DuckDB, not Spark**, because it has to match
every user against every criterion (org, designation, cadre, group, batch,
service, custom fields…) of every active training plan — roughly 40 SQL
joins across 9 criteria types, run twice (once for org-scoped plans, once
for plans with no org restriction), chunked 2 million users at a time. The
same engine pattern is reused by `capAllotment.py` in Stage 2 for CAP
access-control resolution.

### Stage 2 — Reports, warehouse & cache (~40 jobs)

Every job here is independent — none of them import each other, and most
have their own `SparkSession`. What connects them is shared table names.
They fall into five natural groups (also used as the filter categories in
the interactive HTML view):

| Group | Jobs | What it's for |
|---|---|---|
| **Warehouse & dashboard sync** | `dataWarehouse.py`, `dataWarehouseBash.sh`, `dashboardSync.py` | Push computed tables into Postgres/BigQuery and Redis |
| **Compliance & enrolment reports** | `acbpReport`, `assessmentReport`, `bharatKalpReport`, `blendedReport`, `capAllotment`, `courseBasedAssessmentReport`, `courseCompletionSurveyReport`, `courseReport` | Per-MDO CSVs on training compliance, content and assessments |
| **Gamification & scoring** | `dsrComputation(Updated)`, `gamificationJob`, `gamificationNotification{Producer,Consumer}`, `inappReview`, `karmaPoints`, `kcmReport`, `l2Assessments` | Daily KPIs, badges, karma points, competency mapping |
| **Leaderboards & campaigns** | `learnerLeaderboard`, `ministryLeaderboard`, `ministryMetrics`, `nationalLearningWeek`, `npsUpgraded`, `odcsRecommendation`, `org_hierarchy`, `peerValidation{EligibleUsers,NotificationSender}`, `programProgressSyncList_v5` | Rankings, campaign metrics, notification triggers |
| **Surveys & export** | `surveyQuestionReport`, `surveyStatusReport`, `unenrollmentReport`, `userActivity`, `userDataToRedis`, `userEnrolment`, `userReport`, `weeklyClaps`, `workFlowSummarizer`, `zipUpload` | Survey exports, the two "core" user reports, and final packaging |

Two jobs are explicitly part of an orchestrated sequence, not standalone:
`jobs/main.py` runs Stage 1, then `userReport.py` ("Stage 2A"),
`assessmentReport.py` ("Stage 2B"), `kcmReport.py` ("Stage 2C") and
`userEnrolment.py` ("Stage 2D") in that order. Every other Stage 2 job is
invoked on its own.

## How the warehouse actually gets updated

This pipeline has **two separate "warehouse" concepts** and it's easy to
conflate them:

1. **Postgres/BigQuery data warehouse** — a set of 12 tables (`user_detail`,
   `content`, `content_resource`, `assessment_detail`, `bp_enrolments`,
   `cb_plan`, `org_hierarchy`, `kcm_content_mapping`, `kcm_dictionary`,
   `events`, `events_enrolment`, `user_enrolments`) that BI/Looker dashboards
   query. Individual report jobs (`courseReport.py`, `userReport.py`,
   `userEnrolment.py`, etc., plus `contentDFUtil.writeWarehouseParquetFiles`
   in Stage 1) each stage their own slice of this as local Parquet under
   `warehouseReportDir` (`<BASE_DIR>/warehouse/<table>/`). Two separate jobs
   then push that Parquet into the real databases:
   - **`dataWarehouse.py`** — Spark, JDBC `mode="overwrite"` — i.e. a full
     truncate-and-reload of every table, every run. No incremental/upsert
     logic anywhere.
   - **`dataWarehouseBash.sh`** — a DuckDB/bash reimplementation of the same
     12 tables (`ATTACH ... TYPE postgres; TRUNCATE; INSERT`), with explicit
     column casts and dedup that `dataWarehouse.py` lacks. **Both exist in
     this branch** — which one is actually cron-scheduled in production
     cannot be determined from the repository alone.
   - **`bq-scripts.sh`** then does the equivalent for BigQuery: for the same
     12 tables, stage to GCS → drop the BigQuery table → recreate it empty →
     `bq load` the staged Parquet → clean up the staging files. Also a full
     replace every run.

2. **Redis "warehouse"** (informal) — the live dashboard's cache.
   `dashboardSync.py` reads the same warehouse-computed Parquet tables via
   DuckDB SQL (the query library in `constants/QueryConstants.py`) and
   writes **50+ keys** into Redis — org counts, leaderboards, trending
   content, certification counts, campaign tickers. It **never writes to
   Postgres**. This is what makes the admin/learner dashboard feel
   real-time: the UI reads Redis, not the database.

A handful of newer warehouse outputs (`cbp_enrollments`, `apar_cbp_enrollment`,
`bk_course_enrolments`/`bk_event_enrolments`, `cap_allocation_meta`/
`cap_allocation_user_wise`, `user_activity`, `unenrolled_user_audit`,
`userCustomFields`) are written as Parquet by their respective jobs but are
**not yet referenced** by `dataWarehouse.py` or `bq-scripts.sh` — see the
Data Dictionary for the full picture.

## Cross-job dependencies worth knowing about

Because there's no orchestration DAG in this repo, the only way to know
"job B needs job A to have run first" is to trace shared table names by
hand. The dependencies that matter most:

- **`courseReport.py` → `capAllotment.py`** — capAllotment reads the
  `content` warehouse table that courseReport.py writes.
- **`kcmReport.py` → `l2Assessments.py`** — l2Assessments reads both
  `kcm_dictionary` and `kcm_content_mapping`.
- **`userEnrolment.py` → `userActivity.py`, `dsrComputation.py`,
  `nationalLearningWeek.py`, `odcsRecommendation.py`, `ministryMetrics.py`,
  `dashboardSync.py`** — `user_enrolments` is one of the most widely-read
  warehouse tables in the whole pipeline.
- **`gamificationJob.py` → `gamificationNotificationProducer.py`,
  `userReport.py`** — both read the badge-enrolment Parquet gamificationJob
  writes.
- **`peerValidationEligibleUsers.py` → `peerValidationNotificationSender.py`**
  — a Postgres-table outbox/queue pattern, not Kafka despite the class
  names.
- **`gamificationNotificationProducer.py` → `gamificationNotificationConsumer.py`**
  — the same outbox pattern.
- **`userReport.py` + `userEnrolment.py` → `zipUpload.py`** — zipUpload is
  the fan-in consumer of report jobs' CSV output folders.
- **`weeklyClaps.py` → `dataExhaust.py` (next run)** — a closed
  read-modify-write loop: this job updates Postgres `learner_stats`, and
  Stage 0 reads that same table fresh at the start of the *next* pipeline
  run.

## Technology stack

| Layer | Technology |
|---|---|
| Compute | Apache Spark (PySpark), local mode (`local[*]`/`local[N]`) |
| Heavy SQL / high-cardinality joins | DuckDB (ACBP/CAP allocation engines, `dashboardSync.py`'s query library, warehouse export scripts, most CSV writers) |
| Sources | Cassandra, Elasticsearch, Redis, MongoDB, Druid, Postgres (two logical databases: app `sunbird` schema and warehouse `warehouse` schema) |
| Sinks | Postgres (warehouse schema), BigQuery, Redis, Cassandra, Kafka, GCS |
| Intermediate storage | Parquet (Snappy-compressed), on local disk |
| Report delivery | CSV, zipped and password-protected per organization, uploaded to GCS |
| CI/CD | Jenkins (build: zip + archive artifact; deploy: download artifact + `ansible-playbook`) |
| Config management | Ansible (templates `jobs/config.py` from a private inventory at deploy time) |
| Scheduling | **Not present in this repository.** `gamification/prerequisites.md` and a `unifiedParquetPath: 'airflowData/'` config constant both point to an external Airflow instance. No cron/systemd file exists anywhere in the repo. |

See [`pipeline-overview.html`](pipeline-overview.html) for the full,
end-to-end deployment timeline (commit → Jenkins build → Jenkins deploy →
Ansible → external scheduler → Stage 0/1/2 → warehouse/BigQuery sync →
zip/upload → cleanup), and [`JOB_DETAILS.md`](JOB_DETAILS.md) for the
per-job reference this document summarizes.
