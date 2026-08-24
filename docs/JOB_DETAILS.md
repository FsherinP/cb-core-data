# cb-core-data — Job Details

Exhaustive per-job reference for every job in the pipeline, as it exists on
branch **`cbrelease-4.8.39.3`** (as of 2026-08-24). This started as an
analysis of `cbrelease-4.8.39.1-hotfix` and has been updated to reflect 9
files changed and 3 new jobs added since then — each noted inline below as
"NEW on this branch" / "CHANGED on this branch" / "FIXED on this branch."
For the big picture first, read [`ARCHITECTURE.md`](ARCHITECTURE.md); for an
interactive, searchable version of this same content, open
[`pipeline-overview.html`](pipeline-overview.html).

**A note on profiling:** a performance-profiling/phase-clocking
instrumentation layer exists in this codebase's history, but only on a
separate `cbrelease-4.8.39.1-profiling` branch that was never merged into
this lineage (`hotfix` → `newhotfix` → `4.8.39.2` → `4.8.39.3`). It is not
present anywhere in `cbrelease-4.8.39.3`, so nothing needed to be filtered
out of this document.

**Read-type key:** `Cassandra` · `Elasticsearch` · `Postgres(app)` = app DB,
schema `sunbird` · `Postgres(warehouse)` = warehouse DB, schema `warehouse` ·
`Redis` · `MongoDB` · `Druid` · `REST API` · `Parquet(raw)` = Stage-0 cache
under `data-res/pq_files/cache_pq/` · `Parquet(computed)` = Stage-1 output
under `output/computed/` · `Parquet(warehouse)` = local mirror of the
warehouse DB under `warehouseReportDir`.

---

## Table of contents

- [Stage 0 — Extract](#stage-0--extract)
- [Stage 1 — Prejoin](#stage-1--prejoin)
  - [Shared `dfutil` library modules](#shared-dfutil-library-modules)
- [Stage 2 — Warehouse & Dashboard Sync](#stage-2--warehouse--dashboard-sync)
- [Stage 2 — Compliance & Enrolment Reports](#stage-2--compliance--enrolment-reports)
- [Stage 2 — Gamification & Scoring](#stage-2--gamification--scoring)
- [Stage 2 — Leaderboards & Campaigns](#stage-2--leaderboards--campaigns)
- [Stage 2 — Surveys & Export](#stage-2--surveys--export)
- [Known issues found during this read](#known-issues-found-during-this-read)

---

## Stage 0 — Extract

### Data Exhaust — `jobs/stage-0/dataExhaust.py`

**Business purpose:** The single entry point for every raw external system.
Reads Cassandra, Elasticsearch and Postgres directly and writes each result
straight to Parquet — nothing downstream ever opens a live connection to a
source system again.
**How it's run:** `spark-submit jobs/stage-0/dataExhaust.py`, no CLI args.

**Reads (~29 total):**

| # | Type | Source | Notes |
|---|---|---|---|
| 1 | Cassandra | `cassandraUserEnrolmentsTable` (course keyspace) | → `enrolment` cache |
| 2 | Cassandra | `cassandraCourseBatchTable` (course keyspace) | → `batch` cache |
| 3 | Cassandra | `cassandraFrameworkHierarchyTable`, filtered `identifier='kcmfinal_fw'` | → `kcmV6` cache |
| 4 | Cassandra | `cassandraQuestionSetHierarchyTable`, JSON-parsed | → `questionsetHierarchy` cache |
| 5 | Cassandra | `cassandraUserAssessmentTable` | → `userAssessmentRaw` cache |
| 6 | Cassandra | `cassandraContentHierarchyTable` | → `hierarchy` cache |
| 7 | Cassandra | `cassandraRatingSummaryTable` | → `ratingSummary` cache |
| 8 | Cassandra | `cassandraAcbpTable` | → `acbp` cache |
| 9 | Cassandra | `cassandraRatingsTable` | → `rating` cache |
| 10 | Cassandra | `cassandraUserRolesTable` | → `role` cache |
| 11 | Elasticsearch | `compositesearch` — 7 primary categories (Course, Program, Blended Program, …) | → `esContent` cache |
| 12 | Cassandra | `cassandraOrgTable` | → `org` cache, and joined into `orgHierarchy` |
| 13 | Postgres(app) | `appOrgHierarchyTable` (`org_hierarchy_v4`) | → `orgCompleteHierarchy` cache (raw) + joined into `orgHierarchy` |
| 14 | Postgres(app) | `dwLearnerStatsTable` (`learner_stats`) | → `weeklyClaps` cache |
| 15 | Postgres(app) | `cios_content_entity` | → `externalContent` cache |
| 16 | Cassandra | `sunbird_courses.user_external_enrolments` | → `externalCourseEnrolments` cache |
| 17 | Cassandra | `sunbird_courses.enrollment_history_by_action` | → `unenrolledUserAudit` cache |
| 18 | Cassandra | `cassandraOldAssesmentTable`, **safe-columns-only read** (avoids a known timestamp-overflow bug) | → `oldAssessmentDetails` cache |
| 19 | Cassandra | `cassandraUserTable` | → `user` cache |
| 20 | Cassandra | `cassandraLearnerLeaderBoardTable` | → `learnerLeaderBoard` cache |
| 21 | Cassandra | `cassandraKarmaPointsTable` | → `userKarmaPoints` cache |
| 22 | Cassandra | `cassandraKarmaPointsSummaryTable` | → `userKarmaPointsSummary` cache |
| 23 | Elasticsearch | `compositesearch`, `objectType='Event'` | → `eventDetails` cache |
| 24 | Cassandra | `user_entity_enrolments` (course keyspace), joined with #23 | → `eventEnrolmentDetails` cache |
| 25 | Cassandra | `cassandraUserExtendedProfileTable` | → `userExtendedProfile` cache |
| 26 | Elasticsearch | `compositesearch`, Course Assessment + Final Program Assessment | → `esFinalAssessment` cache |
| 27 | Elasticsearch | `compositesearch`, Course Assessment | → `esCourseAssessment` cache (ad hoc path, not a named constant) |
| 28 | Cassandra | `sunbird_courses.access_setting_rules_v2` | → `accessControlSettings` cache (ad hoc path; feeds `capAllotment.py`) |
| 29 | Elasticsearch | `fs-forms-data-alias-v2` (IGOT ES host) | → `courseCompletionSurvey` cache |

**Joins (4 total):**

| # | Left | Right | Type | Key | Why |
|---|---|---|---|---|---|
| 1 | Cassandra `org` | Postgres `org_hierarchy_v4` | LEFT | `sborgid` | attach ministry/dept hierarchy to the cassandra org record |
| 2 | (1) | ministry lookup (self) | LEFT | `l1mapid` | resolve ministry name |
| 3 | (2) | department lookup (self) | LEFT | `l2mapid` | resolve department name |
| 4 | Event enrolments | Event details | LEFT | `event_id` | attach event duration |

**Processing:** Flattens nested ES JSON (speakers, recorded links, badge
metadata); formats durations as `HH:MM:SS`; the org-hierarchy join runs
three times (self, ministry, department) to build a flat mdo→ministry→dept
row. No Mongo or Druid reads happen here — those are specific to certain
Stage 2 jobs (Mongo: `surveyQuestionReport.py`/`surveyStatusReport.py`;
Druid: `dsrComputation(Updated).py`, `ministryMetrics.py`, `npsUpgraded.py`,
`surveyQuestionReport.py`, `surveyStatusReport.py`, `weeklyClaps.py`,
`dashboardSync.py`).

**Writes:** 27 raw Parquet caches under `data-res/pq_files/cache_pq/` — one
folder per row above. No CSV, no warehouse, no Redis, no Kafka.

**Downstream:** Every Stage 1 step and most Stage 2 jobs read from these
caches directly or via Stage 1's computed tables.

---

## Stage 1 — Prejoin

### Prejoin & Compute — `jobs/stage-1/prejoinData.py`

**Business purpose:** Orchestrates 18 join/derive steps (see table below),
turning Stage 0's per-table raw caches into the small set of denormalized
"computed" tables that nearly every Stage 2 job reads from.
**How it's run:** `spark-submit jobs/stage-1/prejoinData.py`, no CLI args;
runs each step via a `run_stage()` wrapper that times and logs it, raising
on the first failure.

**The 18 steps:**

| # | Step | Function | Needs config? |
|---|---|---|---|
| 1 | Clean/derive assessment pass-fail | `assessmentDFUtil.parse_raw_assessment_data` | Yes |
| 2 | Org + hierarchy | `userDFUtil.preComputeOrgWithHierarchy` | No |
| 3 | Content ratings & summary | `contentDFUtil.preComputeRatingAndSummaryDataFrame` | No |
| 4 | Course/program catalog (ES) | `contentDFUtil.preComputeAllCourseProgramESDataFrame` | No |
| 5 | Content master table | `contentDFUtil.preComputeContentDataFrame` | No |
| 6 | Content hierarchy | `contentDFUtil.precomputeContentHierarchyDataFrame` | No |
| 7 | Assessment ES frame | `assessmentDFUtil.precomputeAssessmentEsDataframe` | No |
| 8 | External/marketplace content | `contentDFUtil.preComputeExternalContentDataFrame` | No |
| 9 | User profile master | `userDFUtil.preComputeUser` | No |
| 10 | Enrolment master table | `enrolmentDFUtil.preComputeEnrolment` | No |
| 11 | External enrolment | `enrolmentDFUtil.preComputeExternalEnrolment` | No |
| 12 | User × org master | `userDFUtil.preComputeOrgHierarchyWithUser` | No |
| 13 | Enrolment-warehouse frame | `enrolmentDFUtil.preComputeUserEnrolmentWarehouseData` | No |
| 14 | User-warehouse frame | `userDFUtil.preComputeUserWarehouseData` | No |
| 15 | Content-warehouse frame | `contentDFUtil.preComputeContentWarehouseData` | No |
| 16 | Direct warehouse Parquet writes | `contentDFUtil.writeWarehouseParquetFiles` | Yes |
| 17 | Legacy assessment data | `assessmentDFUtil.precomputeOldAssessmentDataframe` | No |
| 18 | ACBP allocation engine | `acbpDFUtil_v3.preComputeACBPData` | No |

**Downstream:** `USER_ORG_COMPUTED_FILE`, `CONTENT_COMPUTED_PARQUET_FILE` and
`ENROLMENT_COMPUTED_PARQUET_FILE`/`ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE`
are each read by 15–20 of the 40 Stage 2 jobs.

### Shared `dfutil` library modules

These are not independently-run jobs — they're the library code Stage 1
calls, and several are also called directly by Stage 2 report jobs. Listed
here because "how many joins" and "what does it read/write" apply to them
just as much as to a standalone job.

#### `dfutil/user/userDFUtil.py` — 8 joins

**Used by:** Stage 1 (steps 2, 9, 12, 14); `preComputeUserWarehouseData` is
also called indirectly via `userReport.py`.

| # | Left | Right | Type | Key | Why |
|---|---|---|---|---|---|
| 1 | user | role (grouped, concatenated) | LEFT | `userID` | attach role list |
| 2 | (1) | karma points (summed) | LEFT | `userID` | attach total karma points |
| 3 | (2) | weekly claps | LEFT | `userID` | attach clap count |
| 4 | org | org hierarchy | LEFT | `orgID`=`userOrgID` | attach ministry/dept names |
| 5 | user (computed) | org (computed) | INNER (default) | `userOrgID`=`usermergedOrgID` | produces `USER_ORG_COMPUTED_FILE` — the master user×org table |
| 6 | user enrolment | content duration | LEFT | `content_id` | for learning-hours calc |
| 7 | user master | enrolment aggregates | LEFT | `userID` | attach content-consumption counts |
| 8 | user enrolment | event details | INNER (default) | `userID` | for event-hours calc |

**Reads:** `USER_PARQUET_FILE`, `ROLE_PARQUET_FILE`, `USER_KARMA_POINTS_PARQUET_FILE`,
`CLAPS_PARQUET_FILE`, `ORG_PARQUET_FILE`, `ORG_HIERARCHY_PARQUET_FILE`,
`ORG_COMPUTED_PARQUET_FILE`, `USER_COMPUTED_PARQUET_FILE`,
`ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE`, `CONTENT_COMPUTED_PARQUET_FILE`,
`EVENT_ENROLMENT_PARQUET_FILE`.
**Writes:** `USER_SELECT`, `USER_COMPUTED`, `ORG_SELECT`, `ORG_HIERARCHY_SELECT`,
`ORG_COMPUTED`, `USER_ORG_COMPUTED`, `USER_WAREHOUSE_COMPUTED`.

#### `dfutil/content/contentDFUtil.py` — 11 joins

**Used by:** Stage 1 (steps 3–6, 8, 15, 16); `add_hierarchy_column`/
`add_course_org_details` helpers are reused directly by several Stage 2
report jobs.

| # | Left | Right | Type | Key | Why |
|---|---|---|---|---|---|
| 1 | content | content rating | LEFT | `courseID` | attach avg rating |
| 2 | (1) | org | LEFT | `courseOrgID` | produces `CONTENT_COMPUTED_PARQUET_FILE` — master content table |
| 3 | enrolment | resource count | LEFT | `courseID` | for progress % |
| 4 | content | completion aggregates | LEFT | `courseID` | enrolled/in-progress/completed counts |
| 5 | (4), Blended Program subset | batch data | INNER, then broadcast LEFT into main | `courseID` | attach batch name/dates only for Blended Programs |
| 6 | marketplace content | marketplace enrolment aggregates | OUTER | `content_id` | combine even where one side has no match |
| 7 | (5)+(6) union | SCORM-detection flags | LEFT | `courseID` | flag html-archive content |
| 8 | event enrolments | karma points | LEFT | `user_id`,`event_id` | attach karma to event attendance |

*(rows 9–11 are the `addHierarchyColumn`/`addCourseOrgDetails` helper joins,
reused by `allCourseProgramDetailsWithCompetenciesJsonDataFrame` — same join
mechanics as rows 1–2 above, applied by calling code.)*

**NEW on this branch:** `precomputeContentHierarchyDataFrame` (step 6) now
does more than write `CONTENT_HIERARCHY_SELECT`. It also:
- Infers the ES hierarchy JSON's schema dynamically by sampling 30% of rows
  (`infer_hierarchy_schema`) rather than relying on a fixed schema.
- Flattens the hierarchy up to 4 child levels deep — `parent_*` fields plus
  `first_level_child_*` through `fourth_level_child_*` for a fixed list of
  22 fields (duration, mimeType, name, competencies_v3, etc.) — into one
  wide row per `root_content_id`, walking each level's `children` array
  with `explode_outer` and back-filling `NULL`s where a level has no
  children.
- Writes the result to the new `CONTENT_HIERARCHY_FLATTENED_PARQUET_FILE`,
  which `userReport.py` now depends on for its learning-hours calculation
  (see that section below).
- Minor code-quality note: the new helper functions (`infer_hierarchy_schema`,
  `safe_field`, `get_struct_type`, `has_children`) are decorated
  `@staticmethod` at module level, outside any class — a no-op decorator in
  that position, harmless but likely left over from a class-based draft.

**Writes:** `ALL_COURSE_PROGRAM_COMPUTED`, `RATING_SUMMARY_COMPUTED`,
`RATING_COMPUTED`, `CONTENT_RATING_COMPUTED`, `EXTERNAL_CONTENT_COMPUTED`,
`CONTENT_HIERARCHY_SELECT`, `CONTENT_COMPUTED`, `CONTENT_WAREHOUSE_COMPUTED`,
`CONTENT_HIERARCHY_FLATTENED` (new); plus **direct** writes (bypassing the
`output/computed/` convention) to `warehouse/org_hierarchy`,
`warehouse/event_details`, `warehouse/event_enrolment_details`.

> **Note:** `preComputeContentWarehouseData` (Stage 1) and `courseReport.py`
> (Stage 2) are **two independent producers of the warehouse `content`
> table**. `courseReport.py`'s write is the one `capAllotment.py` and the
> sync jobs actually depend on — see [Known Issues](#known-issues-found-during-this-read).

#### `dfutil/enrolment/enrolmentDFUtil.py` — 6 joins

**Used by:** Stage 1 (steps 10, 11, 13); `preComputeUserOrgEnrolment` is
also called directly by `courseReport.py` and `unenrollmentReport.py`.

| # | Left | Right | Type | Key | Why |
|---|---|---|---|---|---|
| 1 | enrolment | batch | LEFT | `courseID`,`batchID` | attach batch name/dates |
| 2 | (1) | user rating | LEFT | `userID`,`courseID` | attach rating — produces `ENROLMENT_COMPUTED_PARQUET_FILE` |
| 3 | (2) | karma points | LEFT | `userID`,`courseID` | attach karma earned per enrolment |
| 4 | enrolment | content org (shared helper) | LEFT | `courseID` | attach content metadata |
| 5 | (4) | user org | LEFT | `userID` | attach user/org profile |
| 6 | external content | external enrolment | INNER | `content_id` | build marketplace enrolment frame, unioned into `ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE` |

**Writes:** `ENROLMENT_SELECT`, `BATCH_SELECT`, `ENROLMENT_COMPUTED`,
`EXTERNAL_ENROLMENT_COMPUTED`, `ENROLMENT_WAREHOUSE_COMPUTED`.

#### `dfutil/assessment/assessmentDFUtil.py` — 12 joins

**Used by:** Stage 1 (steps 1, 7, 17); most of its other functions
(`add_hierarchy_column`, `user_assessment_children_details_dataframe`,
`all_course_program_details_with_rating_df`, `add_course_org_details`) are
called directly by `assessmentReport.py`, `courseBasedAssessmentReport.py`
and `acbpReport.py`.

| # | Left | Right | Type | Key | Why |
|---|---|---|---|---|---|
| 1 | old assessment | course | LEFT | `courseID` | attach course metadata |
| 2 | (1) | user org | LEFT | `userID` | attach user profile |
| 3 | assessment | org | LEFT | `assessOrgID` | attach org name |
| 4 | any df | hierarchy | LEFT | `identifier` (shared `add_hierarchy_column` helper) | unpack section/question JSON |
| 5 | user attempt | assessment children | INNER | `assessChildID` | link raw attempts to assessment structure |
| 6 | (5) | assessment details | LEFT | `assessID` | attach parent metadata |
| 7 | (6) | course details | LEFT | `courseID` | attach course fields |
| 8 | (7) | user org | LEFT | `userID` | attach user profile |
| 9 | course | rating | LEFT | `courseID`,`categoryLower` | attach rating |
| 10 | course | org | LEFT | `courseOrgID` | attach org name |
| 11 | section-wise results | questionset hierarchy | INNER | `assessChildID`=`questionsetID` | apply section-level pass/fail cutoffs |
| 12 | raw attempts | section-derived results | **LEFT** (deliberately, not INNER) | `assessChildID`,`userID`,`assessStartTimestamp` | preserve every raw attempt even where section logic doesn't apply |

**Processing:** Decides pass/fail via one of two rules depending on
assessment shape — **section-level cutoff** (every section must pass) or
**overall-result cutoff** (score ≥ configured minimum). Writes its cleaned
output **back into the raw cache directory** (`userAssessment`), not
`output/computed/` — a deliberate "clean the cache in place" step.

**Writes:** `ALL_ASSESSMENT_COMPUTED`, `OLD_ASSESSMENT_COMPUTED`, and the
raw-cache overwrite of `userAssessment`.

#### `dfutil/enrolment/acbp/acbpDFUtil_v3.py` — the ACBP allocation engine

**Used by:** Stage 1 step 18 (`acbpDFUtil.py`, the non-`_v3` file, is
imported by `dfexportutil.py` but **never actually called** — dead code).

Runs in **DuckDB, not Spark**. For each of 9 criteria types (`rootorgid`,
`alluser`, `user`/`customuser`, `designation`, `cadre`, `group`, `batch`,
`service`, `isoncentraldeputation`) plus optional org-defined custom fields,
it runs one INNER match join against exploded plan criteria, plus (for all
but `rootorgid`/`alluser`) a LEFT join against a rootorgid-scoping lookup —
roughly 2 joins per criterion type. That whole block runs **twice**: once
for org-scoped plans, once for global (no-org) plans, via `UNION ALL`
sections. Net: **~40 SQL joins**, chunked 2 million users at a time for
memory safety. Semantics: a user must satisfy *every* criterion within one
plan's userGroup (AND), but satisfying *any one* userGroup on a plan is
enough (OR).

**Reads:** `ACBP_PARQUET_FILE` (raw), `userExtendedProfile` (raw),
`USER_ORG_COMPUTED_FILE`.
**Writes:** `ACBP_SELECT_FILE`, `ACBP_COMPUTED_FILE` (final user × plan
allocation list — read by `acbpReport.py`, `userEnrolment.py`,
`unenrollmentReport.py`, `l2Assessments.py`).

#### `dfutil/dfexport/dfexportutil.py` — 0 joins

**Used by:** ~15 Stage 2 report jobs, for CSV export.

The shared report-writing layer. `write_csv_per_mdo_id`/
`write_csv_per_mdo_id_duckdb` implement a hybrid strategy: orgs with ≤100,000
rows are written directly via Spark `partitionBy` + CSV; larger orgs are
routed through a temp partitioned-Parquet → DuckDB → CSV conversion,
parallelized across workers. `write_single_csv_duckdb` produces one combined
CSV (for a central/admin view); `write_csv_combined` does both at once. This
is the mechanism behind the `<output_dir>/mdoid=<org_id>/<report>.csv`
folder convention used by nearly every report job below.

#### `dfutil/utils/{utils.py, redis.py, QueryExecutor.py}`

Low-level helpers, not independently run: `utils.py` wraps Druid REST calls
(`druidDFOption`), generic HTTP, GCS sync/zip (`sync_reports`,
`zip_and_sync_reports`), Elasticsearch reads (`read_elasticsearch_data[_scroll]`,
a manual REST implementation, not always the Spark ES connector), Cassandra
writes and Kafka dispatch. `redis.py` wraps the Python `redis` client with
single-key get/update, hash-map get/update/dispatch, DataFrame→Redis-hash
dispatch, and batched pipelined bulk updates — supporting both the main
Redis instance and a separate "karma points" Redis instance
(`redisKpHost`). `QueryExecutor.py` is a generic ad hoc "run N queries, save
to CSV/JSON" helper, not part of the main call graph.

---

## Stage 2 — Warehouse & Dashboard Sync

### Data Warehouse Sync (Postgres) — `jobs/stage-2/dataWarehouse.py`

**Business purpose:** Reads the local Parquet snapshots that other report
jobs stage under `warehouseReportDir` and JDBC-writes each straight into the
Postgres **warehouse** schema.
**How it's run:** standalone, no CLI args.

**Reads (12):** `warehouse/{user_detail, content, assessment_detail,
bp_enrolments, content_resource, cb_plan, org_hierarchy, kcm_content_mapping,
kcm_dictionary, event_details, event_enrolment_details, user_enrolments}`.

**Joins:** 0 — no dataframe joins at all.

**Writes:** 12 Postgres tables, `mode="overwrite"` — a full truncate and
reload every run, not incremental. (`course_completion_survey_details`'s
push line exists in the source but is commented out.)

**Downstream:** `bq-scripts.sh` mirrors the same tables into BigQuery.

### Data Warehouse Sync (DuckDB) — `jobs/stage-2/dataWarehouseBash.sh`

**Business purpose:** A bash/DuckDB reimplementation of the exact same
responsibility as `dataWarehouse.py`, with explicit column casts and dedup
logic the Spark version lacks — more schema-safe. **Both exist in this
branch**; which one actually runs in production isn't visible from the repo.
**How it's run:** shell script; shells out to
`python3 -c "from jobs.config import get_environment_config"` to read
Postgres connection details.

**Reads:** the same `warehouse/<table>/*.snappy.parquet` folders, via
DuckDB `read_parquet()`.
**Joins:** 0 — straight `TRUNCATE` + `INSERT` per table.
**Processing:** lightweight → heavyweight execution order; `content` is
deduplicated via `ROW_NUMBER()` — **fixed on this branch** to add an
explicit `ORDER BY last_published_on DESC`, making the dedup deterministic
(previously unordered, so which of two duplicate rows survived was
arbitrary); `bp_enrolments`/`assessment_detail`/`user_enrolments` filter
out rows with NULL foreign keys before insert; `user_enrolments` (~19GB) is
deliberately loaded last. **New on this branch:** per-table and total
execution timing, logged to stdout with a summary table printed at the end
— a lightweight bash-level addition, unrelated to the separate
(never-merged) Python profiling branch.
**Writes:** the same 12 Postgres tables, via
`ATTACH ... TYPE postgres; TRUNCATE pg.<table>; INSERT INTO pg.<table> ...`.

### Dashboard Sync — `jobs/stage-2/dashboardSync.py`

**Business purpose:** The engine behind the live admin/learner dashboard.
Runs ~15 metric phases against warehouse-computed Parquet via DuckDB SQL
(the query library in `constants/QueryConstants.py`) and writes every
result to **Redis** — never Postgres.
**How it's run:** standalone, no CLI args; the module also defines a
`DashboardDuckDBExecutor` helper class for running arbitrary named queries.

**Reads:** `ENROLMENT_WAREHOUSE_COMPUTED`, `USER_WAREHOUSE_COMPUTED`,
`CONTENT_WAREHOUSE_COMPUTED`, `ORG_COMPUTED` and ~15 more warehouse Parquet
tables, all read inside DuckDB SQL text defined in `QueryConstants.py`.

**Joins:** 50+ SQL joins across ~40 named queries. A shared
`BASE_DATA_COMPLETE` CTE (`INNER JOIN` of enrolment ⋈ user ⋈ content,
`LEFT JOIN` org) is textually concatenated into 15 of those queries.

**Processing phases:** org/MDO-admin counts · enrolment/completion funnels
(overall, per-MDO, per-CBP) · top-5/top-10 leaderboards (users, courses,
content, MDOs, by completion/rating/certifications) · trending
courses/events · National Learning Week campaign metrics · competency
coverage · learning-hours deltas · NPS score.

**CHANGED on this branch (`process_trending`):** trending courses are now
restricted to enrolments from the last 7 days (`enrolled_on` between 7 and
1 days ago), where previously the ranking used all-time enrolment counts.
The entire **"Trending Programs"** calculation — the query, the
per-org breakdown, and the `lhp_trending`/`across:programs` Redis write —
is now commented out; the inline comment explains "the new UI/UX
implementation considers only context_type = Course," so program trending
is no longer computed at all on this branch.

**Writes:**
- **Redis:** 50+ keys (`dashboard_*` counters, top-N leaderboard maps,
  trending/certification lists, NLW ticker, competency coverage).
- **Kafka:** exactly one message per run, topic = `userCourseProgramProgress`
  (via `dpBrokerList`).

**Downstream:** every real-time counter/leaderboard on the dashboard UI.

### Postgres → Parquet Exporter — `jobs/stage-2/postgresToParquet.py`

**Purpose:** NEW on this branch. Runs the usual warehouse flow in reverse —
reads a Postgres table (default `org_hierarchy_new`, the table the
org-hierarchy jobs above just rebuilt) and writes it back out to a local
Parquet file. Reads like an ops/diagnostic export utility rather than a
report or sync job proper: an `argparse` CLI (`--table`, `--schema`,
`--output-dir`, `--filename`, `--chunk-size`, `--compression`), not a
`main()` with no arguments like every other job in this repo.

**Reads (1):** any Postgres app-schema table via CLI args, default
`org_hierarchy_new` / schema `public`.

**Joins:** 0.

**Processing:** Fully config-driven Postgres connection
(`config.dwPostgresHost`/`appPostgresSchema`/`appPostgresUsername`/
`appPostgresCredential`) — no hardcoded credentials. Supports chunked reads
for large tables via `pd.read_sql_query(..., chunksize=...)`. Logs table
shape, column list, dtypes, and null-counts-per-column before writing —
useful as a data-quality spot-check, not just an export.

**Writes:** `<output-dir>/<filename-or-table>.parquet`, Snappy-compressed by
default (configurable: gzip/brotli/lz4/zstd/none). Defaults to
`./output/org_hierarchy_new.parquet`.

**Downstream:** not referenced by any other job in this repo — appears to
be a standalone ops/diagnostic tool rather than a wired pipeline step.

---

## Stage 2 — Compliance & Enrolment Reports

### ACBP Compliance Report — `jobs/stage-2/acbpReport.py`

**Purpose:** Tracks mandatory ACBP course assignments vs. actual completion
per employee; SLA = completed before due date + 1-day grace. Also produces
an APAR export linking completions to competency areas for HR appraisal.

**Reads (9):** `USER_ORG_COMPUTED_FILE`, `CONTENT_HIERARCHY_SELECT`,
`ALL_COURSE_PROGRAM_COMPUTED` (filtered categories), `ORG_SELECT`,
`ENROLMENT_COMPUTED` (enrolled), `ACBP_COMPUTED_FILE`, `ACBP_SELECT_FILE`,
`warehouse/kcm_dictionary`, `warehouse/kcm_content_mapping`.

**Joins (7):** ACBP plans (exploded) ⋈ course details `LEFT courseID` ·
+ enrolment `LEFT (courseID,userID)` · ACBP-select ⋈ course details
`LEFT courseID` · KCM mapping ⋈ KCM dictionary `LEFT competency_area_id` (×2,
dedup pass) · APAR-flagged rows ⋈ user `additionalProperties`
`LEFT userID` · + competency-area result `LEFT courseID`.

**Processing:** Three-tier Ministry → Department → Organization fallback
display logic (reused by `blendedReport.py`, `courseBasedAssessmentReport.py`,
`unenrollmentReport.py`); `assignment_type_mapping` translates ACBP's
internal rule vocabulary (`rootorgid`, `cadre`, `batch`, …) into
warehouse-friendly names — the same vocabulary `capAllotment.py` reuses.

**Writes:** CSV `CBPEnrollmentReport.csv` + `CBPUserSummaryReport.csv`
(combined + per-MDO), under `acbpReportPath/{today}`. Warehouse: `cb_plan`
(synced); `cbp_enrollments`, `apar_cbp_enrollment` (Parquet only, not yet
wired into `dataWarehouse.py`). One dead `kafkaDispatch()` line, commented out.

**Downstream:** `cb_plan` feeds Postgres/BigQuery. `cbp-report-mdo-*`
folders bundled by `zipUpload.py`.

### Standalone Assessment Report — `jobs/stage-2/assessmentReport.py`

**Purpose:** For assessments not embedded in a course, reports each
learner's latest attempt, score, pass/fail and attempt count.

**Reads (7):** `ALL_ASSESSMENT_COMPUTED` (Standalone Assessment only),
`ORG_COMPUTED`, `CONTENT_COMPUTED`, `RATING_SUMMARY_COMPUTED`,
`USER_ORG_COMPUTED_FILE`, `HIERARCHY_PARQUET_FILE`, `USER_ASSESSMENT_PARQUET_FILE`.

**Joins (9):** assessment ⋈ hierarchy `LEFT assessID` · course ⋈ hierarchy
`LEFT courseID` · course ⋈ org `LEFT courseOrgID` · course ⋈ rating
`LEFT (courseID,categoryLower)` · attempt ⋈ assessment-children
`INNER assessChildID` · + assessment details `LEFT assessID` · + course
details `LEFT courseID` · + user-org `LEFT userID` · + latest-attempt
(broadcast) `INNER (assessChildID,userID,assessEndTimestamp)`.

**FIXED on this branch:** the `Percentage_Of_Score` output column now reads
from `assessOverallResult` instead of `assessPassPercentage` — those are two
different numbers (the score a learner actually achieved vs. the minimum
threshold needed to pass), so every report run before this fix showed the
pass threshold in a column labeled as the learner's score.

**Writes:** CSV `StandaloneAssessmentReport.csv` per MDO, under
`standaloneAssessmentReportPath`. No warehouse write. Two dead
`kafkaDispatch()` lines.

**Downstream:** `user-assessment-report-cbp` folder, bundled by
`zipUpload.py` for content providers.

### Bharat Kalp Report — `jobs/stage-2/bharatKalpReport.py`

**Purpose:** For the "Bharat Kalp" talks/podcast initiative, records which
eligible members enrolled in which Bharat-Kalp-tagged courses/events.
Warehouse-only, no CSV.

**Reads (5):** live CMS API (`bharatKalpCoursesApiUrl` — eligible courses +
event tags, refetched every run, not hardcoded), `warehouse/user_enrolments`,
`warehouse/event_details`, `warehouse/event_enrolment_details`,
`USER_COMPUTED` (filtered `isBharatKalpMember`).

**Joins (4):** event enrolments ⋈ eligible members (broadcast)
`INNER user_id` · + events (tag-filtered) `INNER event_id` · course
enrolments ⋈ eligible members (broadcast) `INNER user_id` · + BK course
list (broadcast, from API) `INNER content_id`.

**Processing:** Raises an error rather than silently producing an empty
report if the CMS API returns zero courses or tags.

**Writes:** warehouse `bk_course_enrolments`, `bk_event_enrolments` —
Parquet only, not yet wired into `dataWarehouse.py`.

### Blended Program Report — `jobs/stage-2/blendedReport.py`

**Purpose:** For programs mixing online modules with in-person sessions,
tracks progress and attendance at the individual-component level, including
offline-session attendance that page-view data can't capture.

**Reads (5):** `USER_ORG_COMPUTED_FILE`, `CONTENT_COMPUTED` (Blended
Program), `BATCH_SELECT`, `ENROLMENT_COMPUTED` (enrolled),
`CONTENT_HIERARCHY_SELECT`.

**Joins (12):** batch attach `LEFT` · coordinator-name attach `LEFT` ·
program×batch ⋈ enrolments `INNER` (scoping) · user-org ⋈ completion
`RIGHT` · program ⋈ hierarchy (broadcast) `LEFT` (component walk) ·
completion ⋈ children `LEFT` · + session detail `LEFT` · child-batch
resolution `LEFT` (×2) · + content-status map `LEFT`.

**Processing:** Flattens arbitrarily nested `Program → Course Unit →
Learning Resource` structures into one component list. Offline-session
attendance is derived from the physical session's start date, not a digital
access timestamp. Splits into two exports from the same rows: `-mdo`
(learner's own org, unmasked PII) and `-cbp` (provider org, masked PII).

**Writes:** CSV `BlendedProgramReport.csv` ×2 families. Warehouse:
`bp_enrolments` (synced).

**Downstream:** `blended-program-report-mdo`/`-cbp` folders, bundled
separately by `zipUpload.py`.

### CAP Access-Control Resolver — `jobs/stage-2/capAllotment.py`

**Purpose:** Resolves admin-configured Comprehensive Assessment Program
access-control rules (org + designation, OR cadre-batch tag, etc.) against
the real user base — exactly who can see which CAP. Warehouse-only.

**Reads (3):** `warehouse/content` (**hard dependency: requires
`courseReport.py` to have run first**), raw `accessControlSettings`,
`warehouse/user_detail` (read directly via DuckDB, filtered `status=1`).

**Joins:** ~11-way SQL fan-out in DuckDB, one criteria-type block per
(`rootorgid`, `user`/`customuser`/`alluser`, `designation`, `group`, `tag`,
`cadre`, `civil_service_type`, `service`, `batch`,
`isoncentraldeputation`, `isprofileverified`, `profilestatus`) — each an
INNER match join + LEFT org-scoping guard, the same engine pattern as
`acbpDFUtil_v3.py`.

**Processing:** A regex pre-pass wraps bare scalar `criteriaValue` JSON
fields into arrays before parsing — defends against an inconsistent
upstream shape.

**Writes:** warehouse `cap_allocation_meta`, `cap_allocation_user_wise` —
Parquet only, not yet wired into `dataWarehouse.py` (the newest/most
in-progress feature in this group — no consumer wired anywhere in the repo
yet).

### Course-Based Assessment Report — `jobs/stage-2/courseBasedAssessmentReport.py`

**Purpose:** For assessments embedded inside a course, reports each
learner's best attempt, score, pass/fail and retakes — Government vs.
Non-Government exports.

**Reads (10):** `ALL_ASSESSMENT_COMPUTED` (4 categories), `HIERARCHY`,
`ORG_COMPUTED`, `USER_ASSESSMENT_PARQUET_FILE` (SUBMITTED only),
`CONTENT_COMPUTED` (5 categories), `RATING_SUMMARY_COMPUTED`,
`USER_ORG_COMPUTED_FILE`, `OLD_ASSESSMENT_COMPUTED` (legacy),
`FINAL_ASSESSMENT_PARQUET_FILE`, ad hoc `esCourseAssessment`.

**Joins (12):** hierarchy ×2 `LEFT` · org `LEFT` · rating `LEFT` ·
best-attempt selection `INNER assessChildID` (two different tie-break
windows depending on whether the assessment is sectional) · + details `LEFT`
(×2) · + user-org `LEFT` · + retakes count `LEFT` · ES-final-assessment ⋈
best-attempt `INNER` · + min-pass-% override `LEFT` · + CAP sub-type
detection `LEFT`.

**Processing:** Best-attempt tie-break: basic assessments rank by overall
score; sectional assessments rank by section marks instead. Exact-match
VOLUNTEER/Non-Govt split — a designation or role element must equal exactly
`"VOLUNTEER"`, deliberately not a substring match (avoids false-flagging
"Civil Defence Volunteer"). Unions new-schema and legacy pre-migration
assessment data.

**Writes:** CSV `UserAssessmentReport.csv`, Govt and Non-Govt written
separately (each guarded against an empty-partition `KeyError` in the
shared export helper). Warehouse: `assessment_detail` (synced).

**Downstream:** `cba-report` folder bundled by `zipUpload.py`.
`assessment_detail` feeds Postgres/BigQuery.

### Course Completion Survey Report — `jobs/stage-2/courseCompletionSurveyReport.py`

**Purpose:** Turns raw end-of-course survey submissions (design/content/
delivery/relevance ratings) into a readable one-row-per-submission report,
plus a slim structured warehouse table.

**Reads (1):** `COURSE_COMPLETION_SURVEY_PARQUET_FILE`, filtered to
configured `formIds`.

**Joins:** 0 — single-source explode + pivot only.

**Processing:** `explode(responses)` then `groupBy(...).pivot("question")`
turns long/EAV-shaped survey data back into a wide, one-row-per-submission
report.

**Writes:** CSV `completionSurvey.csv`, grouped by `formId` — the only job
in the repo partitioned by form rather than by org. Warehouse:
`course_completion_survey_details` written as Parquet, but its
`dataWarehouse.py` push line is **commented out** — not currently synced.

**Downstream:** `course-completion-survey-report` folder bundled by
`zipUpload.py`.

### Course / Content Report — `jobs/stage-2/courseReport.py`

**Purpose:** The master content-inventory report — metadata plus
enrolment/completion/certificate counts for every course, program and
marketplace item.

**Reads (8):** `CONTENT_COMPUTED` (14 categories), `CONTENT_HIERARCHY_SELECT`,
`ENROLMENT_COMPUTED` (enrolled), `BATCH_SELECT`, `EXTERNAL_ENROLMENT_COMPUTED`,
`EXTERNAL_CONTENT_COMPUTED`, `ORG_SELECT`, `FINAL_ASSESSMENT_PARQUET_FILE`.

**Joins (8):** hierarchy ⋈ content `INNER` (scoping) · enrolments ⋈
resource-count `LEFT` · content ⋈ completion-aggregates `LEFT` ·
Blended-Program-batch `INNER` then broadcast `LEFT` · marketplace content ⋈
enrolment-agg `OUTER` · + SCORM flags `LEFT` · ES-assessment ⋈ org `LEFT`.

**Processing:** Resource extraction walks the raw content-hierarchy JSON
with category-specific logic — Programs are flat, Courses can carry an
extra "Course Unit" wrapper layer. SCORM detection flags content whose
hierarchy contains an `html-archive` mimeType child.

**Writes:** CSV `ContentReport.csv` per MDO. Warehouse: `content_resource`,
`content` (**both synced** — and this `content` write is what
`capAllotment.py` depends on; see the Known Issues note about the second,
independent producer of this same table in Stage 1).

**Downstream:** `capAllotment.py` hard-depends on this job. `course-report`
folder bundled by `zipUpload.py`.

---

## Stage 2 — Gamification & Scoring

### Daily Summary Report (live) — `jobs/stage-2/dsrComputation.py`

**Purpose:** Computes daily platform-wide KPIs (registrations, enrolments,
completions, MAU/DAU, department onboarding) for the admin dashboard,
explicitly excluding Volunteer/Non-Government users from every metric. Git
history (15+ commits through mid-2026) confirms this is the maintained job
— `dsrComputationUpdated.py` below is an abandoned prototype.

**Reads (9):** `USER_PARQUET_FILE`, `EVENT_ENROLMENT`,
`ENROLMENT_WAREHOUSE_COMPUTED`, `EXTERNAL_ENROLMENT_COMPUTED`,
`CONTENT_WAREHOUSE_COMPUTED` (×2 reads), Elasticsearch `org_v4/_count`
(direct REST, not the Spark ES connector), Druid (30-day MAU, yesterday
logins), Postgres `dsr_metrics_history` (read-before-write).

**Joins (7):** user ⋈ VOLUNTEER-flag `LEFT user_id` · events ⋈ govt-only
users `INNER` · enrolments ⋈ active users `INNER` · + content `LEFT` · +
user-org `INNER` · Druid MAU ⋈ volunteers `LEFT_ANTI` · Druid DAU ⋈
volunteers `LEFT_ANTI`.

**Processing:** Indian-numbering formatter (comma every 2 digits, e.g.
`12,34,567`); hardcoded `CENTRAL_MINISTRIES_COUNT=94`, `STATE_UT_COUNT=36`.

**Writes:** Redis ~15 `dashboard_*_updated_format` keys. Warehouse:
`dsr_metrics_history` — Postgres, full overwrite of history + today's row,
**not** in `bq-scripts.sh`.

### Daily Summary Report (deprecated) — `jobs/stage-2/dsrComputationUpdated.py`

**Purpose:** An abandoned rewrite computing a smaller KPI subset from
different upstream tables — no Volunteer filtering, no Postgres history, no
MAU/DAU.

**Reads (6):** `USER_PARQUET_FILE` (active), `ENROLMENT_SELECT` (a
different source than the live job), `EXTERNAL_COURSE_ENROLMENTS`,
`ESCONTENT_PARQUET_FILE` (raw), a live external portal search API
(**hardcoded bearer token in source**), Redis (`external_course_count_current`,
a rough yesterday-count proxy).

**Joins (4, two pairs duplicated):** content ⋈ enrolment `LEFT`, ⋈ active
users `INNER` — computed twice, once for enrolments and once for
completions.

**Writes:** Redis keys with typos preserved from source
(`yerterday_course_completion`, `sers_registered_yersterday`) — no overlap
with the live job's key set.

### Gamification Dashboard — `jobs/stage-2/gamification/dashboard.py`

**This file is empty (0 lines) in this branch.** No logic to describe.

### Gamification Badge KPIs — `jobs/stage-2/gamificationJob.py`

**Purpose:** Aggregates course-badge metadata with enrolment/completion
data into badge-program KPIs (created, live, awarded, active learners,
earning rate) with month-over-month trend, plus a per-MDO CSV.

**Reads (5):** `ENROLMENT_COMPUTED` (enrolled), `EXTERNAL_ENROLMENT_COMPUTED`,
`EXTERNAL_CONTENT_COMPUTED`, `ALL_COURSE_PROGRAM_COMPUTED`,
`USER_ORG_COMPUTED_FILE`.

**Joins (4):** UNION platform ⋈ external enrolment · UNION internal ⋈
external badge metadata · broadcast `LEFT` enrolment ⋈ content-badge
metadata `content_id` (primary enrichment) · `INNER` reporting ⋈
user-master `userID`.

**Processing:** `SparkSession` is built at **module import time**, not
inside `main()` — importing this file anywhere eagerly launches a
90g-executor Spark session. Imports `broadcast()` only from
`duckdb.experimental.spark.sql.functions`, never native PySpark. Badge
leaderboard via `dense_rank` on award count.

**Writes:** Parquet `GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE` (a
hard dependency for `gamificationNotificationProducer.py` and
`userReport.py`); CSV `GamificationReport.csv` per MDO; Redis — 7 keys
matching `gamification/prerequisites.md`'s spec exactly (badge counts,
earning rate, performance rate, content completion rate).

### Gamification Notification Consumer — `jobs/stage-2/gamificationNotificationConsumer.py`

**Purpose:** Delivers pending badge-reminder notifications by claiming a
batch from a Postgres queue table and POSTing each to the Notification
microservice. **Despite the name, this is a Postgres outbox worker, not a
Kafka consumer** — no Kafka client exists in this file.

**Reads (1):** Postgres `config.dwnotificationQueue` —
`SELECT ... FOR UPDATE SKIP LOCKED` batch-claim pattern. Pure Python, no
Spark.

**Joins:** 0.

**Writes:** Updates `status`/`error_message` on the same queue table (no
new rows); HTTP POST to the Notification microservice.

> **Bug:** `config.dwnotificationQueue` and `gamificationNotificationBatchSize`
> are not defined anywhere in `config.py`, `default_config.py`, or the
> Ansible template — would raise `AttributeError` unless supplied externally.

### Gamification Notification Producer — `jobs/stage-2/gamificationNotificationProducer.py`

**Purpose:** Finds learners in-progress on a badge course whose earning
deadline is exactly N days away, and inserts one reminder row per eligible
user+course into the same Postgres queue the Consumer drains.

**Reads (2):** `GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE` (in-progress
+ deadline set), the Postgres queue (for dedup).

**Joins (1):** `LEFT_ANTI` eligible users ⋈ existing queue rows on
`notification_id`.

**Processing:** `notification_id = md5(userID + content_id)` — deterministic
idempotency key. Fires on an exact-day match, not a rolling window.

**Writes:** `INSERT` into the Postgres notification queue (append).

### In-App Review Nudge — `jobs/stage-2/inappReview.py`

**Purpose:** Generates an in-app "rate the app" nudge for every user whose
weekly-claps record updated today.

**Reads (1):** `CLAPS_PARQUET_FILE`. **Joins:** 0.

**Writes:** Cassandra `sunbird_notifications.notification_feed` (append),
expiring at end of the current ISO week.

### Karma Points Ledger — `jobs/stage-2/karmaPoints.py`

**Purpose:** Computes last month's karma-point score for rating a course
(+2) and completing a course (+10 with a graded assessment, +5 without,
capped at each user's first 4 completions/month), then updates a running
per-user total.

**Reads (5):** `RATING_PARQUET_FILE` (Cassandra TimeUUID decoded via a
custom UDF), `USER_ASSESSMENT_PARQUET_FILE`, `CONTENT_COMPUTED`,
`ENROLMENT_COMPUTED`, `USER_KARMA_POINTS_SUMMARY_PARQUET_FILE`
(self-referencing feedback loop with its own Cassandra output).

**Joins (4):** rating ⋈ course details `LEFT` · completion ⋈ course
details `INNER` · + has-assessment flag `LEFT` · new points `FULL OUTER`
existing running total on `userid`.

**Writes:** Cassandra ×3 — `user_karma_points` ledger, `_credit_lookup`,
`_summary` (cumulative) — **not** the Postgres warehouse.

### Knowledge & Competency Mapping (Stage 2C) — `jobs/stage-2/kcmReport.py`

**Purpose:** Links every course to its competency taxonomy (Area → Theme →
Sub-theme), reconciling course-level tags against the KCM-v6 dictionary.
Also imported by `jobs/main.py` as "Stage 2C."

**Reads (2):** `CONTENT_COMPUTED` (competency ref-id arrays — defensively
parsed as either real arrays or string-encoded arrays), `KCMV6_PARQUET_FILE`
(nested Area/Theme/SubTheme hierarchy JSON).

**Joins (5):** positional area/theme/subtheme realignment (`posexplode`) ·
content ⋈ competency-joined `LEFT` · KCM area `OUTER` KCM theme · subtheme ⋈
area-theme `LEFT` · final content ⋈ competency-details `INNER`.

**Writes:** warehouse `kcm_content_mapping`, `kcm_dictionary` — **both
synced to Postgres AND BigQuery**. CSV `ContentCompetencyMapping.csv`.

**Downstream:** `l2Assessments.py` hard-depends on both warehouse tables
this job writes.

### L2 / Comprehensive Assessment (APAR+CAP) — `jobs/stage-2/l2Assessments.py`

**Purpose:** A unified report combining APAR-mandated CBP course
consumption with Comprehensive Assessment Program exam attempts.

**Reads (10):** `warehouse/kcm_dictionary`, `kcm_content_mapping` (from
`kcmReport.py`), `warehouse/user_enrolments`, `ACBP_COMPUTED_FILE`,
`warehouse/content`, `warehouse/assessment_detail`, `warehouse/org_hierarchy`,
`warehouse/user_detail`, `warehouse/cb_plan`, ad hoc `esCourseAssessment`.

**Joins (12):** user×org `LEFT` · live-ES-min-pass-% override `LEFT` · KCM
reconciliation `INNER`(unused/dead)+`LEFT`(used) · APAR branch (5 joins:
plan→enrolment→content→user→cbplan→competency) · CAP branch (3 joins:
content→enrolment→assessment→user).

**Processing:** CAP dedup keeps the best attempt per (user, content,
assessment) — highest score first, tie-broken Pass over Fail.

**Writes:** Parquet to a **hardcoded absolute path**
(`/mount/data/analytics/igot-reports/assessment-report-apar/parquet`) — not
built from config, unlike every other job in the repo. A commented-out CSV
write references a developer's personal home directory. No warehouse write,
no CSV via the standard mechanism.

---

## Stage 2 — Leaderboards & Campaigns

### Learner Leaderboard — `jobs/stage-2/learnerLeaderboard.py`

**Purpose:** Monthly per-org "top learners" leaderboard ranked by karma
points earned last month, with rank movement vs. last month.

**Reads (3):** `USER_KARMA_POINTS_PARQUET_FILE` (previous month),
`USER_ORG_COMPUTED_FILE`, `LEARNER_LEADERBOARD_PARQUET_FILE` (prior
Cassandra snapshot).

**Joins (3):** orgs with `>10` users only `INNER` (privacy floor) · org
members ⋈ karma points `LEFT` · current ⋈ prior rank `LEFT userid`.

**NEW on this branch:** the final DataFrame gains a `job_execution_datetime`
column (`current_timestamp()`) before the Cassandra write — a run-audit
timestamp, not a behavior change.

**Writes:** Cassandra `learner_leaderboard` + `_lookup` (append). No
Postgres/Redis/Kafka.

### Ministry Leaderboard — `jobs/stage-2/ministryLeaderboard.py`

**Purpose:** Rolls up "top learners" to every level of the ministry/
department/org hierarchy simultaneously, using an embedded DuckDB database
for the hierarchy fan-out.

**Reads (3, loaded into DuckDB):** `USER_ORG_COMPUTED_FILE`,
`ORG_COMPLETE_HIERARCHY_PARQUET_FILE`, `USER_KARMA_POINTS_PARQUET_FILE`
(previous month, filtered in DuckDB).

**Joins (5, DuckDB SQL):** hierarchy × distinct-MDOs `INNER`, then a
3-level self-join chain rolling users up to MDO / department / ministry, ×
karma aggregate `INNER`.

**Processing:** One row per user **per hierarchy level** (MDO, dept,
ministry) via `UNION ALL`, so one `dense_rank()` produces independent
leaderboards at every level at once.

**Writes:** Postgres **app schema** `slw_mdo_top_learners`, JDBC
overwrite+truncate.

> **Bug:** the job's own log lines say "Writing to Cassandra" — it actually
> writes to Postgres. Stale comments, not a functional Cassandra path.

### Ministry Metrics — `jobs/stage-2/ministryMetrics.py`

**Purpose:** Refreshes four at-a-glance counters (24h active users, total
registered, certificates issued, enrolments) rolled up to ministry,
department and org level.

**Reads (4):** `ENROLMENT_WAREHOUSE_COMPUTED`, `USER_COMPUTED` (active),
`ORG_HIERARCHY_PARQUET_FILE` (raw), Druid (24h active users).

**Joins (11):** active-users × Druid `INNER` · × org-hierarchy `LEFT` ·
enrolment × users `INNER` × org-hierarchy `LEFT` · userCount computed 3×
(ministry/dept/org) × org-hierarchy `LEFT` (×3) · each metric ×
ministryNames `INNER` (×4, to resolve the Redis field key).

**Processing:** "One code path, three grain levels" — every metric computed
identically at ministry/department/org grain, then unioned.

**Writes:** Redis 4 hashes — `dashboard_rolled_up_login_percent_last_24_hrs`,
`_user_count`, `_certificates_generated_count`, `_enrolment_content_count`.

### National Learning Week Leaderboard — `jobs/stage-2/nationalLearningWeek.py`

**Purpose:** For a fixed campaign window, computes a per-capita
karma-points leaderboard bucketed by org size, separately for "state" and
"centre" MDOs, plus a live daily certificate/learning-hours ticker.

**Reads (8):** `warehouse/org_hierarchy`, `USER_ORG_COMPUTED_FILE`,
`ENROLMENT_WAREHOUSE_COMPUTED`, `CONTENT_WAREHOUSE_COMPUTED`,
`GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE`,
`USER_KARMA_POINTS_PARQUET_FILE`, `EVENT_PARQUET_FILE`,
`eventEnrolmentDetails` (read via a literal path, not the shared constant).

**Joins (16):** content/event-hours attach `LEFT`/`INNER` · state-hierarchy
resolution `INNER` (×2) · karma+hours attach `LEFT` (×4, state & centre
separately) · fixed-population override `LEFT` (a per-state **hardcoded
target population** overriding the actual registered-user count as the
per-capita denominator!) · org-name resolution `LEFT` · 4 sequential
per-user stat joins.

**Processing:** A real historical bug was fixed here and documented
inline — a lowercase `"xs"` bucket fallback used to create a phantom
rank-1 partition. Contains a leftover hardcoded single-user debug
`.show()` call in the production path.

**Writes:** Parquet `warehouse/nlw_mdo_leaderboard`; Cassandra
`"nlw_mdo_leaderboard"` (hardcoded literal, not config-driven); Postgres
app-schema `"nlw_user_leaderboard"` (same caveat); Redis
`lhp_certifications`/`lhp_learningHours` hashes (merge mode, using Redis
itself as the yesterday/today rollover memory).

### NPS Survey Trigger — `jobs/stage-2/npsUpgraded.py`

**Purpose:** Identifies users who should be prompted for the platform
NPS/rating survey — enrolled, completed, or rated a course in the last 15
days, and haven't already seen the prompt.

**Reads (4):** Druid (already-shown-popup users, 15-day),
`ENROLMENT_COMPUTED` (15-day), `RATING_PARQUET_FILE` (15-day, TimeUUID
decoded), Cassandra `user_feed` (`category=NPS2`, existing).

**Joins:** 0 conventional — eligibility is set algebra (`unionByName` +
`dropDuplicates`, then `.subtract()` for exclusions).

**Writes:** Cassandra `user_feed` (NPS2 entries) + `notification_feed_history`
audit copy.

> **Bug:** the file has two consecutive
> `if __name__=="__main__": main()` blocks — the entire pipeline and its
> Cassandra writes run **twice** per invocation.

### ODCS Content Recommendation — `jobs/stage-2/odcsRecommendation.py`

**Purpose:** Ranks each org's own courses by completion rate, rating and
volume, caching a top-15 list per org.

**Reads (4):** `ENROLMENT_WAREHOUSE_COMPUTED`, `CONTENT_WAREHOUSE_COMPUTED`,
`USER_WAREHOUSE_COMPUTED`, `RATING_PARQUET_FILE` (Course only).

**Joins (4):** enrolments × content `INNER` · + user `INNER` (mdo_id) · +
completion-% `LEFT` · + rating `LEFT`.

**Writes:** Redis `odcs_course_recomendation` (typo preserved from
source), field = `mdo_id`.

### Org Hierarchy Sync (original) — `jobs/stage-2/org_hierarchy.py`

**Purpose:** Fully rebuilds the ministry/department/org hierarchy lookup
in Postgres. **The only job in this repo that is plain Python, not
PySpark.** As of this branch, one of three coexisting variants of this job
— see the two entries immediately below, added together in the same commit
("Added org hierarchy and logging for datawarehouse").

**Reads (2):** Elasticsearch `org_v4` index (hardcoded host), an external
FRAC framework REST API (hardcoded prod host).

**Joins:** 0 — row-by-row Python dict-walking, not Spark.

**Processing:** Unconditionally `TRUNCATE`s 3 Postgres tables at the start
of every run — full refresh, no incremental mode.

**Writes:** Postgres app schema `org_hierarchy_new`, `org_hierarchy_lookup`,
`mdo_children_lookup`.

> **Security flag — PARTIALLY FIXED on this branch:** the hardcoded live
> bearer token was removed and replaced with a commented-out placeholder,
> and a stray `cookie` header was deleted. The hardcoded Postgres password
> (`"password123"`) and both the Elasticsearch (`10.175.5.10`) and Postgres
> (`10.175.5.15`) hostnames are still hardcoded directly in source. The
> sibling job below, `orgHierarchyAll.py`, fully resolves this — but
> `org_hierarchy.py` itself still has the gap.

### Org Hierarchy Sync (config-driven rewrite) — `jobs/stage-2/orgHierarchyAll.py`

**Purpose:** A fully config-driven rewrite of `org_hierarchy.py`'s exact
same responsibility — identical function set (`fetch_es_data`,
`parse_framework_data`, `insert_to_postgres`, `parse_pg_timestamp`,
`build_flat_descendant_map`, `insert_mdo_children_lookup`,
`find_and_insert_all_children`, `process_frameworks`,
`insert_hierarchy_lookup`), same 3 target tables, but every hardcoded value
replaced by a config lookup.

**Reads (2):** Elasticsearch `org_v4` index, via
`config.sparkElasticsearchConnectionHost`/`Port`; the FRAC framework API via
`config.api_url_template` (new config key, also added to
`jobs/config.py`/`config.py.j2` on this branch) — no hardcoded host or
token.

**Joins:** 0 — row-by-row Python dict-walking, not Spark.

**Processing:** Postgres connection built entirely from
`config.dwPostgresHost`/`appPostgresSchema`/`appPostgresUsername`/
`appPostgresCredential` — no hardcoded credentials anywhere in this file.
Otherwise identical logic to `org_hierarchy.py`.

**Writes:** the same 3 Postgres app-schema tables as `org_hierarchy.py`.

> **Observation:** the framework-API call in this file
> (`requests.get(url)`) sends **no** `Authorization` header at all — a
> stricter fix would have replaced the hardcoded bearer token with a
> config-driven one, but this version sends none. Whether the endpoint
> genuinely requires no auth, or this was dropped as a side effect of
> removing the hardcoded credential, isn't determinable from the repo.

### Org Hierarchy Sync (empty-framework patch) — `jobs/stage-2/orgHerarchyUpdatedEmpty.py`

**Purpose:** A narrower variant that (a) also queries two specific
hardcoded org identifiers directly, in addition to the usual state/ministry
query (a code comment references "karmayogi bharat" and "karyogi prorambh
trainee"), and (b) — the "Empty" in the filename — inserts a default
all-`NULL` hierarchy row for any org whose framework has no `LevelOne`
terms, instead of silently skipping it the way the other two variants do.

**Reads (2):** Elasticsearch `org_v4` index (config-driven, same as
`orgHierarchyAll.py`) via two separate scroll queries — the standard
state/ministry filter, and a second query filtered to the 2 hardcoded org
IDs; the FRAC framework API via `config.api_url_template`.

**Joins:** 0. Both ES result sets are unioned and deduplicated by
`identifier` in pandas before processing.

**Processing:** Only `TRUNCATE`s and writes `org_hierarchy_new` — the calls
that would populate `org_hierarchy_lookup` and `mdo_children_lookup`
(`insert_hierarchy_lookup`, `find_and_insert_all_children`) are commented
out in `process_frameworks`, and the corresponding `TRUNCATE` statements for
those two tables are commented out too — so this variant deliberately
leaves those two tables untouched.

**Writes:** Postgres app schema `org_hierarchy_new` only.

**Downstream (all three org-hierarchy variants):** other services needing
"children of MDO X" lookups or the flattened ministry→department→org path;
not read by any other job in this repo (those read org hierarchy from
Parquet caches instead). Which of the three is actually scheduled in
production isn't determinable from the repo — see Known Issues.

### Peer Validation — Eligible Users — `jobs/stage-2/peerValidationEligibleUsers.py`

**Purpose:** Determines which learners should be asked to complete a
peer-review survey for a course they just finished, and enqueues eligible
(user, form) pairs into a Postgres queue.

**Reads (6):** Postgres `peer_validation_notification_queue` (dedup),
`peer_validation_form_state` (watermark), Elasticsearch `fs-forms-alias-v2`
(peerValidationSurvey forms), `ENROLMENT_COMPUTED` (certified+completed),
`USER_ORG_COMPUTED_FILE`, `CONTENT_COMPUTED`.

**Joins (5):** forms × history `LEFT` · enrolment × user-org `LEFT` · +
forms (exploded) `INNER course_id` · + course details `LEFT` · eligible
`LEFT_ANTI` existing queue.

**Processing:** Incremental watermark window (scans only since
`last_processed_date + 1` after the first run); `notification_id =
md5(user_id + form_id)` idempotency key.

**Writes:** `INSERT` into `peer_validation_notification_queue` (dw schema,
append).

### Peer Validation — Notification Sender — `jobs/stage-2/peerValidationNotificationSender.py`

**Purpose:** Drains PENDING rows from the peer-validation queue, POSTs
them in batches to the Notification API, and advances each form's watermark
once its notifications are confirmed sent.

**Reads (2):** the same Postgres queue (PENDING), `peer_validation_form_state`.

**Joins (1):** `LEFT_ANTI` untouched forms ⋈ newly-advanced forms —
preserves prior watermarks before the union+overwrite.

**Processing:** Kill-switch config (`apiBasedNotificationEnabled`) allows
dry-run mode.

**Writes:** Per-row `UPDATE` of status on the queue; full `OVERWRITE` of
`form_state`; Kafka topic `peerValidationKafkaTopic`; HTTP POST to the
Notification API.

> **Config gap:** the Kafka broker config `kpBrokerList` has no dev
> fallback default in `default_config.py`, unlike `brokerList` — it can
> silently fail outside a fully templated deployment.

### Content Status Validation (v5) — `jobs/stage-2/programProgressSyncList_v5.py`

**Purpose:** Designed to cross-check denormalized completion status
against the row-level Cassandra consumption table and flag mismatches — but
**the entire validation pipeline is dead code** in this branch.

**Reads (1 live):** Cassandra `user_content_consumption_v2` — the only
read that actually executes.

**Joins:** 0 live (the dormant code contains ~15 joins across PySpark and
DuckDB, never called).

> **Critical finding:** lines ~839–933 (course validation, DuckDB program
> validation, 3 cert-readiness metrics, all report writing) are wrapped in
> an unused Python triple-quoted string literal — syntactically valid,
> never executes. No `_v1`–`_v4` files exist anywhere in the repo; this job
> is absent even from `bq-scripts.sh`.

**Writes:** Parquet `consumption_v2` — a refreshed cache of the Cassandra
table; the only thing that actually happens.

---

## Stage 2 — Surveys & Export

### Survey Question Report — `jobs/stage-2/surveyQuestionReport.py`

**Purpose:** Generates one CSV per survey "solution" with every question
response, enriched with respondent profile fields. Column layout is driven
entirely by a MongoDB config document.

**Reads (4):** MongoDB `reportConfigCollection` (dynamic column config),
`solutions` (end-dates); Druid `sl-survey` — distinct submission IDs, then
batched row data.

**Joins:** 0 — Druid filtering substitutes for joins.

**Processing:** Batches Druid queries by submission ID to avoid overloading
Druid. Column set, profile fields and sort order all come from Mongo — ops
can add columns without a code change.

**Writes:** One CSV per solution, zipped and synced to GCS under
`standalone-reports/ml-report`.

### Survey Status Report — `jobs/stage-2/surveyStatusReport.py`

**Purpose:** Intended to report survey completion status, but is a near
byte-for-byte copy of `surveyQuestionReport.py`.

**Reads (4):** identical to `surveyQuestionReport.py` — still queries the
MongoDB config document named `"surveyQuestionReport"` (not
`"surveyStatusReport"`), still uses `SurveyQuestionReportBatchSize` (not the
dedicated `SurveyStatusReportBatchSize` config that exists).

**Writes:** the identical output folder `SurveyQuestionsReport` — running
both jobs on the same day targets the same folder.

### Unenrolment Report — `jobs/stage-2/unenrollmentReport.py`

**Purpose:** Per-org report of every user unenrolled from a course — who
unenrolled them, when, why, and how far they'd progressed.

**Reads (4 + ACBP):** `UNENROLMENT_AUDIT_PARQUET_FILE` (UNENROLL only),
`ENROLMENT_COMPUTED` (enrolled), `USER_ORG_COMPUTED_FILE`,
`CONTENT_COMPUTED`, `ACBP_COMPUTED_FILE`.

**Joins (4):** enrolment × content `LEFT` · + user-org `LEFT` · +
ACBP-mandate flag `LEFT` · + unenrolment audit `INNER` — this inner join
**is** the row filter limiting the report to actually-unenrolled rows.

**Writes:** CSV `UnenrollmentReport.csv` per MDO. Warehouse:
`unenrolled_user_audit` — Parquet, not yet in `dataWarehouse.py`'s sync
list.

### User Activity Fact Table — `jobs/stage-2/userActivity.py`

**Purpose:** Builds one unified "what did this user consume and when" fact
table, combining content and event activity. Warehouse-only, no CSV.

**Reads (7, 3 dead/unused):** `USER_ORG_COMPUTED_FILE`; `warehouse/content`,
`warehouse/user_enrolments` (cross-job dependency on `userEnrolment.py`);
`EVENT_PARQUET_FILE`, `EVENT_ENROLMENT_PARQUET_FILE`. (`ORG_COMPUTED`,
`USER_COMPUTED`, `ORG_HIERARCHY_SELECT` are read but never used downstream.)

**Joins (3):** event-enrolments × events (broadcast) `LEFT` · + user-org
(broadcast) `LEFT` · enrolments × content (broadcast) `LEFT`.

**Writes:** warehouse `user_activity` — Parquet, not currently synced by
`dataWarehouse.py`.

### User Data → Redis — `jobs/stage-2/userDataToRedis.py`

**Purpose:** Pushes a lightweight per-user profile snapshot into Redis so
front-end services can render user cards without hitting Cassandra/Postgres
on every request.

**Reads (1):** `USER_SELECT_PARQUET_FILE`. **Joins:** 0.

**Processing:** `repartition(500)` + `foreachPartition` with pipelined
batched `SET` (25,000/batch).

**Writes:** Redis `user:{user_id}` → JSON profile snapshot, overwritten
every run, no TTL.

> **Bug:** uses a plain `redis.Redis` client without selecting
> `config.redisDB` — always writes DB 0 regardless of the configured DB
> index.

### User Enrolment Report (Stage 2D) — `jobs/stage-2/userEnrolment.py`

**Purpose:** The canonical "who is enrolled in what, with what
progress/certificate status" report for every org — platform and
marketplace content combined, split Govt vs. Non-Govt. Part of
`jobs/main.py`'s orchestrated pipeline.

**Reads (7):** `UNENROLMENT_AUDIT` (deduped via a `row_number()` window — an
explicit fan-out-prevention fix), `ENROLMENT_COMPUTED` (no filter),
`USER_ORG_COMPUTED_FILE`, `CONTENT_COMPUTED`, `EXTERNAL_ENROLMENT_COMPUTED`,
`EXTERNAL_CONTENT_COMPUTED`, `ACBP_COMPUTED_FILE`.

**Joins (6):** enrolment × content `LEFT` · + user-org `LEFT` ·
external-content × external-enrolment `INNER` · + user-org `LEFT` · +
ACBP-mandate `LEFT` · + unenrolment audit `LEFT` (not `INNER` — unlike
`unenrollmentReport.py`, this keeps both enrolled *and* unenrolled rows).

**Processing:** Govt/Non-Govt split by exact-match VOLUNTEER
designation/role; orgs with both cohorts get two output folders.

**Writes:** CSV `ConsumptionReport.csv` ×2 (Govt + Non-Govt). Warehouse:
`user_enrolments` (synced) — read by `userActivity.py`, `dsrComputation.py`,
`nationalLearningWeek.py`, `odcsRecommendation.py`, `ministryMetrics.py`,
`dashboardSync.py`.

**Downstream:** one of the most widely-depended-on outputs in the entire
pipeline. Bundled by `zipUpload.py`.

### User Report (Stage 2A) — `jobs/stage-2/userReport.py`

**Purpose:** The canonical per-user profile+activity report for every org —
karma points, badges, enrolment/completion counts, learning hours, and
dynamically-pivoted org-specific custom fields. Part of `jobs/main.py`'s
orchestrated pipeline. Its learning-hours calculation was substantially
reworked on this branch.

**Reads (8):** `USER_ORG_COMPUTED_FILE`, `ENROLMENT_WAREHOUSE_COMPUTED`,
`GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE`, `CONTENT_HIERARCHY_FLATTENED_PARQUET_FILE`
(**changed** — replaces the old `CONTENT_COMPUTED` read for duration),
`EVENT_ENROLMENT_PARQUET_FILE` (via `userDFUtil` helper), `USER_EXTENDED_PROFILE`
(org custom fields), `EXTERNAL_CONTENT_PARQUET_FILE` (**new**),
`EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE` (**new**).

**Joins (6):** external enrolments ⋈ external content `LEFT content_id`
(**new** — builds a per-user external/marketplace content-duration total) ·
+ external duration `LEFT userID` (**new**) · content-duration attach
`LEFT` (helper) · + event-details `LEFT` (helper) · + badges `LEFT` ·
org-scoped custom-field pivot `LEFT` (per org).

**Processing:** `SparkSession` built at module **import time** (same
unusual pattern as `gamificationJob.py`). **REWORKED on this branch:**
course duration for `Blended Program`, `Comprehensive Assessment Program`
and `Curated Program` categories is now summed from the *eligible
first-level children* of each course (via the new
`CONTENT_HIERARCHY_FLATTENED_PARQUET_FILE`, filtering children by
`primaryCategory` — `Course Unit`/`Learning Resource` for Blended Program,
`Course Assessment` for the other two) instead of using one flat
parent-level duration value — a real accuracy fix, since those content
types' true duration lives on their components, not the parent record.
`Total_Learning_Hours` now also adds in marketplace/external content
duration (`total_external_content_duration`, pulled from
`EXTERNAL_CONTENT_PARQUET_FILE`'s `cios_data.content.duration` JSON field
for users with a completed, certificated external enrolment) — previously
external content wasn't counted toward learning hours at all. A `DOB` field
(from `personalDetails.dob`) was also added to the report output. The
existing explicit perf-optimization split (documented as fixing a 30-min →
3-hour regression: orgs *with* custom fields go through an expensive
per-org pivot path with 8 parallel workers; orgs *without* go through a
fast bulk path) is unchanged.

**Writes:** CSV `UserReport.csv` ×2 (Govt + Non-Govt). Warehouse:
`user_detail` (synced), `userCustomFields`, per-org
`user_custom_report/<org>_custom_report.parquet`.

**Downstream:** now also depends on `contentDFUtil`'s new
`CONTENT_HIERARCHY_FLATTENED_PARQUET_FILE` output (see that module's entry
above).

> **Config gap:** production `config.py`'s zip-bundle list is missing
> `"user-custom-report"` that exists in the local-dev fallback list in
> `default_config.py` — that output may not get bundled in a real
> deployment.

### Weekly Claps (engagement streak) — `jobs/stage-2/weeklyClaps.py`

**Purpose:** A streak mechanic: if a user's weekly time-on-app crosses a
threshold, they earn a "clap" for the week, tracked in a rolling 4-week
window.

**Reads (2):** `CLAPS_PARQUET_FILE` (prior state, itself sourced from
Postgres `learner_stats` via Stage 0), Druid (this week's engagement time).

**Joins (1):** `FULL OUTER` existing claps ⋈ this-week engagement on
`userid` — brand-new users still get a row.

**Processing:** Monday-rollover state machine shifts the `w1`–`w4` window
and finalizes last week's clap based on whether the cutoff (default 60
min) was crossed.

**Writes:** Postgres **app schema** `learner_stats`, overwrite+truncate — a
closed read-modify-write loop: this job writes what Stage 0 reads again
next run.

### Workflow Summarizer — `jobs/stage-2/workFlowSummarizer.py`

**Purpose:** A Python re-implementation of a Scala telemetry-session-
summarization model, referenced in config as `WFS_MODEL_CONFIG`. **Its
`main()` only exercises the algorithm against an empty test RDD in this
branch** — no real data source is wired up.

**Reads:** none in practice (`sc.parallelize([])`). **Joins:** 0.

**Processing:** The state-machine session reconstruction (idle-time/
session-break detection) is fully implemented as a library, just never
called with real data here. No Kafka dispatch happens despite config
declaring a sink.

**Writes:** none.

### Zip, Password & Upload — `jobs/stage-2/zipUpload.py`

**Purpose:** The terminal packaging step: gathers every report job's per-org
CSV output, generates one password-protected ZIP per org (shared across a
ministry), stores the passwords in Redis, and uploads everything to GCS.

**Reads:** `warehouse/org_hierarchy` (drives password grouping); the
filesystem tree of every report job's dated `mdoid=<org>` output folder,
per config's bundle lists.

**Joins:** 0 — Spark only reads `org_hierarchy`; the rest is filesystem/OS
work.

**Processing:** Password grouping priority: ministry > department >
individual org — orgs under the same ministry share one password.

**Writes:** password-protected `reports.zip` per org, uploaded to GCS;
Redis `CB_EXT_{mdoid}_password` → the password string (read by the
notification/portal layer).

> **Bug:** the disabled `createFullReport` branch references two undefined
> names (`password`, `part2_start`) — a `NameError` risk, currently inert
> only because that flag defaults `False` everywhere.

**Downstream:** the fan-in consumer of `userReport.py` and
`userEnrolment.py`'s CSV outputs, among others.

---

## Known issues found during this read

Flagged for engineering awareness. This list started against
`cbrelease-4.8.39.1-hotfix` and has now been checked directly against the
current `cbrelease-4.8.39.3` branch (2026-08-24) — live file content, not
just diffs. **Result: 15 of 17 are present exactly as described; 1 is
partially fixed; 1 is a new observation** surfaced by the org-hierarchy
jobs added on this branch. Nothing was changed as part of this check.

| Severity | Status | Issue |
|---|---|---|
| Critical | Open | `programProgressSyncList_v5.py`'s entire validation pipeline is wrapped in an unused Python string literal — only a Cassandra→Parquet cache refresh actually runs, despite the file being 994 lines. |
| Critical | **Partially fixed** | `org_hierarchy.py` — on this branch, the hardcoded live framework-API bearer token was removed and replaced with a commented-out placeholder (and a stray `cookie` header was deleted). The hardcoded Postgres password (`password123`) and both the Elasticsearch (`10.175.5.10`) and Postgres (`10.175.5.15`) hostnames are **still hardcoded directly in source**. A sibling job added on this branch, `orgHierarchyAll.py`, does fully resolve this by reading everything from config — but `org_hierarchy.py` itself still has the gap, and it's not clear which of the two is actually scheduled. |
| Serious | Open | `surveyStatusReport.py` is a functional duplicate of `surveyQuestionReport.py` — wrong Mongo config name, wrong batch-size config key, and both write to the identical output folder. |
| Serious | Open | `npsUpgraded.py` has two consecutive `if __name__=="__main__"` blocks — the whole pipeline and its Cassandra writes run twice per invocation. |
| Serious | Open | `gamificationNotificationConsumer.py` / `gamificationNotificationProducer.py` reference config keys (`dwnotificationQueue`, `gamificationNotificationBatchSize`, `gamificationNotificationEligibilityDays`) that don't exist anywhere in `config.py`, `default_config.py`, or the Ansible template. |
| Serious | Open | `l2Assessments.py` writes to a hardcoded absolute filesystem path instead of the config-driven convention every other job follows. |
| Warning | Open | `userDataToRedis.py` always writes to Redis DB 0 — it never selects `config.redisDB`. |
| Warning | Open | `ministryLeaderboard.py`'s own log lines say "Writing to Cassandra" but the job actually writes to Postgres. |
| Warning | Open | `zipUpload.py`'s disabled `createFullReport` branch references two undefined names (`password`, `part2_start`) — a `NameError` risk if that flag is ever turned on. |
| Warning | Open | `dsrComputationUpdated.py` (deprecated) contains a hardcoded bearer token for a live external portal API call. |
| Warning | Open | `peerValidationNotificationSender.py`'s Kafka broker config (`kpBrokerList`) has no fallback default in `default_config.py`, unlike `brokerList` — it can silently fail outside a fully templated deployment. |
| Info | Open | Two parallel implementations exist for the same warehouse-sync responsibility — `dataWarehouse.py` (Spark/JDBC overwrite) and `dataWarehouseBash.sh` (DuckDB, more schema-safe, and the one that gained real fixes on this branch) — which one runs in production isn't visible from the repo alone. |
| Info | Open | The Postgres `content` warehouse table has two independent producers — Stage 1's `contentDFUtil.preComputeContentWarehouseData` and Stage 2's `courseReport.py` — the latter is the one `capAllotment.py` and the sync jobs actually depend on. |
| Info | Open | Several newer warehouse outputs (`cbp_enrollments`, `apar_cbp_enrollment`, `bk_course_enrolments`/`bk_event_enrolments`, `cap_allocation_meta`/`cap_allocation_user_wise`) are written as Parquet but not yet wired into `dataWarehouse.py`'s Postgres sync — likely in-progress features. |
| Info | Open | `course_completion_survey_details`'s Postgres sync line exists in `dataWarehouse.py` but is commented out — Parquet-only in practice today. |
| Info | Open | `userReport.py`'s "user-custom-report" output exists in `default_config.py`'s zip-bundle fallback list but is missing from the templated production `config.py` list — may not get bundled in a real deployment. |
| Info | **New** | Three coexisting org-hierarchy jobs now exist (`org_hierarchy.py`, `orgHierarchyAll.py`, `orgHerarchyUpdatedEmpty.py`) with near-identical logic and no cross-references between them — which one is actually scheduled in production isn't determinable from the repo. `orgHierarchyAll.py`'s framework-API call also sends no `Authorization` header at all, unlike the original's (now-disabled) bearer token — unclear whether the endpoint needs none, or this was simply dropped along with the hardcoded credential rather than replaced with a config-driven one. |

### How this was verified

Every "Open" row above was confirmed by directly grepping the live file
content on `cbrelease-4.8.39.3` — e.g. `grep -n "if __name__"
jobs/stage-2/npsUpgraded.py` still returns two matches, `grep -n
"part2_start" jobs/stage-2/zipUpload.py` still finds the undefined
reference, `dwnotificationQueue`/`gamificationNotificationBatchSize` are
still absent from both config files, and `dataWarehouse.py` still has no
reference to `cap_allocation_*`/`bk_course_enrolments`/`cbp_enrollments`/
`apar_cbp_enrollment`. A `git diff` between the originally-analyzed hotfix
commit and current `cbrelease-4.8.39.3` touched exactly 9 files
(`org_hierarchy.py`, `jobs/config.py`, `constants/ParquetFileConstants.py`,
`dfutil/content/contentDFUtil.py`, `userReport.py`, `learnerLeaderboard.py`,
`assessmentReport.py`, `dashboardSync.py`, `dataWarehouseBash.sh`,
`ansible/roles/pyspark-deploy/templates/config.py.j2`) and added 3 new job
files (`orgHierarchyAll.py`, `orgHerarchyUpdatedEmpty.py`,
`postgresToParquet.py`), all read in full for this update. Real, unrelated
feature work (a new content-hierarchy-flattening capability, a
learning-hours calculation fix, a DOB field addition, an assessment
score-column fix, a trending-courses date-window fix, a warehouse-sync
determinism fix) landed alongside the one partial security fix, but none
of it touches the other 15 originally-flagged issues.
