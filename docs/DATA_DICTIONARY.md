# cb-core-data — Data Dictionary

Every named dataset in the pipeline: raw Parquet caches, Stage-1 computed
Parquet tables, the Postgres/BigQuery warehouse, and the Cassandra/Redis
tables written directly by Stage 2 jobs. Cross-reference with
[`JOB_DETAILS.md`](JOB_DETAILS.md) for what each job does with these, and
[`pipeline-overview.html`](pipeline-overview.html) for an interactive,
searchable version of the same tables.

All Parquet path constants are defined in one place:
`constants/ParquetFileConstants.py`. `INPUT_DIR` = raw caches
(`data-res/pq_files/cache_pq/`); `OUTPUT_COMPUTED_DIR` = Stage-1 output
(`output/computed/`).

---

## 1. Raw Parquet caches — written by Stage 0

One folder per Cassandra table, Elasticsearch query, or Postgres table
pulled straight from source, with no transformation beyond column renaming
and light JSON parsing. Written once per Stage-0 run; everything downstream
reads these instead of hitting the live source.

| Constant | Folder | Source | Populated by |
|---|---|---|---|
| `ACBP_PARQUET_FILE` | `acbp/` | Cassandra `cassandraAcbpTable` | `dataExhaust.py` |
| `BATCH_PARQUET_FILE` | `batch/` | Cassandra `cassandraCourseBatchTable` | `dataExhaust.py` |
| `ENROLMENT_PARQUET_FILE` | `enrolment/` | Cassandra `cassandraUserEnrolmentsTable` | `dataExhaust.py` |
| `ESCONTENT_PARQUET_FILE` | `esContent/` | Elasticsearch `compositesearch` (content query) | `dataExhaust.py` |
| `EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE` | `externalCourseEnrolments/` | Cassandra `sunbird_courses.user_external_enrolments` | `dataExhaust.py` |
| `EXTERNAL_CONTENT_PARQUET_FILE` | `externalContent/` | Postgres(app) `cios_content_entity` | `dataExhaust.py` |
| `EVENT_PARQUET_FILE` | `eventDetails/` | Elasticsearch `compositesearch` (`objectType='Event'`) | `dataExhaust.py` |
| `EVENT_ENROLMENT_PARQUET_FILE` | `eventEnrolmentDetails/` | Cassandra `user_entity_enrolments`, joined w/ event details | `dataExhaust.py` |
| `HIERARCHY_PARQUET_FILE` | `hierarchy/` | Cassandra `cassandraContentHierarchyTable` | `dataExhaust.py` |
| `KCMV6_PARQUET_FILE` | `kcmV6/` | Cassandra `cassandraFrameworkHierarchyTable` (`identifier='kcmfinal_fw'`) | `dataExhaust.py` |
| `LEARNER_LEADERBOARD_PARQUET_FILE` | `learnerLeaderBoard/` | Cassandra `cassandraLearnerLeaderBoardTable` | `dataExhaust.py` |
| `NLW_CONTENT_CERTIFICATE_GENERATED_COUNT_PARQUET_FILE` | `nlwContentCertificateGeneratedCount/` | *defined but not observed read/written in this branch* | — |
| `NLW_CONTENT_LEARNING_HOURS_PARQUET_FILE` | `nlwContentLearningHours/` | *defined but not observed read/written in this branch* | — |
| `ORG_PARQUET_FILE` | `org/` | Cassandra `cassandraOrgTable` | `dataExhaust.py` |
| `ORG_COMPLETE_HIERARCHY_PARQUET_FILE` | `orgCompleteHierarchy/` | Postgres(app) `org_hierarchy_v4` (raw, unjoined) | `dataExhaust.py` |
| `ORG_HIERARCHY_PARQUET_FILE` | `orgHierarchy/` | Cassandra org ⋈ Postgres `org_hierarchy_v4` (3 joins) | `dataExhaust.py` |
| `RATING_PARQUET_FILE` | `rating/` | Cassandra `cassandraRatingsTable` | `dataExhaust.py` |
| `RATING_SUMMARY_PARQUET_FILE` | `ratingSummary/` | Cassandra `cassandraRatingSummaryTable` | `dataExhaust.py` |
| `ROLE_PARQUET_FILE` | `role/` | Cassandra `cassandraUserRolesTable` | `dataExhaust.py` |
| `USER_PARQUET_FILE` | `user/` | Cassandra `cassandraUserTable` | `dataExhaust.py` |
| `CLAPS_PARQUET_FILE` | `weeklyClaps/` | Postgres(app) `learner_stats` | `dataExhaust.py`; also rewritten by `weeklyClaps.py`'s Postgres update on the *next* run |
| `USER_KARMA_POINTS_PARQUET_FILE` | `userKarmaPoints/` | Cassandra `cassandraKarmaPointsTable` | `dataExhaust.py` |
| `USER_KARMA_POINTS_SUMMARY_PARQUET_FILE` | `userKarmaPointsSummary/` | Cassandra `cassandraKarmaPointsSummaryTable` | `dataExhaust.py` |
| `USER_ASSESSMENT_PARQUET_FILE` | `userAssessment/` | Cleaned **in place** by `assessmentDFUtil.parse_raw_assessment_data` (Stage 1) | Stage 1, overwriting the raw cache dir |
| `OLD_ASSESSMENT_PARQUET_FILE` | `oldAssessmentDetails/` | Cassandra `cassandraOldAssesmentTable` (safe-columns read) | `dataExhaust.py` |
| `USER_EXTENDED_PROFILE` | `userExtendedProfile/` | Cassandra `cassandraUserExtendedProfileTable` | `dataExhaust.py` |
| `FINAL_ASSESSMENT_PARQUET_FILE` | `esFinalAssessment/` | Elasticsearch `compositesearch` (Course + Final Program Assessment) | `dataExhaust.py` |
| `COURSE_COMPLETION_SURVEY_PARQUET_FILE` | `courseCompletionSurvey/` | Elasticsearch `fs-forms-data-alias-v2` | `dataExhaust.py` |
| `PEER_VALIDATION_FORMS_PARQUET_FILE` | `peerValidationSurveys/` | *defined but not observed read/written in this branch* — `peerValidationEligibleUsers.py` reads forms live from ES instead | — |
| `PEER_VALIDATION_ELIGIBLE_USERS_PARQUET_FILE` | `peerValidationEligibleUsers/` | *defined but not observed read/written in this branch* | — |
| `QUESTIONSET_HIERARCHY_PARQUET_FILE` | `questionsetHierarchy/` | Cassandra `cassandraQuestionSetHierarchyTable` | `dataExhaust.py` |
| `ASSESSMENT_DATA_RAW_PARQUET_FILE` | `userAssessmentRaw/` | Cassandra `cassandraUserAssessmentTable` | `dataExhaust.py` |
| `UNENROLMENT_AUDIT_PARQUET_FILE` | `unenrolledUserAudit/` | Cassandra `sunbird_courses.enrollment_history_by_action` | `dataExhaust.py` |

Two additional raw caches exist under `data-res/pq_files/cache_pq/` **without**
a named constant in `ParquetFileConstants.py` (read via an inline, ad hoc
path string in the jobs that use them):

| Folder | Source | Read by |
|---|---|---|
| `accessControlSettings/` | Cassandra `sunbird_courses.access_setting_rules_v2` | `dataExhaust.py` (write), `capAllotment.py` (read) |
| `esCourseAssessment/` | Elasticsearch `compositesearch` (Course Assessment) | `dataExhaust.py` (write), `courseBasedAssessmentReport.py` / `l2Assessments.py` (read) |

---

## 2. Computed Parquet tables — written by Stage 1

Live under `output/computed/`. These are the tables almost every Stage 2 job
reads from — nothing here talks to a live source system.

| Constant | Folder | Built by | Notes |
|---|---|---|---|
| `USER_SELECT_PARQUET_FILE` | `user-select/` | `userDFUtil.preComputeUser` | mid-step intermediate |
| `ACBP_SELECT_FILE` | `acbp-select/` | `acbpDFUtil_v3.preComputeACBPData` | plan-level, pre-allocation |
| `ENROLMENT_SELECT_PARQUET_FILE` | `enrolment-select/` | `enrolmentDFUtil.preComputeEnrolment` | mid-step intermediate |
| `ENROLMENT_COMPUTED_PARQUET_FILE` | `enrolment-computed/` | `enrolmentDFUtil.preComputeEnrolment` | **the master enrolment table**, read by ~15 Stage 2 jobs |
| `EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE` | `external-enrolment-computed/` | `enrolmentDFUtil.preComputeExternalEnrolment` | marketplace enrolments |
| `EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE` | `external-content-computed/` | `contentDFUtil.preComputeExternalContentDataFrame` | marketplace content metadata |
| `RATING_SELECT_PARQUET_FILE` | `rating-select/` | *defined but not observed read/written in this branch* | — |
| `CONTENT_COMPUTED_PARQUET_FILE` | `content-computed/` | `contentDFUtil.preComputeContentDataFrame` | **the master content table**, read by ~18 Stage 2 jobs |
| `USER_COMPUTED_PARQUET_FILE` | `user-computed` | `userDFUtil.preComputeUser` | user profile, roles, karma, claps |
| `ORG_SELECT_PARQUET_FILE` | `org-select/` | `userDFUtil.preComputeOrgWithHierarchy` | mid-step intermediate |
| `ORG_HIERARCHY_SELECT_PARQUET_FILE` | `org-hierarchy-select/` | `userDFUtil.preComputeOrgWithHierarchy` | mid-step intermediate |
| `ORG_COMPUTED_PARQUET_FILE` | `org-computed/` | `userDFUtil.preComputeOrgWithHierarchy` | org + hierarchy names |
| `USER_ORG_COMPUTED_FILE` | `user-org-computed/` | `userDFUtil.preComputeOrgHierarchyWithUser` | **the single most-read table in the repo** — every user joined to org/ministry/dept |
| `ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE` | `all-course-program-computed/` | `contentDFUtil.preComputeAllCourseProgramESDataFrame` | course/program catalog from ES |
| `ALL_ASSESSMENT_COMPUTED_PARQUET_FILE` | `all-assessment-computed/` | `assessmentDFUtil.precomputeAssessmentEsDataframe` | assessment catalog from ES |
| `ACBP_COMPUTED_FILE` | `acbp-computed` | `acbpDFUtil_v3.preComputeACBPData` | final user × plan ACBP allocation |
| `RATING_SUMMARY_COMPUTED_PARQUET_FILE` | `rating-summary-computed/` | `contentDFUtil.preComputeRatingAndSummaryDataFrame` | per-course rating rollup |
| `RATING_COMPUTED_PARQUET_FILE` | `rating-computed/` | `contentDFUtil.preComputeRatingAndSummaryDataFrame` | per-user rating rows |
| `CONTENT_RATING_COMPUTED_PARQUET_FILE` | `content-rating-computed/` | `contentDFUtil.preComputeRatingAndSummaryDataFrame` | per-course rating count/avg |
| `TEMP_COMPUTE_FILE` | `temp-computed/` | scratch constant, not a durable output | — |
| `CONTENT_HIERARCHY_SELECT_PARQUET_FILE` | `content-hierarchy-select/` | `contentDFUtil.precomputeContentHierarchyDataFrame` | raw hierarchy JSON, selected columns |
| `BATCH_SELECT_PARQUET_FILE` | `batch-select/` | `enrolmentDFUtil.preComputeEnrolment` | mid-step intermediate |
| `OLD_ASSESSMENT_COMPUTED_PARQUET_FILE` | `old-assessment/` | `assessmentDFUtil.precomputeOldAssessmentDataframe` | legacy pre-migration assessments |
| `ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE` | `enrolment-warehouse-computed/` | `enrolmentDFUtil.preComputeUserEnrolmentWarehouseData` | feeds `dashboardSync.py`'s entire query library |
| `USER_WAREHOUSE_COMPUTED_PARQUET_FILE` | `user-warehouse-computed/` | `userDFUtil.preComputeUserWarehouseData` | warehouse-shaped user frame |
| `CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE` | `content-warehouse-computed/` | `contentDFUtil.preComputeContentWarehouseData` | warehouse-shaped content frame — **also independently rewritten by `courseReport.py` in Stage 2** |
| `GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE` | `user_enrolment_badge_computed/` | `gamificationJob.py` — **Stage 2, not Stage 1**, despite living alongside these constants | badge-per-enrolment facts |

---

## 3. The Postgres warehouse

Two logical Postgres databases exist:

- **App database** (`sunbird` schema, host `appPostgresHost`) — the live
  application's own operational tables (`org_hierarchy_v4`, `learner_stats`,
  and a handful of tables written directly by Stage 2 jobs — see §5).
- **Warehouse database** (`warehouse` schema, host `dwPostgresHost`) — the
  BI-facing analytics warehouse. This is what `dataWarehouse.py`/
  `dataWarehouseBash.sh` populate.

Every table below is staged first as local Parquet under
`{warehouseReportDir}/<table>/` (default `<BASE_DIR>/warehouse/<table>/`) by
its "populated by" job, then pushed into Postgres by `dataWarehouse.py` (or
its DuckDB sibling `dataWarehouseBash.sh`) with a **full truncate + reload
every run** — there is no incremental/upsert path anywhere in this pipeline.

| Warehouse table | Populated by (Parquet write) | Synced to Postgres? | Synced to BigQuery? | Content |
|---|---|---|---|---|
| `user_detail` | `userReport.py` (Stage 2A) | ✅ overwrite | ✅ | Full user profile: karma points, badges, learning hours |
| `content` | `courseReport.py` **and** Stage-1's `contentDFUtil.preComputeContentWarehouseData` (two independent producers — see note below) | ✅ overwrite | ✅ | Course/program/marketplace catalog, ratings, SCORM flag |
| `content_resource` | `courseReport.py` | ✅ overwrite | ✅ | Flattened resource-level rows per course/program |
| `assessment_detail` | `courseBasedAssessmentReport.py` | ✅ overwrite | ✅ | Best attempt, score, pass/fail per user × assessment |
| `bp_enrolments` | `blendedReport.py` | ✅ overwrite | ✅ | Component/session-level Blended Program attendance |
| `cb_plan` | `acbpReport.py` | ✅ overwrite | ✅ | ACBP plan-level metadata |
| `org_hierarchy` | Stage-1 `contentDFUtil.writeWarehouseParquetFiles` (direct write, not via a report job) | ✅ overwrite | ✅ | Ministry → Department → MDO hierarchy |
| `kcm_content_mapping` | `kcmReport.py` | ✅ overwrite | ✅ | Course → competency-area mapping |
| `kcm_dictionary` | `kcmReport.py` | ✅ overwrite | ✅ | Area/Theme/SubTheme competency dictionary |
| `events` (Parquet folder: `event_details`) | Stage-1 `contentDFUtil.writeWarehouseParquetFiles` | ✅ overwrite | ✅ (as `event_details`) | Live-session event catalog |
| `events_enrolment` (Parquet folder: `event_enrolment_details`) | Stage-1 `contentDFUtil.writeWarehouseParquetFiles` | ✅ overwrite | ✅ (as `event_enrolment_details`) | Per-user event attendance + karma points earned |
| `user_enrolments` | `userEnrolment.py` (Stage 2D) | ✅ overwrite | ✅ | Platform + marketplace enrolment/completion facts (~19GB — loaded last in `dataWarehouseBash.sh`) |
| `course_completion_survey_details` | `courseCompletionSurveyReport.py` | ⚠️ written as Parquet, but the push line in `dataWarehouse.py` is **commented out** | ❌ | End-of-course survey ratings |
| `dsr_metrics_history` | `dsrComputation.py`, writes Postgres directly (not via `dataWarehouse.py`) | ✅ (own JDBC write) | ❌ | Daily platform KPI history |

**Note on `content`'s two producers:** `contentDFUtil.preComputeContentWarehouseData`
runs in Stage 1 and writes `CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE`.
`courseReport.py` runs in Stage 2 and writes the same `warehouse/content`
Parquet folder again, with additional fields (SCORM flag, ES-sourced
standalone-assessment rows). Because `courseReport.py` runs after Stage 1,
its write is the one that survives — and it's the one `capAllotment.py` and
`dataWarehouse.py`/`bq-scripts.sh` actually read. The Stage-1 write is
effectively overwritten before anyone reads it downstream.

### Warehouse-shaped tables not yet wired into the Postgres/BigQuery sync

These are written as local Parquet by their respective Stage 2 jobs, in the
same `warehouseReportDir/<table>/` convention as the table above, but
**`dataWarehouse.py` and `bq-scripts.sh` don't reference them yet** — most
likely in-progress features:

| Parquet folder | Written by | Would represent |
|---|---|---|
| `cbp_enrollments` | `acbpReport.py` | ACBP enrolment detail |
| `apar_cbp_enrollment` | `acbpReport.py` | APAR-linked competency export |
| `bk_course_enrolments` / `bk_event_enrolments` | `bharatKalpReport.py` | Bharat Kalp initiative enrolments |
| `cap_allocation_meta` / `cap_allocation_user_wise` | `capAllotment.py` | CAP access-control resolution results |
| `user_activity` | `userActivity.py` | Unified content+event activity fact table |
| `unenrolled_user_audit` | `unenrollmentReport.py` | Unenrolment audit trail |
| `userCustomFields` / `user_custom_report/<org>_*.parquet` | `userReport.py` | Per-org custom profile field pivots |

### Postgres tables written directly by Stage 2 jobs (outside `dataWarehouse.py`)

Several jobs skip the Parquet-staging convention entirely and JDBC-write
straight to Postgres themselves:

| Table | Database | Written by | Update method |
|---|---|---|---|
| `dsr_metrics_history` | warehouse schema | `dsrComputation.py` | full overwrite (history + today unioned) |
| `learner_stats` | **app** schema | `weeklyClaps.py` | overwrite+truncate — feeds back into Stage 0's own next-run read |
| `slw_mdo_top_learners` | **app** schema | `ministryLeaderboard.py` | overwrite+truncate |
| `nlw_user_leaderboard` | **app** schema | `nationalLearningWeek.py` | overwrite+truncate |
| `org_hierarchy_new`, `org_hierarchy_lookup`, `mdo_children_lookup` | **app** schema | `org_hierarchy.py` | unconditional `TRUNCATE` + re-`INSERT`, every run |
| `peer_validation_notification_queue`, `peer_validation_form_state` | warehouse schema | `peerValidationEligibleUsers.py` (insert) / `peerValidationNotificationSender.py` (update) | append + per-row update |
| `dwnotificationQueue` (gamification queue — **config key undefined**, see Known Issues) | warehouse schema | `gamificationNotificationProducer.py` (insert) / `gamificationNotificationConsumer.py` (update) | append + per-row update |

---

## 4. BigQuery sync reference

`bq-scripts.sh` loops over the same local `warehouseReportDir/<table>/`
Parquet folders and, for each, fully replaces the matching BigQuery table:
delete any staged files in GCS → copy fresh Parquet to GCS → `bq rm -f` the
table → `bq mk` an empty replacement (schema auto-inferred) → `bq load` →
clean up staged files. Target project `prj-kb-prd-looker-gcp-1014`, dataset
`kb_prod_dataset`, staging bucket `kb_prod_avro` (transient only — wiped
before and after every load; the "avro" naming is stale, the payload is
Parquet).

| Local source folder | BigQuery table | Notes |
|---|---|---|
| `warehouse/assessment_detail/` | `assessment_detail` | |
| `warehouse/bp_enrolments/` | `bp_enrolments` | |
| `warehouse/cb_plan/` | `cb_plan` | |
| `warehouse/content/` | `content` | |
| `warehouse/content_resource/` | `content_resource` | |
| `warehouse/kcm_content_mapping/` | `kcm_content_mapping` | |
| `warehouse/kcm_dictionary/` | `kcm_dictionary` | |
| `warehouse/org_hierarchy/` | `org_hierarchy` | |
| `warehouse/user_detail/` | `user_detail` | |
| `warehouse/user_enrolments/` | `user_enrolments` | largest table, ~19GB |
| `warehouse/event_details/` | `event_details` | Postgres equivalent is named `events` — naming diverges between the two sync targets |
| `warehouse/event_enrolment_details/` | `event_enrolment_details` | Postgres equivalent is named `events_enrolment` — same naming divergence |

This is the exact same 12-table set `dataWarehouse.py` pushes to Postgres,
kept in lockstep by both scripts reading from the same staged Parquet.

---

## 5. Cassandra tables written directly by Stage 2 jobs

Not every write in this pipeline goes through the Postgres/BigQuery
warehouse. Several jobs write user-facing, low-latency data straight to
Cassandra:

| Table | Keyspace | Written by | Mode |
|---|---|---|---|
| `learner_leaderboard`, `learner_leaderboard_lookup` | user keyspace | `learnerLeaderboard.py` | append |
| `nlw_mdo_leaderboard` (hardcoded literal name) | user keyspace | `nationalLearningWeek.py` | overwrite |
| `user_karma_points`, `user_karma_points_credit_lookup`, `user_karma_points_summary` | user keyspace | `karmaPoints.py` | append |
| `notification_feed` | `sunbird_notifications` | `inappReview.py` (append), `npsUpgraded.py` (write) | append |
| `notification_feed_history` (hardcoded `sunbird_notifications`) | `sunbird_notifications` | `npsUpgraded.py` | append (audit copy) |
| `user_content_consumption_v2` | `sunbird_courses` | *read only* by `programProgressSyncList_v5.py` (the job's real writes are dead code) | — |

---

## 6. Redis key patterns

Redis is the pipeline's real-time layer — anything written here is read
directly by the live dashboard or app backend, with no Postgres/BigQuery in
the loop. Two Redis instances are in play: the main instance
(`redisHost`/`redisPort`) and a separate "karma points" instance
(`redisKpHost`), both wrapped by `dfutil/utils/redis.py`.

| Key pattern | Written by | Shape | Purpose |
|---|---|---|---|
| `dashboard_*` (50+ keys) | `dashboardSync.py` | scalar / hash / DataFrame-as-hash | Every real-time counter and leaderboard on the admin/learner dashboard |
| `dashboard_*_updated_format` (~15 keys) | `dsrComputation.py` | scalar strings, Indian-numeral-formatted | Daily Summary Report KPI cards |
| `dashboard_rolled_up_*` (4 hashes) | `ministryMetrics.py` | hash, field=`ministryID` | Ministry/department/org rollup counters |
| `dashboard_all_course_badge_count_last_month_diff` + 6 more | `gamificationJob.py` | hash / list-of-map | Badge KPI cards (matches `gamification/prerequisites.md` spec exactly) |
| `lhp_certifications`, `lhp_learningHours` | `nationalLearningWeek.py` | hash, merge mode | Campaign daily ticker — Redis itself is the yesterday/today rollover memory |
| `odcs_course_recomendation` *(typo preserved from source)* | `odcsRecommendation.py` | hash, field=`mdo_id` | "Recommended for your org" content carousel |
| `user:{user_id}` | `userDataToRedis.py` | JSON string | Per-user profile snapshot for fast API reads (⚠️ always DB 0 — see Known Issues) |
| `CB_EXT_{mdoid}_password` | `zipUpload.py` | string | The password for that org's report ZIP, read by the notification/portal layer |
| `external_course_count_current` | `dsrComputationUpdated.py` (deprecated) | string | Yesterday-count proxy for an external API with no timestamp filter |

---

## 7. Data quality / naming caveats worth remembering

- `NLW_CONTENT_CERTIFICATE_GENERATED_COUNT_PARQUET_FILE`,
  `NLW_CONTENT_LEARNING_HOURS_PARQUET_FILE`, `RATING_SELECT_PARQUET_FILE`,
  `PEER_VALIDATION_FORMS_PARQUET_FILE` and
  `PEER_VALIDATION_ELIGIBLE_USERS_PARQUET_FILE` are all defined in
  `ParquetFileConstants.py` but were not observed being read or written by
  any job in this branch — likely leftover from an earlier design or a
  feature that now reads its data a different way (e.g.
  `peerValidationEligibleUsers.py` reads survey forms live from
  Elasticsearch rather than from `PEER_VALIDATION_FORMS_PARQUET_FILE`).
- BigQuery's `event_details`/`event_enrolment_details` table names don't
  match Postgres's `events`/`events_enrolment` names for the same data —
  both are correct, they're just named differently per target system.
- `CLAPS_PARQUET_FILE` and `learner_stats` form a closed loop across runs:
  Stage 0 reads Postgres `learner_stats` into this cache, `weeklyClaps.py`
  reads the cache, updates the streak logic, and writes the result back to
  the same Postgres table — which Stage 0 will read fresh on the *next*
  pipeline run.
