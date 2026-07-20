#!/bin/bash

set -e

echo "🚀 STARTING WAREHOUSE LOAD (SCHEMA SAFE VERSION)"

readonly PROJECT_ROOT="/home/analytics/pyspark"

CONFIG_VALUES=$(python3 -c "
import sys
sys.path.insert(0, '${PROJECT_ROOT}')
from jobs.config import get_environment_config

cfg = get_environment_config()
print(cfg.get('dwPostgresHost'))
print(cfg.get('dwPostgresUsername'))
print(cfg.get('dwPostgresCredential'))
print(cfg.get('dwPostgresSchema'))
")

PG_HOST_PORT=$(echo "$CONFIG_VALUES" | sed -n '1p')
PG_USER=$(echo "$CONFIG_VALUES" | sed -n '2p')
PG_PASSWORD=$(echo "$CONFIG_VALUES" | sed -n '3p')
PG_DB=$(echo "$CONFIG_VALUES" | sed -n '4p')

# Split "host:port" into separate variables
if [[ "$PG_HOST_PORT" == *:* ]]; then
    PG_HOST="${PG_HOST_PORT%:*}"
    PG_PORT="${PG_HOST_PORT##*:}"
else
    PG_HOST="$PG_HOST_PORT"
    PG_PORT="5432"
fi

if [[ -z "$PG_HOST" || -z "$PG_PASSWORD" ]]; then
    echo "❌ Failed to load Postgres config from jobs/config.py"
    exit 1
fi

export PGPASSWORD="$PG_PASSWORD"
# Limits DuckDB to 8GB RAM to prevent OOM (Out of Memory) kills
export DUCKDB_MEMORY_LIMIT="20GB"

DATA="/home/analytics/pyspark/warehouse"

echo "🔗 Postgres: $PG_HOST:$PG_PORT/$PG_DB"

# =========================================================
# COMMON LOADER
# =========================================================
load_table () {
    TABLE=$1
    QUERY=$2

    echo "=============================="
    echo "🚀 Loading $TABLE"
    echo "=============================="
    duckdb -c "
        INSTALL postgres;
        LOAD postgres;

        ATTACH 'host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASSWORD'
        AS pg (TYPE postgres);

        TRUNCATE pg.$TABLE;

	INSERT INTO pg.$TABLE
        $QUERY;
    "

    echo "✅ Done $TABLE"
}

# =========================================================
# EXECUTION ORDER (Lightweight -> Heavyweight)
# =========================================================

# 1. USER DETAIL
load_table "user_detail" "
SELECT
    user_id,
    mdo_id,
    CAST(status AS INTEGER) AS status,
    CAST(no_of_karma_points AS INTEGER) AS no_of_karma_points_val,
    full_name,
    designation,
    email,
    phone_number,
    pincode,
    groups,
    tag,
    profile_status,
    user_registration_date,
    profile_last_updated_date,
    roles,
    gender,
    category,
    marked_as_not_my_user,
    is_verified_karmayogi,
    created_by_id,
    external_system,
    external_system_id,
    CAST(weekly_claps_day_before_yesterday AS INTEGER),
    CAST(total_event_learning_hours AS DOUBLE),
    CAST(total_content_learning_hours AS DOUBLE),
    CAST(total_learning_hours AS DOUBLE),
    employee_id,
    cadre,
    civil_service_type,
    civil_services,
    cadre_batch,
    is_on_central_deputation,
    is_from_organised_service_of_govt,
    data_last_generated_on,
    CAST(total_badges_earned AS BIGINT)
FROM read_parquet('${DATA}/user_detail/*.snappy.parquet')
"

# 2. CONTENT
load_table "content" "
SELECT
    content_id,
    content_provider_id,
    content_provider_name,
    content_name,
    content_type,
    batch_id,
    batch_name,
    batch_start_date,
    batch_end_date,
    content_duration,
    TRY_CAST(content_rating AS FLOAT) AS content_rating,
    last_published_on,
    content_retired_on,
    content_status,
    TRY_CAST(resource_count AS INTEGER) AS resource_count,
    TRY_CAST(total_certificates_issued AS INTEGER) AS total_certificates_issued,
    content_substatus,
    language,
    content_sub_type,
    scorm_flag,
    difficulty_level,
    data_last_generated_on
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY content_id) AS rn
    FROM read_parquet('${DATA}/content/*.snappy.parquet')
)
WHERE rn = 1
"

# 3. ASSESSMENT
load_table "assessment_detail" "
SELECT
    user_id,
    content_id,
    assessment_id,
    assessment_name,
    assessment_type,
    assessment_sub_type,
    assessment_duration,
    time_spent_by_the_user,
    completion_date,
    TRY_CAST(score_achieved AS FLOAT) AS score_achieved,
    TRY_CAST(overall_score AS FLOAT) AS overall_score,
    TRY_CAST(cut_off_percentage AS FLOAT) AS cut_off_percentage,
    TRY_CAST(total_question AS INTEGER) AS total_question,
    TRY_CAST(number_of_incorrect_responses AS INTEGER) AS number_of_incorrect_responses,
    TRY_CAST(number_of_retakes AS INTEGER) AS number_of_retakes,
    pass,
    data_last_generated_on
FROM read_parquet('${DATA}/assessment_detail/*.snappy.parquet')
WHERE content_id IS NOT NULL
"

# 4. BP ENROLMENTS
load_table "bp_enrolments" "
SELECT
    user_id,
    content_id,
    batch_id,
    batch_location,
    component_name,
    component_id,
    component_type,
    component_mode,
    component_status,
    component_duration,
    TRY_CAST(component_progress_percentage AS FLOAT) AS component_progress_percentage,
    TRY_CAST(component_completed_on AS DATE) AS component_completed_on,
    TRY_CAST(last_accessed_on AS DATE) AS last_accessed_on,
    TRY_CAST(offline_session_date AS DATE) AS offline_session_date,
    offline_session_start_time,
    offline_session_end_time,
    offline_attendance_status,
    instructors_name,
    program_coordinator_name,
    data_last_generated_on
FROM read_parquet('${DATA}/bp_enrolments/*.snappy.parquet')
WHERE content_id IS NOT NULL
  AND user_id IS NOT NULL
  AND batch_id IS NOT NULL
"

# 5. CONTENT RESOURCE
load_table "content_resource" "
SELECT *
FROM read_parquet('${DATA}/content_resource/*.snappy.parquet')
"

# 6. CB PLAN
load_table "cb_plan" "
SELECT *
FROM read_parquet('${DATA}/cb_plan/*.snappy.parquet')
"

# 7. ORG HIERARCHY
load_table "org_hierarchy" "
SELECT *
FROM read_parquet('${DATA}/org_hierarchy/*.snappy.parquet')
"

# 8. KCM CONTENT
load_table "kcm_content_mapping" "
SELECT
    course_id,
    competency_area_id,
    competency_theme_id,
    competency_sub_theme_id,
    data_last_generated_on
FROM read_parquet('${DATA}/kcm_content_mapping/*.snappy.parquet')
"

# 9. KCM DICTIONARY
load_table "kcm_dictionary" "
SELECT *
FROM read_parquet('${DATA}/kcm_dictionary/*.snappy.parquet')
"

# 10. EVENTS
load_table "events" "
SELECT *
FROM read_parquet('${DATA}/event_details/*.snappy.parquet')
"

# 11. EVENTS ENROLMENT
load_table "events_enrolment" "
SELECT *
FROM read_parquet('${DATA}/event_enrolment_details/*.snappy.parquet')
"

# 12. COURSE COMPLETION SURVEY
#load_table "course_completion_survey_details" "
#SELECT *
#FROM read_parquet('${DATA}/course_completion_survey_details/*.snappy.parquet')
#"

# 13. USER ENROLMENTS (MOVED TO LAST - 19GB)
load_table "user_enrolments" "
SELECT
    user_id,
    batch_id,
    content_id,
    enrolled_on,
    TRY_CAST(content_progress_percentage AS FLOAT) AS content_progress_percentage,
    TRY_CAST(resource_count_consumed AS INTEGER) AS resource_count_consumed,
    user_consumption_status,
    first_completed_on,
    first_certificate_generated_on,
    last_completed_on,
    last_certificate_generated_on,
    content_last_accessed_on,
    certificate_generated,
    TRY_CAST(number_of_certificate AS INTEGER) AS number_of_certificate,
    TRY_CAST(user_rating AS FLOAT) AS user_rating,
    certificate_id,
    CASE
        WHEN live_cbp_plan_mandate = TRUE THEN TRUE
        WHEN live_cbp_plan_mandate = FALSE THEN FALSE
        WHEN lower(CAST(live_cbp_plan_mandate AS VARCHAR)) IN ('true','1','yes') THEN TRUE
        WHEN lower(CAST(live_cbp_plan_mandate AS VARCHAR)) IN ('false','0','no') THEN FALSE
        ELSE NULL
    END AS live_cbp_plan_mandate,
    data_last_generated_on,
    TRY_CAST(karma_points AS DOUBLE) AS karma_points,
    badge_id
FROM read_parquet('${DATA}/user_enrolments/*.snappy.parquet')
WHERE content_id IS NOT NULL
"

echo "🎉 ALL TABLES LOADED SUCCESSFULLY"
