import findspark

findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import time
from datetime import datetime, timedelta

sys.path.append(str(Path(__file__).resolve().parents[2]))

from jobs.default_config import create_config
from jobs.config import get_environment_config

# NOTE: adjust this import to match wherever druidDFOption actually lives in
# your project (mirrors the pattern from your MAU query snippet).
from dfutil.utils.utils import druidDFOption

import json

# Initialize Spark
# NOTE: the JDBC driver package MUST be added here, on the builder, before
# getOrCreate(). Setting PYSPARK_SUBMIT_ARGS or spark.jars.packages inside
# main() is too late - the JVM/gateway is already started at import time
# (this "spark = ..." line runs on import, before main() ever executes).
spark = SparkSession.builder \
    .appName("DSRValidationJob") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

print("✅ Spark Session initialized")

# ---------------------------------------------------------------------------
# CONFIG - tune without touching check logic
# ---------------------------------------------------------------------------

HISTORY_DAYS = 5
OUTPUT_FILE_NAME = "dsr_validation_report.txt"
PG_TABLE = "dsr_metrics_history"

OOM_SAFE_BAND = 0.20
OOM_REVIEW_BAND = 0.40
DRUID_DROP_RATIO = 0.10
REG_MAGNITUDE_REVIEW_BAND = 0.30
REG_MAGNITUDE_CRITICAL_BAND = 0.60
# Certificates-vs-completions correlation: certificates issued should track
# the day-over-day DELTA in completions (roughly 1 certificate per
# completion), not just move in the same direction.
CERT_COMPLETIONS_REVIEW_BAND = 0.30
CERT_COMPLETIONS_CRITICAL_BAND = 0.60

# Dept/Orgs onboarded: small day-to-day swings (up or down) are normal;
# only a jump of ~100s is suspicious.
DEPT_ORGS_SAFE_ABS_DELTA = 50
DEPT_ORGS_CRITICAL_ABS_DELTA = 100

# ---------------------------------------------------------------------------
# WFS (WorkFlowSummaryModel) CONFIG - hardcoded for now, no config object.
# ---------------------------------------------------------------------------

WFS_LOG_DIR = "/mount/data/analytics/scripts/logs"
WFS_TODAY_LOG_FILENAME = "joblog.log"
WFS_HISTORY_DAYS = 7
WFS_OUTPUT_FILE_NAME = "wfs_validation_report.txt"

# TODO: fill in your actual Druid router host, same as config.sparkDruidRouterHost
# in your MAU query example.
DRUID_ROUTER_HOST = "10.175.5.33"

DRUID_INPUT_DATASOURCE = "telemetry-events-syncts"
DRUID_OUTPUT_DATASOURCE = "summary-events"
DRUID_QUERY_LIMIT = 10000000

# Confirmed: with/without the +5:30 offset gives the SAME count for
# summary-events, so the offset isn't what's causing the input/output
# mismatch - it's a genuine discrepancy (or the two numbers aren't measuring
# exactly the same thing - e.g. the job's outputEvents count vs raw row
# count in summary-events may differ by definition, not by a timezone bug).
# Keeping the offset applied uniformly since it's confirmed correct for
# telemetry-events-syncts and harmless (no-op) for summary-events.
DRUID_USE_IST_OFFSET = True

# Day-over-day magnitude check (log value vs recent average). Symmetric -
# both sudden increase and sudden decrease are treated as equally suspicious.
WFS_OOM_SAFE_BAND = 0.20
WFS_OOM_REVIEW_BAND = 0.40

# Log value vs Druid count(*) - should be close, tighter tolerance than the
# day-over-day magnitude check since it's meant to be a near-exact match.
DRUID_MATCH_REVIEW_BAND = 0.05
DRUID_MATCH_CRITICAL_BAND = 0.15

# Platform/device split check - context_pdata_id values from telemetry-events-syncts
WEB_PDATA_IDS = ["prod.sunbird-cb-adminportal", "prod.sunbird-cb-orgportal", "prod.sunbird-cb-portal"]
ANDROID_PDATA_ID = "prod.karmayogi-mobile-android"
IOS_PDATA_ID = "prod.karmayogi-mobile-ios"

PLATFORM_SAFE_BAND = 0.20
PLATFORM_REVIEW_BAND = 0.40
# If a platform's count today falls below this fraction of its recent
# average, flag CRITICAL outright - a platform going near-zero (an app/SDK
# outage) can hide inside a healthy aggregate total.
PLATFORM_NEAR_ZERO_RATIO = 0.05

METRICS = [
    {"key": "central_depts",            "name": "Central Departments Onboarded",              "category": "Catalogue",     "check": "non_decreasing_critical"},
    {"key": "dept_orgs",                 "name": "Department/Organisations Onboarded",         "category": "Catalogue",     "check": "small_fluctuation_dept_orgs"},
    {"key": "states_uts",                "name": "States/UTs Onboarded",                       "category": "Catalogue",     "check": "non_decreasing_critical"},
    {"key": "course_publishers",         "name": "Course Publishers",                          "category": "Catalogue",     "check": "non_decreasing_critical"},
    {"key": "courses_published",         "name": "Courses Published",                          "category": "Catalogue",     "check": "non_decreasing_critical"},
    {"key": "course_duration_hours",     "name": "Duration of Courses Published (Hours)",      "category": "Catalogue",     "check": "duration_alignment"},
    {"key": "course_enrolments",         "name": "Course Enrolments",                          "category": "Enrolment",     "check": "strictly_increasing"},
    {"key": "course_completions",        "name": "Course Completions",                         "category": "Enrolment",     "check": "strictly_increasing"},
    {"key": "event_enrolments",          "name": "Event Enrolments",                           "category": "Enrolment",     "check": "strictly_increasing"},
    {"key": "event_completions",         "name": "Event Completions",                          "category": "Enrolment",     "check": "strictly_increasing"},
    {"key": "users_registered_total",    "name": "Users Registered (Total)",                   "category": "Registration",  "check": "non_decreasing_critical"},
    {"key": "new_user_registrations",    "name": "New User Registrations Yesterday",           "category": "Registration",  "check": "registration_cross_check"},
    {"key": "users_enrolled_one_course", "name": "Users Enrolled in at least one course",      "category": "Catalogue",     "check": "non_decreasing_critical"},
    {"key": "certificates_issued",       "name": "Certificates Issued Yesterday",              "category": "Dynamic",       "check": "certificates_correlate_with_completions"},
    {"key": "users_logged_in",           "name": "Users Logged In Yesterday",                  "category": "Dynamic",       "check": "order_of_magnitude_druid"},
    {"key": "mau",                       "name": "Monthly Active Users",                       "category": "Dynamic",       "check": "order_of_magnitude_symmetric"},
]

SAFE, REVIEW, CRITICAL = "SAFE", "NEEDS REVIEW", "CRITICAL"
SEVERITY_ORDER = {SAFE: 0, REVIEW: 1, CRITICAL: 2}


def worse(status_a, status_b):
    return status_a if SEVERITY_ORDER[status_a] >= SEVERITY_ORDER[status_b] else status_b


def recent_average(history, key, days=5):
    vals = [h[key] for h in history[-days:] if key in h and h[key] is not None]
    return sum(vals) / len(vals) if vals else None


# ---------------------------------------------------------------------------
# CHECK FUNCTIONS - each returns (status, message)
# ---------------------------------------------------------------------------

def check_non_decreasing_critical(key, today_values, yesterday, history):
    today = today_values[key]
    if yesterday is None or key not in yesterday:
        return REVIEW, "No prior-day value found to compare against."
    y = yesterday[key]
    if today >= y:
        delta = today - y
        if delta == 0:
            return SAFE, "Unchanged from yesterday - acceptable for a cumulative count."
        return SAFE, f"Increased by {delta:,.0f} vs yesterday."
    return CRITICAL, f"Decreased from {y:,.0f} to {today:,.0f} - cumulative counts should never drop. Likely data error."


def check_strictly_increasing(key, today_values, yesterday, history):
    today = today_values[key]
    if yesterday is None or key not in yesterday:
        return REVIEW, "No prior-day value found to compare against."
    y = yesterday[key]
    if today > y:
        pct = (today - y) / y * 100 if y else 0
        return SAFE, f"Increased by {today - y:,.0f} ({pct:.2f}%) vs yesterday - as expected for a running total."
    return CRITICAL, f"Did not strictly increase (today {today:,.0f} vs yesterday {y:,.0f}). Enrolment/completion totals must always grow - likely data error."


def check_duration_alignment(key, today_values, yesterday, history):
    today = today_values[key]
    courses_today = today_values.get("courses_published")
    if yesterday is None or key not in yesterday or "courses_published" not in yesterday:
        return REVIEW, "No prior-day value found to compare against."
    y = yesterday[key]
    courses_y = yesterday["courses_published"]
    courses_increased = courses_today is not None and courses_today > courses_y
    if courses_increased:
        if today > y:
            return SAFE, f"Courses published increased, hours also increased ({y:,.0f} -> {today:,.0f}) - consistent."
        return CRITICAL, f"Courses published increased but duration hours did not ({y:,.0f} -> {today:,.0f}) - inconsistent, likely data error."
    else:
        if today == y:
            return SAFE, "Course count unchanged and hours match yesterday - consistent."
        return REVIEW, f"Course count unchanged but hours changed ({y:,.0f} -> {today:,.0f}) - worth a manual look."


def check_registration_cross_check(key, today_values, yesterday, history):
    new_regs = today_values[key]
    total_today = today_values.get("users_registered_total")
    if yesterday is None or "users_registered_total" not in yesterday:
        return REVIEW, "No prior-day value found to compare against."
    total_yesterday = yesterday["users_registered_total"]
    total_delta = (total_today - total_yesterday) if total_today is not None else None
    total_increased = total_delta is not None and total_delta > 0

    if total_increased and new_regs <= 0:
        return CRITICAL, "Users Registered (total) went up but New User Registrations Yesterday is zero/negative - the two figures disagree."
    if (not total_increased) and new_regs > 0:
        return CRITICAL, "New User Registrations Yesterday is positive but Users Registered (total) did not increase - the two figures disagree."

    if total_increased and new_regs > 0:
        diff_ratio = abs(total_delta - new_regs) / new_regs
        if diff_ratio > REG_MAGNITUDE_CRITICAL_BAND:
            return CRITICAL, f"Total registered users grew by {total_delta:,.0f}, but New User Registrations Yesterday was {new_regs:,.0f} - magnitudes disagree by {diff_ratio*100:.0f}%."
        if diff_ratio > REG_MAGNITUDE_REVIEW_BAND:
            return REVIEW, f"Total registered users grew by {total_delta:,.0f} vs New User Registrations Yesterday of {new_regs:,.0f} - a {diff_ratio*100:.0f}% gap, worth a glance."

    avg = recent_average(history, key)
    if avg and avg > 0:
        deviation = abs(new_regs - avg) / avg
        if deviation > OOM_REVIEW_BAND:
            return REVIEW, f"Cross-check with total is consistent, but today's value ({new_regs:,.0f}) is well outside the last-{HISTORY_DAYS}-day average ({avg:,.0f})."
    return SAFE, f"Consistent with total registered users (delta {total_delta:,.0f} vs {new_regs:,.0f} new registrations), and in line with recent daily volumes."


def check_small_fluctuation(key, today_values, yesterday, history, safe_delta, critical_delta):
    """For metrics where small day-to-day moves (up or down) are routine, and
    only a large jump (in either direction) is suspicious - unlike a strict
    cumulative count, this one is allowed to dip a little."""
    today = today_values[key]
    if yesterday is None or key not in yesterday:
        return REVIEW, "No prior-day value found to compare against."
    y = yesterday[key]
    delta = today - y
    abs_delta = abs(delta)
    direction = "increased" if delta > 0 else ("decreased" if delta < 0 else "unchanged")

    if abs_delta <= safe_delta:
        return SAFE, f"{direction.capitalize()} by {delta:+,.0f} vs yesterday - small day-to-day movement, within normal range."
    elif abs_delta <= critical_delta:
        return REVIEW, f"{direction.capitalize()} by {delta:+,.0f} vs yesterday - larger than the usual day-to-day movement, worth a glance."
    else:
        return CRITICAL, f"{direction.capitalize()} by {delta:+,.0f} vs yesterday - a swing of this size is unusual for this metric, likely a data error."


def check_order_of_magnitude(key, today_values, yesterday, history, druid=False, downside_only_critical=False):
    """downside_only_critical=True: an unusually HIGH value (more activity -
    more logins, more certificates, etc.) is capped at NEEDS REVIEW, never
    CRITICAL, since a spike is rarely a data error. An unusually LOW value
    can still be CRITICAL, since drops are the pattern that actually
    indicates a pipeline/data problem."""
    today = today_values[key]
    avg = recent_average(history, key)
    if avg is None or avg == 0:
        return REVIEW, "Not enough history yet to establish a baseline range."
    deviation = abs(today - avg) / avg
    is_increase = today > avg

    if druid and today < avg * DRUID_DROP_RATIO:
        return CRITICAL, f"Dropped to {today:,.0f} from a recent average of {avg:,.0f} - matches a Druid pipeline issue, not a real usage drop."

    if deviation <= OOM_SAFE_BAND:
        return SAFE, f"{today:,.0f} is within the normal range of the last few days (avg {avg:,.0f})."
    elif deviation <= OOM_REVIEW_BAND:
        return REVIEW, f"{today:,.0f} deviates {deviation*100:.0f}% from the recent average ({avg:,.0f}) - same order of magnitude, but worth a glance."
    else:
        if downside_only_critical and is_increase:
            return REVIEW, f"{today:,.0f} is {deviation*100:.0f}% above the recent average ({avg:,.0f}) - a jump, but on the increase side (more activity), so flagged for review rather than critical."
        return CRITICAL, f"{today:,.0f} deviates {deviation*100:.0f}% from the recent average ({avg:,.0f}) - outside expected order of magnitude."


def check_certificates_correlation(key, today_values, yesterday, history):
    """Certificates issued should be roughly the same size as the day-over-day
    DELTA in completions - e.g. if completions went from 13 to 20, that's 7
    new completions, so certificates issued should be somewhere near 7, not
    just 'also went up'. Runs alongside the normal order-of-magnitude range
    check and reports whichever is worse."""
    base_status, base_message = check_order_of_magnitude(key, today_values, yesterday, history, druid=False, downside_only_critical=True)

    today_cert = today_values[key]
    if yesterday is None or key not in yesterday:
        return base_status, base_message

    completions_today = (today_values.get("course_completions", 0) or 0) + (today_values.get("event_completions", 0) or 0)
    completions_yesterday = (yesterday.get("course_completions", 0) or 0) + (yesterday.get("event_completions", 0) or 0)
    completions_delta = completions_today - completions_yesterday

    if completions_delta <= 0:
        return base_status, base_message

    diff_ratio = abs(completions_delta - today_cert) / completions_delta

    if diff_ratio > CERT_COMPLETIONS_CRITICAL_BAND:
        corr_status = CRITICAL
        corr_message = (f"Completions grew by {completions_delta:,.0f} day-over-day, but Certificates Issued Yesterday "
                        f"was {today_cert:,.0f} - magnitudes disagree by {diff_ratio*100:.0f}%, well beyond the expected "
                        f"~1 certificate per completion.")
        return worse(base_status, corr_status), base_message + " ALSO: " + corr_message
    elif diff_ratio > CERT_COMPLETIONS_REVIEW_BAND:
        corr_status = REVIEW
        corr_message = (f"Completions grew by {completions_delta:,.0f} day-over-day vs Certificates Issued Yesterday "
                        f"of {today_cert:,.0f} - a {diff_ratio*100:.0f}% gap, worth a glance.")
        return worse(base_status, corr_status), base_message + " ALSO: " + corr_message

    return base_status, base_message + f" Also consistent with completions delta ({completions_delta:,.0f} new completions vs {today_cert:,.0f} certificates issued)."


CHECKS = {
    "non_decreasing_critical": check_non_decreasing_critical,
    "strictly_increasing": check_strictly_increasing,
    "duration_alignment": check_duration_alignment,
    "registration_cross_check": check_registration_cross_check,
    "certificates_correlate_with_completions": check_certificates_correlation,
    "small_fluctuation_dept_orgs": lambda k, t, y, h: check_small_fluctuation(k, t, y, h, DEPT_ORGS_SAFE_ABS_DELTA, DEPT_ORGS_CRITICAL_ABS_DELTA),
    "order_of_magnitude": lambda k, t, y, h: check_order_of_magnitude(k, t, y, h, druid=False, downside_only_critical=True),
    "order_of_magnitude_symmetric": lambda k, t, y, h: check_order_of_magnitude(k, t, y, h, druid=False, downside_only_critical=False),
    "order_of_magnitude_druid": lambda k, t, y, h: check_order_of_magnitude(k, t, y, h, druid=True, downside_only_critical=True),
}


def run_validation(today_values, history):
    yesterday = history[-1] if history else None
    results = []
    for m in METRICS:
        fn = CHECKS[m["check"]]
        status, message = fn(m["key"], today_values, yesterday, history)
        results.append({
            "key": m["key"], "name": m["name"], "category": m["category"],
            "status": status, "message": message, "value": today_values.get(m["key"]),
        })
    if any(r["status"] == CRITICAL for r in results):
        overall = CRITICAL
    elif any(r["status"] == REVIEW for r in results):
        overall = REVIEW
    else:
        overall = SAFE
    return results, overall


def format_report(results, overall, today_date):
    lines = []
    lines.append("=" * 70)
    lines.append(f"DSR VALIDATION REPORT - {today_date}  (generated {datetime.now().strftime('%Y-%m-%d %H:%M')})")
    lines.append(f"Compared against last {HISTORY_DAYS} day(s) of history")
    lines.append("=" * 70)

    banner = {
        SAFE: "ALL CLEAR - safe to trigger the 8 AM DSR.",
        REVIEW: "NEEDS REVIEW - a human should glance before triggering the 8 AM DSR.",
        CRITICAL: "DO NOT TRIGGER - critical issue(s) found. Notify the data team immediately.",
    }[overall]
    lines.append(banner)
    lines.append("")

    for label, status in [("SAFE", SAFE), ("NEEDS REVIEW", REVIEW), ("CRITICAL", CRITICAL)]:
        group = [r for r in results if r["status"] == status]
        if not group:
            continue
        lines.append(f"{label}  ({len(group)} metric{'s' if len(group) != 1 else ''})")
        lines.append("-" * 70)
        for r in group:
            val = f"{r['value']:,.0f}" if isinstance(r["value"], (int, float)) else str(r["value"])
            lines.append(f"  [{r['category']}] {r['name']}: {val}")
            lines.append(f"      -> {r['message']}")
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)


def write_report_to_file(results, overall, today_date, path):
    with open(path, "w") as f:
        f.write(format_report(results, overall, today_date))
    return path


def ensure_parent_dir(file_path):
    directory = os.path.dirname(file_path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def to_float(value):
    """Postgres NUMERIC columns come back via JDBC as decimal.Decimal, which
    doesn't mix with float arithmetic (e.g. Decimal * float raises TypeError).
    Cast everything to float right at the source so all check functions can
    do normal float math."""
    return None if value is None else float(value)


# ---------------------------------------------------------------------------
# WFS (WorkFlowSummaryModel) - LOG FILE PARSING
# ---------------------------------------------------------------------------

def wfs_get_log_filename(date_obj, today_date):
    if date_obj == today_date:
        return WFS_TODAY_LOG_FILENAME
    return f"joblog-{date_obj.strftime('%m-%d-%Y')}-1.log"


def wfs_parse_job_end(log_path):
    """Scans a log file for WorkFlowSummaryModel JOB_END entries and returns
    the LAST one found (in case of reruns), or None if no such entry exists
    or the file doesn't exist."""
    if not os.path.exists(log_path):
        return None

    last_entry = None
    with open(log_path, "r", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or '"JOB_END"' not in line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("eid") != "JOB_END":
                continue
            data = record.get("edata", {}).get("data", {})
            if data.get("model") != "WorkFlowSummaryModel":
                continue
            last_entry = {
                "status": record.get("edata", {}).get("status"),
                "inputEvents": data.get("inputEvents"),
                "outputEvents": data.get("outputEvents"),
                "date": data.get("date"),
                "timeTaken": data.get("timeTaken"),
            }
    return last_entry


def wfs_collect_data(log_dir, today_date, history_days):
    """Returns (today_entry, history). history is oldest -> newest, each
    entry tagged with 'log_date' (the calendar date its log file covers).
    Missing files (e.g. no run that day) are simply skipped."""
    today_path = os.path.join(log_dir, WFS_TODAY_LOG_FILENAME)
    today_entry = wfs_parse_job_end(today_path)
    if today_entry:
        today_entry["log_date"] = today_date.strftime("%Y-%m-%d")

    history = []
    for i in range(history_days, 0, -1):
        day = today_date - timedelta(days=i)
        path = os.path.join(log_dir, wfs_get_log_filename(day, today_date))
        entry = wfs_parse_job_end(path)
        if entry:
            entry["log_date"] = day.strftime("%Y-%m-%d")
            history.append(entry)
    return today_entry, history


def wfs_recent_average(history, key, days=5):
    vals = [h[key] for h in history[-days:] if h.get(key) is not None]
    return sum(vals) / len(vals) if vals else None


# ---------------------------------------------------------------------------
# WFS - CHECK FUNCTIONS
# ---------------------------------------------------------------------------

def wfs_check_job_status(entry):
    if entry is None:
        return CRITICAL, "No WorkFlowSummaryModel JOB_END entry found in today's log - job may not have run."
    status = entry.get("status")
    if status == "SUCCESS":
        return SAFE, "Job completed with status SUCCESS."
    return CRITICAL, f"Job did not complete successfully - status: {status}."


def wfs_check_events_magnitude(key, entry, history, safe_band=WFS_OOM_SAFE_BAND, review_band=WFS_OOM_REVIEW_BAND):
    today_val = entry.get(key)
    if today_val is None:
        return CRITICAL, f"{key} missing from today's JOB_END entry."
    avg = wfs_recent_average(history, key)
    if avg is None or avg == 0:
        return REVIEW, "Not enough history yet to establish a baseline range."
    deviation = abs(today_val - avg) / avg
    direction = "above" if today_val > avg else "below"
    if deviation <= safe_band:
        return SAFE, f"{today_val:,.0f} is within the normal range of the last {len(history)} day(s) (avg {avg:,.0f})."
    elif deviation <= review_band:
        return REVIEW, f"{today_val:,.0f} is {deviation*100:.0f}% {direction} the recent average ({avg:,.0f}) - worth a glance."
    else:
        return CRITICAL, f"{today_val:,.0f} is {deviation*100:.0f}% {direction} the recent average ({avg:,.0f}) - sudden jump/drop, outside expected range."


def wfs_build_day_count_query(datasource, date_str, use_ist_offset=True):
    """date_str: 'YYYY-MM-DD' - the calendar date the job processed.
    When use_ist_offset=True, mirrors the reference MAU query's pattern
    (TIME_FLOOR(<ts> + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')) to convert an
    IST calendar date's boundaries assuming __time is stored in UTC.
    When False, __time is assumed already in IST, so no offset is applied."""
    if use_ist_offset:
        return f"""SELECT COUNT(*) AS cnt
                FROM "{datasource}"
                WHERE __time >= TIME_FLOOR(TIMESTAMP '{date_str} 00:00:00' + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')
                  AND __time <  TIME_FLOOR(TIMESTAMP '{date_str} 00:00:00' + INTERVAL '1' DAY + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')"""
    else:
        return f"""SELECT COUNT(*) AS cnt
                FROM "{datasource}"
                WHERE __time >= TIMESTAMP '{date_str} 00:00:00'
                  AND __time <  TIMESTAMP '{date_str} 00:00:00' + INTERVAL '1' DAY"""


def wfs_query_druid_count(datasource, date_str, spark):
    query = wfs_build_day_count_query(datasource, date_str, use_ist_offset=DRUID_USE_IST_OFFSET)
    df = druidDFOption(query, DRUID_ROUTER_HOST, limit=DRUID_QUERY_LIMIT, spark=spark)
    if df is None:
        return None
    rows = df.collect()
    if not rows:
        return 0
    return int(rows[0]["cnt"])


def wfs_build_platform_split_query(datasource, date_str):
    """Groups raw input events by context_pdata_id for the given IST
    calendar date - same offset pattern as the day-count query."""
    return f"""SELECT context_pdata_id, COUNT(*) AS cnt
                FROM "{datasource}"
                WHERE __time >= TIME_FLOOR(TIMESTAMP '{date_str} 00:00:00' + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')
                  AND __time <  TIME_FLOOR(TIMESTAMP '{date_str} 00:00:00' + INTERVAL '1' DAY + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')
                GROUP BY context_pdata_id"""


def wfs_query_platform_split(date_str, spark):
    """Returns bucketed counts {web, android, ios, mobile, other, total} for
    the given date, or None if the Druid query fails."""
    query = wfs_build_platform_split_query(DRUID_INPUT_DATASOURCE, date_str)
    try:
        df = druidDFOption(query, DRUID_ROUTER_HOST, limit=DRUID_QUERY_LIMIT, spark=spark)
    except Exception:
        return None
    if df is None:
        return None
    rows = df.collect()
    pdata_counts = {row["context_pdata_id"]: int(row["cnt"]) for row in rows if row["context_pdata_id"] is not None}

    web = sum(pdata_counts.get(p, 0) for p in WEB_PDATA_IDS)
    android = pdata_counts.get(ANDROID_PDATA_ID, 0)
    ios = pdata_counts.get(IOS_PDATA_ID, 0)
    mobile = android + ios
    known_ids = set(WEB_PDATA_IDS) | {ANDROID_PDATA_ID, IOS_PDATA_ID}
    other = sum(v for k, v in pdata_counts.items() if k not in known_ids)

    return {"web": web, "android": android, "ios": ios, "mobile": mobile, "other": other, "total": web + mobile + other}


def wfs_check_platform_magnitude(label, today_val, history_vals, safe_band=PLATFORM_SAFE_BAND,
                                 review_band=PLATFORM_REVIEW_BAND, near_zero_ratio=PLATFORM_NEAR_ZERO_RATIO):
    if not history_vals:
        return REVIEW, f"{label}: {today_val:,.0f} - not enough history yet to establish a baseline."
    avg = sum(history_vals) / len(history_vals)
    if avg == 0:
        return REVIEW, f"{label}: {today_val:,.0f} - recent average is 0, can't establish a baseline."

    if today_val < avg * near_zero_ratio:
        return CRITICAL, f"{label}: {today_val:,.0f} vs recent average {avg:,.0f} - dropped to near zero, likely an app/SDK-side outage on this platform."

    deviation = abs(today_val - avg) / avg
    direction = "above" if today_val > avg else "below"
    if deviation <= safe_band:
        return SAFE, f"{label}: {today_val:,.0f} is within the normal range (avg {avg:,.0f})."
    elif deviation <= review_band:
        return REVIEW, f"{label}: {today_val:,.0f} is {deviation*100:.0f}% {direction} the recent average ({avg:,.0f}) - worth a glance."
    else:
        return CRITICAL, f"{label}: {today_val:,.0f} is {deviation*100:.0f}% {direction} the recent average ({avg:,.0f}) - outside expected range."


def wfs_run_platform_split_check(data_date, spark, history_days=WFS_HISTORY_DAYS):
    """Fetches today's + history days' platform split directly from Druid
    (not from the job log, since the split isn't in the JOB_END entry) and
    checks Web/Android/iOS each against their own recent average."""
    today_split = wfs_query_platform_split(data_date, spark)
    if today_split is None:
        return [{"category": "Platform Split", "name": "Device/Platform Split",
                 "status": REVIEW, "message": "Druid query for platform split failed or returned nothing - could not check."}]

    base_date = datetime.strptime(data_date, "%Y-%m-%d").date()
    history_splits = []
    for i in range(history_days, 0, -1):
        day_str = (base_date - timedelta(days=i)).strftime("%Y-%m-%d")
        split = wfs_query_platform_split(day_str, spark)
        if split:
            history_splits.append(split)

    results = []
    for key, label in [("web", "Web (portal) events"), ("android", "Android app events"), ("ios", "iOS app events")]:
        history_vals = [h[key] for h in history_splits]
        status, msg = wfs_check_platform_magnitude(label, today_split[key], history_vals)
        results.append({"category": "Platform Split", "name": label, "status": status, "message": msg})

    summary = (f"Today's split - Web: {today_split['web']:,.0f}, Android: {today_split['android']:,.0f}, "
               f"iOS: {today_split['ios']:,.0f}, Other: {today_split['other']:,.0f}, Total: {today_split['total']:,.0f}")
    results.append({"category": "Platform Split", "name": "Split Summary", "status": SAFE, "message": summary})

    return results


def wfs_check_druid_match(label, log_value, druid_count, review_band=DRUID_MATCH_REVIEW_BAND, critical_band=DRUID_MATCH_CRITICAL_BAND):
    if log_value is None:
        return REVIEW, f"{label}: no value from job log to compare."
    if druid_count is None:
        return REVIEW, f"{label}: Druid query failed or returned nothing - could not cross-check."
    if druid_count == 0:
        if log_value == 0:
            return SAFE, f"{label}: both log and Druid report 0."
        return CRITICAL, f"{label}: Druid count is 0 but log reports {log_value:,.0f}."
    diff_ratio = abs(log_value - druid_count) / druid_count
    direction = "higher than" if log_value > druid_count else "lower than"
    if diff_ratio <= review_band:
        return SAFE, f"{label}: log value {log_value:,.0f} matches Druid count {druid_count:,.0f} (diff {diff_ratio*100:.1f}%)."
    elif diff_ratio <= critical_band:
        return REVIEW, f"{label}: log value {log_value:,.0f} is {diff_ratio*100:.1f}% {direction} Druid count {druid_count:,.0f} - worth a glance."
    else:
        return CRITICAL, f"{label}: log value {log_value:,.0f} is {diff_ratio*100:.1f}% {direction} Druid count {druid_count:,.0f} - likely a data/pipeline issue."


def run_wfs_validation(spark, today_date=None):
    today_date = today_date or datetime.now().date()
    today_entry, history = wfs_collect_data(WFS_LOG_DIR, today_date, WFS_HISTORY_DAYS)

    results = []

    status, msg = wfs_check_job_status(today_entry)
    results.append({"category": "Job Status", "name": "WFS Job Status", "status": status, "message": msg})

    if today_entry is None:
        return results, CRITICAL, None

    for key, label in [("inputEvents", "Input Events (vs last N days)"), ("outputEvents", "Output Events (vs last N days)")]:
        status, msg = wfs_check_events_magnitude(key, today_entry, history)
        results.append({"category": "Day-over-day", "name": label, "status": status, "message": msg})

    data_date = today_entry.get("date") or today_entry.get("log_date")

    druid_input_count, druid_input_error = None, None
    try:
        druid_input_count = wfs_query_druid_count(DRUID_INPUT_DATASOURCE, data_date, spark)
    except Exception as e:
        druid_input_error = str(e)

    druid_output_count, druid_output_error = None, None
    try:
        druid_output_count = wfs_query_druid_count(DRUID_OUTPUT_DATASOURCE, data_date, spark)
    except Exception as e:
        druid_output_error = str(e)

    status, msg = wfs_check_druid_match(f"Input Events vs Druid ({DRUID_INPUT_DATASOURCE})", today_entry.get("inputEvents"), druid_input_count)
    if druid_input_error:
        msg += f" [Druid query error: {druid_input_error}]"
    results.append({"category": "Druid cross-check", "name": "Input Events vs Druid", "status": status, "message": msg})

    status, msg = wfs_check_druid_match(f"Output Events vs Druid ({DRUID_OUTPUT_DATASOURCE})", today_entry.get("outputEvents"), druid_output_count)
    if druid_output_error:
        msg += f" [Druid query error: {druid_output_error}]"
    results.append({"category": "Druid cross-check", "name": "Output Events vs Druid", "status": status, "message": msg})

    # Check 5: platform/device split (Web vs Android vs iOS) vs each platform's own history
    results.extend(wfs_run_platform_split_check(data_date, spark))

    if any(r["status"] == CRITICAL for r in results):
        overall = CRITICAL
    elif any(r["status"] == REVIEW for r in results):
        overall = REVIEW
    else:
        overall = SAFE

    return results, overall, data_date


def wfs_escalation_priority(results):
    """WFS never blocks the DSR trigger - this only determines how urgently
    the data team needs to look at it.
      URGENT   : raw events (input, telemetry-events-syncts) mismatch is
                 CRITICAL, OR a platform (Web/Android/iOS) split check is
                 CRITICAL. DSR reads from this same raw table, so either
                 case could mean DSR's own numbers are already short -
                 needs a same-day look even though DSR still triggers.
      STANDARD : output/summary events mismatch, or any other CRITICAL
                 finding (job status failure, day-over-day flag). Not
                 relevant to DSR (summary events feed other things), just
                 needs a normal-priority data team look if CRITICAL.
      NONE     : nothing above NEEDS REVIEW.
    """
    input_druid_critical = any(
        r["status"] == CRITICAL and r["name"] == "Input Events vs Druid" for r in results
    )
    platform_split_critical = any(
        r["status"] == CRITICAL and r["category"] == "Platform Split" for r in results
    )
    if input_druid_critical or platform_split_critical:
        return "URGENT"
    if any(r["status"] == CRITICAL for r in results):
        return "STANDARD"
    if any(r["status"] == REVIEW for r in results):
        return "STANDARD"
    return "NONE"


def format_wfs_section(results, overall, data_date):
    escalation = wfs_escalation_priority(results)
    lines = []
    lines.append("=" * 70)
    lines.append(f"WFS VALIDATION - data date {data_date}")
    lines.append(f"Compared against last {WFS_HISTORY_DAYS} day(s) of history")
    lines.append("=" * 70)

    banner = {
        SAFE: "ALL CLEAR - WFS job output looks healthy. Does not affect DSR trigger.",
        REVIEW: "NEEDS REVIEW - a human should glance at the flagged item(s). Does not affect DSR trigger.",
        CRITICAL: "CRITICAL finding(s) - does NOT block the DSR trigger, but needs data team analysis.",
    }[overall]
    lines.append(banner)
    if overall != SAFE:
        escalation_label = {
            "URGENT": "ESCALATION: URGENT - raw telemetry mismatch, DSR reads this same table, team needs to look immediately.",
            "STANDARD": "ESCALATION: STANDARD - not relevant to DSR, inform data team for normal-priority analysis.",
            "NONE": "",
        }[escalation]
        if escalation_label:
            lines.append(escalation_label)
    lines.append("")

    for label, status in [("SAFE", SAFE), ("NEEDS REVIEW", REVIEW), ("CRITICAL", CRITICAL)]:
        group = [r for r in results if r["status"] == status]
        if not group:
            continue
        lines.append(f"{label}  ({len(group)} check{'s' if len(group) != 1 else ''})")
        lines.append("-" * 70)
        for r in group:
            lines.append(f"  [{r['category']}] {r['name']}")
            lines.append(f"      -> {r['message']}")
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)


class DSRValidator:

    def __init__(self, spark):
        self.spark = spark

    def read_postgres_table(self, url: str, table: str, username: str, password: str):
        """Read data from PostgreSQL table"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

    def processData(self, config):
        """
        DSR Validation: fetch today + history from Postgres, validate, write output file
        """
        try:
            start_time = time.time()
            today_date = datetime.now().strftime("%Y-%m-%d")

            # Step 1: Fetch DSR metrics from Postgres
            print("📊 Step 1: Fetching DSR metrics from Postgres...")
            dwPostgresUrl = f"jdbc:postgresql://{config.dwPostgresHost}/{config.dwPostgresSchema}"
            dsr_metrics_df = self.read_postgres_table(
                dwPostgresUrl,
                PG_TABLE,
                config.dwPostgresUsername,
                config.dwPostgresCredential
            )
            print("✅ Step 1 Complete")

            # Step 2: Extract today's snapshot + history window
            print("🔎 Step 2: Extracting today's snapshot and history window...")
            metric_keys = [m["key"] for m in METRICS]
            cols = ["date"] + metric_keys

            today_row = dsr_metrics_df.filter(col("date") == today_date).select(*cols).limit(1).collect()
            if not today_row:
                raise ValueError(f"No row found in {PG_TABLE} for date {today_date}.")
            today_values = {k: to_float(today_row[0][k]) for k in metric_keys}

            history_rows = (
                dsr_metrics_df.filter(col("date") < today_date)
                .select(*cols)
                .orderBy(col("date").desc())
                .limit(HISTORY_DAYS)
                .collect()
            )
            history = [
                {**{k: to_float(row[k]) for k in metric_keys}, "date": str(row["date"])}
                for row in reversed(history_rows)
            ]
            print("✅ Step 2 Complete")

            # Step 3: Run validation checks
            print("✅ Step 3: Running validation checks...")
            results, overall = run_validation(today_values, history)
            print("✅ Step 3 Complete")

            duration = time.time() - start_time
            print(f"[INFO] DSR validation duration: {duration:.2f}s")

            return results, overall, today_date

        except Exception as e:
            print(f"\n❌ Error occurred: {str(e)}")
            raise

    def processWFSData(self):
        """
        WFS Validation: parse job logs, check status + magnitude, cross-check
        against Druid via druidDFOption.
        """
        try:
            start_time = time.time()
            today_date = datetime.now().date()

            print("📊 Step 1: Parsing WFS job logs and running checks...")
            results, overall, data_date = run_wfs_validation(self.spark, today_date=today_date)
            print("✅ Step 1 Complete")

            duration = time.time() - start_time
            print(f"[INFO] WFS validation duration: {duration:.2f}s")

            return results, overall, data_date

        except Exception as e:
            print(f"\n❌ Error occurred: {str(e)}")
            raise


COMBINED_OUTPUT_FILE_NAME = "combined_validation_report.txt"


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] Validation started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    validator = DSRValidator(spark)

    # Step 1: DSR validation runs first
    print("\n========== RUNNING DSR VALIDATION ==========")
    dsr_results, dsr_overall, dsr_date = validator.processData(config)
    dsr_section = format_report(dsr_results, dsr_overall, dsr_date)
    print(dsr_section)

    # Step 2: WFS validation runs second, only after DSR has completed
    print("\n========== RUNNING WFS VALIDATION ==========")
    wfs_results, wfs_overall, wfs_date = validator.processWFSData()
    wfs_section = format_wfs_section(wfs_results, wfs_overall, wfs_date)
    print(wfs_section)

    # Combined report is for visibility only. The DSR TRIGGER decision is
    # gated exclusively by DSR's own result - WFS findings never block it,
    # per SOP: WFS issues always go to the data team for analysis, but the
    # DSR still goes out for the day regardless of WFS status.
    combined_overall = worse(dsr_overall, wfs_overall)
    combined_text = (
        f"COMBINED VALIDATION REPORT (generated {datetime.now().strftime('%Y-%m-%d %H:%M')})\n"
        f"DSR trigger decision: {'DO NOT TRIGGER' if dsr_overall == CRITICAL else 'OK TO TRIGGER'} (gated by DSR checks only)\n"
        f"DSR status: {dsr_overall}  |  WFS status: {wfs_overall}\n\n"
        f"{dsr_section}\n\n{wfs_section}\n"
    )
    with open(COMBINED_OUTPUT_FILE_NAME, "w") as f:
        f.write(combined_text)
    print(f"\nCombined report written to {COMBINED_OUTPUT_FILE_NAME}")

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Validation completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

    # Only DSR's own CRITICAL result blocks the trigger. WFS CRITICAL findings
    # are surfaced above (with URGENT/STANDARD escalation) but never raise here.
    if dsr_overall == CRITICAL:
        raise SystemExit("DSR validation: CRITICAL issues found - do not trigger 8 AM DSR.")


if __name__ == "__main__":
    main()