# SPEC for gamification job for dashboard and reporting 

Design document BE 
- https://karmayogibharat.atlassian.net/wiki/spaces/TES/pages/826179586/Gamification+design+doc
- https://karmayogibharat.atlassian.net/wiki/spaces/TES/pages/832405505/Gamification+APIs

Figma design 
- https://www.figma.com/design/bKWBedEW6T9Q4i8ZA6cPK1/Gamification?node-id=267-4621&t=HNDSToeB4kpGx0LW-1


This document describes the workflow, data structures, and reporting metrics for the **Gamification Badge System**.

The gamification job calculates badge-related KPIs and pushes them to **Redis** for dashboard consumption.

---

# Architecture Overview

1. Course metadata stores **badge information**.
2. When a user completes a course, **badge details are issued and stored** in the user enrolment table.
3. Aggregation jobs compute **badge metrics for reporting**.
4. KPIs are pushed to **Redis** for dashboard consumption.
5. **Airflow DAG scheduling** ensures the job runs after the dashboard sync job.

---

# Job Scheduling

The gamification job must run **daily**.

## Requirements

- The job pushes **calculated KPIs into Redis**
- It must run **after the Dashboard Sync Job**
- An **Airflow DAG update** is required to maintain the execution order

## Execution Order

---

# Course Badge Metadata

Courses contain a **new map field** that stores badge information.

## Data Source

Badge information is stored in **Content Metadata**.

| Field | Source |
|------|------|
| Badge Metadata | Elasticsearch Index |
| Index Name | `badgeDetails_v1` |

---

# User Badge Awarding

When a user **completes a course**, a badge is issued.

This information is stored in the **user enrolment data** and aggregated for analytics.

## Storage Locations

- Data Warehouse  
- BigQuery (BQ)

## Table

`user_enrolments_v2`

## Badge Field Structure

This field stores **badge details awarded to a user**.

---

# SPV Dashboard Aggregations

Separate aggregations are performed for the **SPV Dashboard** to provide insights into badge performance.

---

## Metrics

### 1. Total Badges Created

| Metric | Type |
|------|------|
| `dashboard_all_course_badge_count` | integer |

**Description:**  
Total number of badges created across all courses.

---

### 2. Total Live Badges

| Metric | Type |
|------|------|
| `dashboard_live_course_badge_count` | integer |

**Description:**  
Total number of badges currently active.

---

### 3. Total Badges Awarded

| Metric | Type |
|------|------|
| `dashboard_total_badge_awarded_count` | integer |

**Description:**  
Total number of badges awarded to users till date.

---

### 4. Active Learners for Badge Courses

| Metric | Type |
|------|------|
| `dashboard_active_learners_for_badge_courses_count` | integer |

**Description:**  
Total number of active learners enrolled in courses that contain badges.

---

### 5. Badge Award Rate

**Formula**

| Metric | Type |
|------|------|
| `dashboard_badge_award_rate` | list<frozen<map<text,float>>> |

---

### 6. Badge Performance Rate

**Contains**

- Badge Name  
- Count of badges awarded  
- Badge award rate  

| Metric | Type |
|------|------|
| `dashboard_badge_performance_rate` | list<frozen<map<text,text>>> |

---

### 7. Top 10 Courses with Badges

**Contains**

- Course Name  
- Count of badges awarded  
- Badge award rate  

| Metric | Type |
|------|------|
| `dashboard_courses_with_badges` | list<frozen<map<text,text>>> |

---

# Summary

The **Gamification Job** enables badge analytics through:

- Course metadata badge configuration
- User badge issuance tracking
- Aggregated dashboard metrics
- Daily Airflow execution
- Redis-based KPI serving

This enables the **SPV Dashboard** to track badge creation, distribution, and performance across courses effectively.