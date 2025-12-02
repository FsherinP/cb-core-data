from constants.ParquetFileConstants import ParquetFileConstants
from datetime import datetime, timedelta, timezone
import pytz


class QueryConstants:
    """
    Complete Query Constants for Dashboard Sync
    CORRECTED VERSION - Uses proper JOINs between warehouse tables

    Schema:
    - user_warehouse_computed: user_id, mdo_id, status, full_name, designation, etc.
    - content_warehouse_computed: content_id, content_provider_id, content_name, content_status, content_type, etc.
    - enrolment_warehouse_computed: userID, content_id, user_consumption_status, enrolled_on, first_completed_on, certificateID
    - org_hierarchy: orgID, orgName (for mdo_name mapping)
    """

    # ==================== DATE & TIME CALCULATIONS ====================
    currentDate = datetime.now().date()
    istOffset = timezone(timedelta(hours=5, minutes=30))
    previousDayStartTime = datetime.combine(currentDate - timedelta(days=1), datetime.min.time()).replace(
        tzinfo=istOffset)
    previousDayEndTime = (datetime.combine(currentDate, datetime.min.time()) - timedelta(seconds=1)).replace(
        tzinfo=istOffset)
    previousDayStartTimeString = previousDayStartTime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+0530"
    previousDayEndTimeString = previousDayEndTime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+0530"

    # Epoch calculations
    twentyFourHoursAgoEpochMillisTime = int(
        datetime.combine(currentDate - timedelta(days=1), datetime.min.time()).replace(tzinfo=istOffset).timestamp())
    previousDayEndEpochMillis = int((datetime.combine(currentDate, datetime.min.time()) - timedelta(seconds=1)).replace(
        tzinfo=istOffset).timestamp()) * 1000
    twentyFourHoursAgoEpochMillis = int(datetime.combine(currentDate - timedelta(days=1), datetime.min.time()).replace(
        tzinfo=istOffset).timestamp()) * 1000
    twelveMonthsAgoEpochMillis = int(
        datetime.combine(currentDate - timedelta(days=365), datetime.min.time()).replace(tzinfo=istOffset).timestamp())

    # Week range for certifications
    currentDayStart = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=istOffset)
    endOfCurrentDay = int((currentDayStart + timedelta(days=1)).timestamp())
    startOf7thDay = int((currentDayStart - timedelta(days=7)).timestamp())

    print(f"twentyFourHoursAgoEpochMillis: {twentyFourHoursAgoEpochMillis}")
    print(f"twelveMonthsAgoEpochMillis: {twelveMonthsAgoEpochMillis}")
    print(f"previousDayEndEpochMillis: {previousDayEndEpochMillis}")
    print(f"startOf7thDay: {startOf7thDay}")
    print(f"endOfCurrentDay: {endOfCurrentDay}")

    @staticmethod
    def get_epoch_for_ist_datetime(date_str):
        """Convert IST datetime string to epoch seconds"""
        ist = pytz.timezone('Asia/Kolkata')
        dt = datetime.strptime(date_str.strip("'"), '%Y-%m-%d %H:%M:%S')
        dt_ist = ist.localize(dt)
        return int(dt_ist.timestamp())

    # NLW dates - Update these with your actual dates
    NLW_START_DATE = "'2024-01-15 00:00:00'"
    NLW_END_DATE = "'2024-01-21 23:59:59'"
    nlw_start_epoch = get_epoch_for_ist_datetime(NLW_START_DATE)
    nlw_end_epoch = get_epoch_for_ist_datetime(NLW_END_DATE)

    # ==================== EXISTING QUERIES (PRESERVED - NO CHANGES NEEDED) ====================

    ORG_BASED_DESIGNATION_LIST = f"""
        SELECT  
            userOrgID,  
            STRING_AGG(DISTINCT COALESCE(designation, professionalDetails.designation)) as org_designations  
        FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet')  
        WHERE COALESCE(designation, professionalDetails.designation) IS NOT NULL  
        GROUP BY userOrgID
    """

    ORG_USER_COUNT_DATAFRAME_QUERY = f"""
        SELECT 
            userOrgID AS orgID,
            userOrgName AS orgName,
            COUNT(userID) AS registeredCount,
            10000 AS totalCount
        FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet')
        WHERE userOrgID IS NOT NULL 
        AND userStatus = 1
        AND userOrgStatus = 1
        GROUP BY userOrgID, userOrgName
        ORDER BY registeredCount DESC
    """

    TOP_10_LEARNERS_BY_MDO_QUERY = f"""
        WITH ranked_users AS (
            SELECT
                *,
                RANK() OVER (PARTITION BY userOrgID ORDER BY total_points DESC) AS rank
            FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet')
            WHERE total_points IS NOT NULL
        ),
        top10_by_org AS (
            SELECT * FROM ranked_users WHERE rank <= 10
        ),
        json_ready AS (
            SELECT
                userOrgID,
                FORMAT(
                    '{{{{"userID":"{{}}", "fullName":"{{}}", "userOrgName":"{{}}", "designation":"{{}}", "userProfileImgUrl":{{}}, "total_points":{{}}, "rank":{{}}}}}}',
                    userID,
                    fullName,
                    userOrgName,
                    COALESCE(designation, ''),
                    CASE WHEN userProfileImgUrl IS NULL THEN 'null' ELSE '"' || userProfileImgUrl || '"' END,
                    total_points,
                    rank
                ) AS json_details_str
            FROM top10_by_org
        )
        SELECT
            userOrgID,
            json_object('top_learners', list(json_details_str)) AS top_learners
        FROM json_ready
        GROUP BY userOrgID
    """

    ORG_BASED_MDO_ADMIN_COUNT = f"""
        WITH exploded_roles AS (
            SELECT 
                userID,
                userOrgID,
                TRIM(unnested_role.unnest) AS role
            FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet'),
                UNNEST(STRING_SPLIT(role, ',')) AS unnested_role
            WHERE userStatus = 1 AND userOrgStatus = 1
        ),
        filtered_roles AS (
            SELECT DISTINCT userOrgID
            FROM exploded_roles
            WHERE role = 'MDO_ADMIN'
        )
        SELECT COUNT(*) AS org_with_admin_count
        FROM filtered_roles
    """

    USER_REGISTERED_YESTERDAY = f"""
        SELECT count(*) as count
        FROM read_parquet('{ParquetFileConstants.USER_COMPUTED_PARQUET_FILE}/**.parquet')
        WHERE userStatus = 1   
        AND userCreatedTimestamp >= extract(epoch from date_trunc('day', current_timestamp - interval '1 day')) * 1000
        AND userCreatedTimestamp < extract(epoch from date_trunc('day', current_timestamp)) * 1000
    """

    COURSE_COUNT_BY_STATUS_GROUP_BY_ORG = f"""
        SELECT 
            main.courseOrgID,
            main.category,
            COUNT(*) AS totalCourseCount,
            COUNT(CASE WHEN main.courseStatus = 'Live' THEN 1 END) AS liveCourseCount,
            COUNT(CASE WHEN LOWER(main.courseStatus) = 'draft' THEN 1 END) AS draftCourseCount,
            COUNT(CASE WHEN main.courseStatus = 'Review' THEN 1 END) AS reviewCourseCount,
            COUNT(CASE WHEN main.courseStatus = 'Retired' THEN 1 END) AS retiredCourseCount,
            COUNT(CASE WHEN main.courseStatus = 'Review' AND main.courseReviewStatus = 'Reviewed' THEN 1 END) AS pendingPublishCourseCount,
            AVG(ratings.ratingAverage) AS avgRating
        FROM 
            read_parquet('{ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE}/**.parquet') AS main
        LEFT JOIN 
            (
                SELECT 
                    courseID,
                    LOWER(category) AS categoryLower,
                    ratingAverage
                FROM 
                    read_parquet('{ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE}/**.parquet')
            ) AS ratings
        ON 
            main.courseID = ratings.courseID AND LOWER(main.category) = ratings.categoryLower
        GROUP BY 
            main.courseOrgID, main.category
        ORDER BY 
            totalCourseCount DESC
    """

    # ==================== CORRECTED BASE DATA WITH WAREHOUSE JOINS ====================

    BASE_DATA_COMPLETE = f"""
    WITH base_data AS (
        SELECT 
            e.userID,
            e.content_id as courseID,
            e.user_consumption_status,
            e.certificateID,
            e.enrolled_on,
            e.first_completed_on,
            e.batchID,

            -- User fields
            u.user_id,
            u.mdo_id as userOrgID,
            u.status as userStatus,
            u.full_name,
            u.designation,
            u.email,

            -- Org fields
            o.orgName as userOrgName,

            -- Content fields
            c.content_id,
            c.content_provider_id as courseOrgID,
            c.content_provider_name as courseOrgName,
            c.content_name as courseName,
            c.content_type as category,
            c.content_status as courseStatus,
            c.content_duration,
            c.content_rating,
            c.total_certificates_issued as issuedCertificateCount,

            -- Derived completion status (map to dbCompletionStatus)
            CASE 
                WHEN e.user_consumption_status = 'not-started' THEN 0
                WHEN e.user_consumption_status = 'in progress' THEN 1
                WHEN e.user_consumption_status = 'completed' THEN 2
                ELSE -1
            END AS dbCompletionStatus,

            -- Timestamps (convert string 'yyyy-mm-dd hh:mm:ss' to epoch)
            CASE 
                WHEN e.enrolled_on IS NOT NULL AND e.enrolled_on != '' 
                THEN epoch(strptime(e.enrolled_on, '%Y-%m-%d %H:%M:%S'))
                ELSE NULL
            END as courseEnrolledTimestamp,
            CASE 
                WHEN e.first_completed_on IS NOT NULL AND e.first_completed_on != '' 
                THEN epoch(strptime(e.first_completed_on, '%Y-%m-%d %H:%M:%S'))
                ELSE NULL
            END as courseCompletedTimestamp,

            -- Certificate generated on (use first_completed_on when certificate exists)
            CASE 
                WHEN e.certificateID IS NOT NULL AND e.certificateID != '' 
                THEN e.first_completed_on
                ELSE NULL
            END as certificateGeneratedOn,

            -- Filter categories
            CASE 
                WHEN c.content_status IN ('Live', 'Retired') AND u.status = 1 THEN 'live_retired_content'
                ELSE 'other'
            END AS live_retired_content_eligible,

            CASE 
                WHEN c.content_type IN ('Course', 'Program', 'Blended Program', 'CuratedCollections', 'Curated Program') 
                    AND c.content_status IN ('Live', 'Retired') 
                    AND u.status = 1 THEN 'live_retired_enrolment'
                ELSE 'other'
            END AS live_retired_enrolment_eligible,

            CASE 
                WHEN c.content_type = 'Course' 
                    AND c.content_status IN ('Live', 'Retired') 
                    AND u.mdo_id IS NOT NULL THEN 'live_retired_course'
                ELSE 'other'
            END AS live_retired_course_eligible,

            CASE 
                WHEN c.content_type IN ('Course', 'Program') 
                    AND c.content_status IN ('Live', 'Retired') 
                    AND u.status = 1 THEN 'live_retired_course_program'
                ELSE 'other'
            END AS live_retired_course_program_eligible,

            CASE 
                WHEN c.content_type IN ('Course', 'Program', 'Blended Program', 'CuratedCollections', 'Standalone Assessment', 'Curated Program') 
                    AND c.content_status IN ('Live', 'Retired') 
                    AND u.status = 1 THEN 'live_retired_course_program_excluding_moderated'
                ELSE 'other'
            END AS live_retired_course_program_excluding_moderated_eligible,

            CASE 
                WHEN c.content_type IN ('Course', 'Moderated Course') 
                    AND c.content_status IN ('Live', 'Retired') 
                    AND u.status = 1 THEN 'live_retired_course_moderated'
                ELSE 'other'
            END AS live_retired_course_moderated_eligible,

            CASE 
                WHEN e.user_consumption_status = 'not-started' THEN 'not_started'
                WHEN e.user_consumption_status = 'in progress' THEN 'in_progress' 
                WHEN e.user_consumption_status = 'completed' THEN 'completed'
                ELSE 'unknown'
            END AS completion_category,

            CASE 
                WHEN c.content_type IN ('Course', 'Program') 
                    AND c.content_status IN ('Live', 'Retired') 
                    AND u.status = 1 
                    AND e.user_consumption_status = 'completed'
                    AND e.first_completed_on IS NOT NULL
                    AND e.first_completed_on != ''
                    AND epoch(strptime(e.first_completed_on, '%Y-%m-%d %H:%M:%S')) >= {twentyFourHoursAgoEpochMillisTime} 
                THEN 'completed_yesterday'
                ELSE 'other'
            END AS completed_yesterday_category,

            CASE 
                WHEN c.content_type = 'Course' 
                    AND c.content_status IN ('Live', 'Retired') 
                    AND u.status = 1 
                    AND e.enrolled_on IS NOT NULL
                    AND e.enrolled_on != ''
                    AND epoch(strptime(e.enrolled_on, '%Y-%m-%d %H:%M:%S')) >= {twelveMonthsAgoEpochMillis}
                THEN 'enrolled_last_12_months'
                ELSE 'other'
            END AS enrolled_last_12_months_category,

            CASE 
                WHEN e.certificateID IS NOT NULL AND e.certificateID != '' THEN 'certificate_generated'
                ELSE 'no_certificate'
            END AS certificate_category,

            CASE 
                WHEN e.first_completed_on IS NULL OR e.first_completed_on = '' OR LENGTH(e.first_completed_on) = 0 THEN 0
                ELSE COALESCE(TRY_CAST(epoch(strptime(e.first_completed_on, '%Y-%m-%d %H:%M:%S')) AS BIGINT), 0)
            END AS epoch_seconds

        FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
        INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
            ON e.userID = u.user_id
        INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
            ON e.content_id = c.content_id
        LEFT JOIN read_parquet('{ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE}/**.parquet') o
            ON u.mdo_id = o.orgID
    )
    """

    # ==================== QUERIES USING BASE_DATA (NO CHANGES NEEDED - JUST USE UPDATED BASE) ====================

    OVERALL_METRICS = BASE_DATA_COMPLETE + f"""
    SELECT 
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as enrolment_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_program_eligible = 'live_retired_course_program' AND completion_category = 'completed') as landing_page_completed_count,
        COUNT(*) FILTER (WHERE completed_yesterday_category = 'completed_yesterday') as landing_page_completed_yesterday_count,
        COUNT(*) FILTER (WHERE live_retired_enrolment_eligible = 'live_retired_enrolment') as content_enrolment_count,
        COUNT(*) FILTER (WHERE live_retired_enrolment_eligible = 'live_retired_enrolment' AND completion_category = 'completed') as content_completed_count,
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content' AND completion_category = 'completed') as live_retired_content_completed_count,
        COUNT(*) FILTER (WHERE live_retired_enrolment_eligible = 'live_retired_enrolment'
                                AND completion_category = 'completed'
                                AND epoch_seconds > 0
                                AND epoch_seconds * 1000 >= {twentyFourHoursAgoEpochMillis} 
                                AND epoch_seconds * 1000 <= {previousDayEndEpochMillis}) as landing_page_content_completed_yesterday_count
    FROM base_data
    """

    EXTERNAL_CONTENT_METRICS = f"""
    SELECT 
        COUNT(*) as external_content_enrolment_count,
        COUNT(*) FILTER (WHERE status = 2) as external_content_completed_count
    FROM read_parquet('{ParquetFileConstants.EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE}')
    """

    LIVE_COURSE_PROGRAM_ENROLMENT_COUNTS = BASE_DATA_COMPLETE + """
    SELECT 
        courseID,
        COUNT(*) as enrolmentCount
    FROM base_data 
    WHERE live_retired_course_program_eligible = 'live_retired_course_program'
    GROUP BY courseID
    """

    MDO_WISE_COMPREHENSIVE = BASE_DATA_COMPLETE + f"""
    SELECT 
        userOrgID,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as course_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as course_enrolment_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content') as content_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_content_eligible = 'live_retired_content') as content_enrolment_unique_user_count,
        COUNT(*) FILTER (WHERE enrolled_last_12_months_category = 'enrolled_last_12_months') as enrolled_last_12_months_count,
        COUNT(DISTINCT userID) FILTER (WHERE enrolled_last_12_months_category = 'enrolled_last_12_months') as active_users_last_12_months,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_unique_user_count
    FROM base_data
    WHERE userOrgID IS NOT NULL
    GROUP BY userOrgID
    """

    CBP_WISE_COMPREHENSIVE = BASE_DATA_COMPLETE + """
    SELECT 
        courseOrgID,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as course_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as course_enrolment_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content' AND completion_category = 'completed') as content_completed_count,
        COUNT(*) FILTER (WHERE live_retired_course_moderated_eligible = 'live_retired_course_moderated') as course_moderated_course_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_moderated_eligible = 'live_retired_course_moderated') as course_moderated_course_enrolment_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content') as content_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_content_eligible = 'live_retired_content') as content_enrolment_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content' AND certificate_category = 'certificate_generated') as certificates_generated_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_content_eligible = 'live_retired_content' AND certificate_category = 'certificate_generated') as certificates_generated_unique_user_count,
        COUNT(*) FILTER (WHERE live_retired_course_moderated_eligible = 'live_retired_course_moderated' AND certificate_category = 'certificate_generated') as course_moderated_course_certificates_generated_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_moderated_eligible = 'live_retired_course_moderated' AND certificate_category = 'certificate_generated') as course_moderated_course_certificates_generated_unique_user_count
    FROM base_data
    WHERE courseOrgID IS NOT NULL
    GROUP BY courseOrgID
    """

    TOP_COURSES_BY_ORG = BASE_DATA_COMPLETE + """,
    course_counts AS (
        SELECT 
            courseOrgID,
            courseID,
            COUNT(*) as course_count
        FROM base_data
        WHERE live_retired_content_eligible = 'live_retired_content'
        GROUP BY courseOrgID, courseID
    ),
    ranked_courses AS (
        SELECT 
            courseOrgID,
            courseID,
            course_count,
            ROW_NUMBER() OVER (PARTITION BY courseOrgID ORDER BY course_count DESC) as rank
        FROM course_counts
    )
    SELECT 
        courseOrgID,
        STRING_AGG(courseID, ',' ORDER BY rank) as courseIDs
    FROM ranked_courses
    GROUP BY courseOrgID
    """

    # ==================== NEW QUERIES WITH WAREHOUSE JOINS ====================

    TOP_10_COURSES_PROGRAMS_ASSESSMENTS_COMBINED = BASE_DATA_COMPLETE + f""",
    live_retired_completed AS (
        SELECT 
            courseOrgID,
            courseID,
            category,
            userID
        FROM base_data
        WHERE live_retired_course_program_excluding_moderated_eligible = 'live_retired_course_program_excluding_moderated'
        AND completion_category = 'completed'
    ),
    course_stats AS (
        SELECT 
            courseOrgID,
            courseID,
            COUNT(DISTINCT userID) as user_enrolment_count,
            'courses' as content_type
        FROM live_retired_completed
        WHERE category = 'Course'
        GROUP BY courseOrgID, courseID
    ),
    program_stats AS (
        SELECT 
            courseOrgID,
            courseID,
            COUNT(DISTINCT userID) as user_enrolment_count,
            'programs' as content_type
        FROM live_retired_completed
        WHERE category = 'Program'
        GROUP BY courseOrgID, courseID
    ),
    assessment_stats AS (
        SELECT 
            courseOrgID,
            courseID,
            COUNT(DISTINCT userID) as user_enrolment_count,
            'assessments' as content_type
        FROM live_retired_completed
        WHERE category = 'Standalone Assessment'
        GROUP BY courseOrgID, courseID
    ),
    all_content AS (
        SELECT * FROM course_stats
        UNION ALL
        SELECT * FROM program_stats
        UNION ALL
        SELECT * FROM assessment_stats
    )
    SELECT 
        courseOrgID || ':' || content_type as courseOrgID_content,
        STRING_AGG(courseID, ',' ORDER BY user_enrolment_count DESC) as sorted_courseIDs
    FROM all_content
    GROUP BY courseOrgID, content_type
    """

    CERTIFICATES_GENERATED_BY_USER_ORG = BASE_DATA_COMPLETE + """
    SELECT 
        userOrgID,
        COUNT(*) as count,
        COUNT(DISTINCT userID) as uniqueUserCount
    FROM base_data
    WHERE live_retired_content_eligible = 'live_retired_content'
    AND certificate_category = 'certificate_generated'
    GROUP BY userOrgID
    """

    # ===== TRENDING QUERIES WITH DIRECT JOINS =====
    TRENDING_COURSES_BY_ORG = f"""
    WITH course_enrollments AS (
        SELECT 
            u.mdo_id as userOrgID,
            e.content_id as courseID,
            COUNT(*) as enrollment_count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
        INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
            ON e.userID = u.user_id
        INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
            ON e.content_id = c.content_id
        WHERE e.user_consumption_status IN ('not-started', 'in progress', 'completed')
        AND c.content_status = 'Live'
        AND c.content_type = 'Course'
        GROUP BY u.mdo_id, e.content_id
    ),
    ranked_courses AS (
        SELECT 
            userOrgID,
            courseID,
            enrollment_count,
            ROW_NUMBER() OVER (PARTITION BY userOrgID ORDER BY enrollment_count DESC) as rank
        FROM course_enrollments
    )
    SELECT 
        userOrgID || ':courses' as userOrgID_courses,
        STRING_AGG(courseID, ',' ORDER BY rank) as trendingCourseList
    FROM ranked_courses
    WHERE rank <= 50
    AND userOrgID IS NOT NULL
    AND userOrgID != ''
    GROUP BY userOrgID
    """

    TRENDING_PROGRAMS_BY_ORG = f"""
    WITH program_enrollments AS (
        SELECT 
            u.mdo_id as userOrgID,
            e.content_id as courseID,
            COUNT(*) as enrollment_count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
        INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
            ON e.userID = u.user_id
        INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
            ON e.content_id = c.content_id
        WHERE e.user_consumption_status IN ('not-started', 'in progress', 'completed')
        AND c.content_status = 'Live'
        AND c.content_type IN ('Blended Program', 'Curated Program')
        GROUP BY u.mdo_id, e.content_id
    ),
    ranked_programs AS (
        SELECT 
            userOrgID,
            courseID,
            enrollment_count,
            ROW_NUMBER() OVER (PARTITION BY userOrgID ORDER BY enrollment_count DESC) as rank
        FROM program_enrollments
    )
    SELECT 
        userOrgID || ':programs' as userOrgID_programs,
        STRING_AGG(courseID, ',' ORDER BY rank) as trendingProgramList
    FROM ranked_programs
    WHERE rank <= 50
    AND userOrgID IS NOT NULL
    AND userOrgID != ''
    GROUP BY userOrgID
    """

    MOST_ENROLLED_TAG = f"""
    WITH course_enrollments AS (
        SELECT 
            e.content_id as courseID,
            COUNT(*) as enrollment_count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
        INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
            ON e.content_id = c.content_id
        WHERE e.user_consumption_status IN ('not-started', 'in progress', 'completed')
        AND c.content_status = 'Live'
        AND c.content_type = 'Course'
        GROUP BY e.content_id
        ORDER BY enrollment_count DESC
    ),
    total_and_limit AS (
        SELECT CEIL(COUNT(*) * 0.1) as limit_count
        FROM course_enrollments
    )
    SELECT STRING_AGG(courseID, ',') as most_enrolled_tag
    FROM (
        SELECT courseID, enrollment_count
        FROM course_enrollments
        LIMIT (SELECT limit_count FROM total_and_limit)
    )
    """

    # ===== CERTIFICATION QUERIES WITH JOINS =====
    CERTIFICATIONS_TILL_TODAY = f"""
    SELECT COUNT(*) as total_certifications
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
    INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
        ON e.userID = u.user_id
    INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
        ON e.content_id = c.content_id
    WHERE c.content_status = 'Live'
    AND u.status = 1
    AND e.user_consumption_status = 'completed'
    AND e.certificateID IS NOT NULL
    AND e.certificateID != ''
    """

    CERTIFICATIONS_OF_THE_WEEK = f"""
    SELECT 
        e.content_id as courseID,
        u.mdo_id as userOrgID,
        COUNT(*) as courseCount
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
    INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
        ON e.userID = u.user_id
    INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
        ON e.content_id = c.content_id
    WHERE c.content_status = 'Live'
    AND u.status = 1
    AND e.first_completed_on IS NOT NULL
    AND e.first_completed_on != ''
    AND epoch(strptime(e.first_completed_on, '%Y-%m-%d %H:%M:%S')) > {startOf7thDay}
    AND epoch(strptime(e.first_completed_on, '%Y-%m-%d %H:%M:%S')) < {endOfCurrentDay}
    AND e.user_consumption_status = 'completed'
    AND e.certificateID IS NOT NULL
    AND e.certificateID != ''
    GROUP BY e.content_id, u.mdo_id
    """

    TOP_10_CERTIFICATIONS = f"""
    WITH certifications_of_week AS (
        SELECT courseID, COUNT(*) as courseCount
        FROM ({CERTIFICATIONS_OF_THE_WEEK})
        GROUP BY courseID
    )
    SELECT 
        STRING_AGG(courseID, ',' ORDER BY courseCount DESC) as course_ids
    FROM (
        SELECT courseID, courseCount
        FROM certifications_of_week
        ORDER BY courseCount DESC
        LIMIT 10
    )
    """

    TOP_CERTIFICATIONS_BY_MDO = f"""
    WITH certs_by_mdo AS (
        SELECT 
            userOrgID,
            courseID,
            courseCount,
            ROW_NUMBER() OVER (PARTITION BY userOrgID ORDER BY courseCount DESC) as rank
        FROM ({CERTIFICATIONS_OF_THE_WEEK})
    )
    SELECT 
        userOrgID || ':certifications' as userOrgID_certifications,
        STRING_AGG(courseID, ',' ORDER BY rank) as certifications
    FROM certs_by_mdo
    WHERE rank <= 10
    GROUP BY userOrgID
    LIMIT 10
    """

    # ===== TOP 5 QUERIES WITH JOINS =====
    TOP_5_USERS_BY_COMPLETION_BY_MDO = f"""
    WITH user_completion_stats AS (
        SELECT 
            e.userID,
            u.full_name as fullName,
            u.email as maskedEmail,
            u.mdo_id as userOrgID,
            o.orgName as userOrgName,
            COUNT(e.content_id) as completed_count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
        INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
            ON e.userID = u.user_id
        INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
            ON e.content_id = c.content_id
        LEFT JOIN read_parquet('{ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE}/**.parquet') o
            ON u.mdo_id = o.orgID
        WHERE c.content_type = 'Course'
        AND c.content_status IN ('Live', 'Retired')
        AND e.user_consumption_status = 'completed'
        AND u.status = 1
        GROUP BY e.userID, u.full_name, u.email, u.mdo_id, o.orgName
    ),
    ranked_users AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY userOrgID ORDER BY completed_count DESC) as rank
        FROM user_completion_stats
    )
    SELECT 
        userOrgID,
        JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'rank', rank,
                'userID', userID,
                'fullName', fullName,
                'maskedEmail', maskedEmail,
                'completed_count', completed_count
            )
        ) as jsonData
    FROM ranked_users
    WHERE rank <= 5
    GROUP BY userOrgID
    """

    TOP_5_COURSES_BY_COMPLETION_BY_MDO = f"""
    WITH course_completion_stats AS (
        SELECT 
            e.content_id as courseID,
            c.content_name as courseName,
            u.mdo_id as userOrgID,
            o.orgName as userOrgName,
            COUNT(e.userID) as enrolled_count,
            SUM(CASE WHEN e.user_consumption_status = 'not-started' THEN 1 ELSE 0 END) as not_started_count,
            SUM(CASE WHEN e.user_consumption_status = 'in progress' THEN 1 ELSE 0 END) as in_progress_count,
            SUM(CASE WHEN e.user_consumption_status = 'completed' THEN 1 ELSE 0 END) as completed_count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
        INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
            ON e.userID = u.user_id
        INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
            ON e.content_id = c.content_id
        LEFT JOIN read_parquet('{ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE}/**.parquet') o
            ON u.mdo_id = o.orgID
        WHERE c.content_type = 'Course'
        AND c.content_status IN ('Live', 'Retired')
        AND u.status = 1
        GROUP BY e.content_id, c.content_name, u.mdo_id, o.orgName
    ),
    ranked_courses AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY userOrgID ORDER BY completed_count DESC) as rank
        FROM course_completion_stats
    )
    SELECT 
        userOrgID,
        JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'rank', rank,
                'courseID', courseID,
                'courseName', courseName,
                'enrolled_count', enrolled_count,
                'not_started_count', not_started_count,
                'in_progress_count', in_progress_count,
                'completed_count', completed_count
            )
        ) as jsonData
    FROM ranked_courses
    WHERE rank <= 5
    GROUP BY userOrgID
    """

    TOP_5_CONTENT_BY_COMPLETION_BY_ORG = BASE_DATA_COMPLETE + f""",
    ranked_content AS (
        SELECT 
            courseID,
            courseName,
            courseOrgID,
            COUNT(userID) as enrolledCount,
            SUM(CASE WHEN dbCompletionStatus = 2 THEN 1 ELSE 0 END) as completedCount,
            ROW_NUMBER() OVER (PARTITION BY courseOrgID ORDER BY SUM(CASE WHEN dbCompletionStatus = 2 THEN 1 ELSE 0 END) DESC) as rowNum
        FROM base_data
        WHERE courseStatus IN ('Live', 'Retired')
        AND userStatus = 1
        GROUP BY courseID, courseName, courseOrgID
    )
    SELECT 
        courseOrgID,
        JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'rowNum', rowNum,
                'courseID', courseID,
                'courseName', courseName,
                'courseOrgID', courseOrgID,
                'completedCount', completedCount
            )
        ) as jsonData
    FROM ranked_content
    WHERE rowNum <= 5
    GROUP BY courseOrgID
    """

    TOP_5_CONTENT_BY_ENROLLMENTS_BY_CBP = BASE_DATA_COMPLETE + f""",
    content_enrollment_stats AS (
        SELECT 
            courseID,
            courseName,
            courseOrgID,
            COUNT(userID) as enrollment_count
        FROM base_data
        WHERE courseStatus IN ('Live', 'Retired')
        AND userStatus = 1
        GROUP BY courseID, courseName, courseOrgID
    ),
    ranked_content AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY courseOrgID ORDER BY enrollment_count DESC) as rank
        FROM content_enrollment_stats
    )
    SELECT 
        courseOrgID,
        JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'rank', rank,
                'courseID', courseID,
                'courseName', courseName,
                'enrollment_count', enrollment_count
            )
        ) as jsonData
    FROM ranked_content
    WHERE rank <= 5
    GROUP BY courseOrgID
    """

    TOP_5_COURSES_BY_RATING = f"""
    WITH course_ratings AS (
        SELECT 
            c.content_id as courseID,
            c.content_name as courseName,
            c.content_provider_name as courseOrgName,
            TRY_CAST(c.content_rating AS DOUBLE) as rating_average,
            COUNT(r.userID) as rating_count
        FROM read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
        LEFT JOIN read_parquet('{ParquetFileConstants.RATING_COMPUTED_PARQUET_FILE}/**.parquet') r
            ON c.content_id = r.courseID
        WHERE c.content_type = 'Course'
        AND c.content_status = 'Live'
        AND c.content_rating IS NOT NULL
        AND c.content_rating != ''
        AND TRY_CAST(c.content_rating AS DOUBLE) > 0
        AND TRY_CAST(c.content_rating AS DOUBLE) <= 5.0
        GROUP BY c.content_id, c.content_name, c.content_provider_name, c.content_rating
        HAVING COUNT(r.userID) > 0
    ),
    ranked_courses AS (
        SELECT 
            courseID,
            courseName,
            courseOrgName,
            ROUND(rating_average, 1) as rating_average,
            rating_count,
            (rating_count * rating_average) as rating_metric
        FROM course_ratings
        ORDER BY rating_metric DESC
        LIMIT 5
    )
    SELECT 
        JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'courseID', courseID,
                'courseName', courseName,
                'courseOrgName', courseOrgName,
                'rating_average', rating_average,
                'rating_count', rating_count
            )
        ) as jsonData
    FROM ranked_courses
    """

    # ===== TOP 5 CONTENT BY RATING BY ORG =====
    TOP_5_CONTENT_BY_RATING_BY_ORG = f"""
    WITH content_ratings AS (
        SELECT 
            c.content_provider_id as courseOrgID,
            c.content_id as courseID,
            c.content_name as courseName,
            TRY_CAST(c.content_rating AS DOUBLE) as rating_average,
            COUNT(r.userID) as rating_count
        FROM read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
        LEFT JOIN read_parquet('{ParquetFileConstants.RATING_COMPUTED_PARQUET_FILE}/**.parquet') r
            ON c.content_id = r.courseID
        WHERE c.content_status = 'Live'
        AND c.content_rating IS NOT NULL
        AND c.content_rating != ''
        AND TRY_CAST(c.content_rating AS DOUBLE) > 0
        AND TRY_CAST(c.content_rating AS DOUBLE) <= 5.0
        GROUP BY c.content_provider_id, c.content_id, c.content_name, c.content_rating
        HAVING COUNT(r.userID) > 0
    ),
    ranked_content AS (
        SELECT 
            courseOrgID,
            courseID,
            courseName,
            ROUND(rating_average, 1) as averageRating,
            rating_count as totalRatings,
            ROW_NUMBER() OVER (PARTITION BY courseOrgID ORDER BY rating_average DESC, rating_count DESC) as rank
        FROM content_ratings
    )
    SELECT 
        courseOrgID,
        JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'courseID', courseID,
                'courseName', courseName,
                'averageRating', averageRating,
                'totalRatings', totalRatings
            )
        ) as jsonData
    FROM ranked_content
    WHERE rank <= 5
    GROUP BY courseOrgID
    """

    # ===== TOTAL RATINGS BY ORG =====
    TOTAL_RATINGS_BY_ORG = f"""
    SELECT 
        c.content_provider_id as courseOrgID,
        COUNT(r.userID) as totalRatings
    FROM read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
    INNER JOIN read_parquet('{ParquetFileConstants.RATING_COMPUTED_PARQUET_FILE}/**.parquet') r
        ON c.content_id = r.courseID
    WHERE c.content_status = 'Live'
    GROUP BY c.content_provider_id
    """

    # ===== RATINGS SPREAD BY ORG =====
    RATINGS_SPREAD_BY_ORG = f"""
    SELECT 
        c.content_provider_id as courseOrgID,
        JSON_OBJECT(
            'count5', SUM(CASE WHEN r.userRating >= 4.5 THEN 1 ELSE 0 END),
            'count4', SUM(CASE WHEN r.userRating >= 3.5 AND r.userRating < 4.5 THEN 1 ELSE 0 END),
            'count3', SUM(CASE WHEN r.userRating >= 2.5 AND r.userRating < 3.5 THEN 1 ELSE 0 END),
            'count2', SUM(CASE WHEN r.userRating >= 1.5 AND r.userRating < 2.5 THEN 1 ELSE 0 END),
            'count1', SUM(CASE WHEN r.userRating < 1.5 THEN 1 ELSE 0 END)
        ) as jsonData
    FROM read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
    INNER JOIN read_parquet('{ParquetFileConstants.RATING_COMPUTED_PARQUET_FILE}/**.parquet') r
        ON c.content_id = r.courseID
    WHERE c.content_status = 'Live'
    GROUP BY c.content_provider_id
    """

    TOP_5_MDO_BY_COMPLETION = BASE_DATA_COMPLETE + f""",
    mdo_completion_stats AS (
        SELECT 
            userOrgID,
            userOrgName,
            COUNT(courseID) as completedCount
        FROM base_data
        WHERE category = 'Course'
        AND courseStatus IN ('Live', 'Retired')
        AND dbCompletionStatus = 2
        GROUP BY userOrgID, userOrgName
    )
    SELECT 
        userOrgID,
        userOrgName,
        completedCount
    FROM mdo_completion_stats
    ORDER BY completedCount DESC
    LIMIT 5
    """

    # ===== TOP 5 MDO BY LIVE COURSES =====
    TOP_5_MDO_BY_LIVE_COURSES = f"""
    WITH course_counts AS (
        SELECT 
            content_provider_id as courseOrgID,
            content_provider_name as courseOrgName,
            COUNT(content_id) as publishedCount
        FROM read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet')
        WHERE content_type = 'Course'
        AND content_status = 'Live'
        GROUP BY content_provider_id, content_provider_name
    )
    SELECT 
        JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'courseOrgID', courseOrgID,
                'courseOrgName', courseOrgName,
                'publishedCount', publishedCount
            )
        ) as jsonData
    FROM (
        SELECT * FROM course_counts
        ORDER BY publishedCount DESC
        LIMIT 5
    )
    """

    # ===== ADDITIONAL QUERIES (NEED RATING TABLE) =====
    TOP_10_REVIEWS_BY_ORG = f"""
    WITH reviews_with_course AS (
        SELECT 
            r.activityid as courseID,
            r.userid as userID,
            r.rating,
            r.review,
            c.courseOrgID
        FROM read_parquet('{ParquetFileConstants.RATING_PARQUET_FILE}') r
        INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE}/**.parquet') c
            ON r.activityid = c.courseID
        WHERE r.review IS NOT NULL
        AND r.rating >= 4.5
    ),
    ranked_reviews AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY courseOrgID ORDER BY rating DESC) as rank
        FROM reviews_with_course
    )
    SELECT 
        courseOrgID,
        JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'courseID', courseID,
                'userID', userID,
                'rating', rating,
                'review', review
            )
        ) as jsonData
    FROM ranked_reviews
    WHERE rank <= 10
    GROUP BY courseOrgID
    """

    # ===== TRENDING OVERALL =====
    TRENDING_COURSES_OVERALL = f"""
    WITH course_enrollments AS (
        SELECT 
            e.content_id as courseID,
            COUNT(*) as enrollment_count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
        INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
            ON e.content_id = c.content_id
        WHERE e.user_consumption_status IN ('not-started', 'in progress', 'completed')
        AND c.content_status = 'Live'
        AND c.content_type = 'Course'
        GROUP BY e.content_id
        ORDER BY enrollment_count DESC
    ),
    total_and_limit AS (
        SELECT 
            COUNT(*) as total_count,
            CEIL(COUNT(*) * 0.1) as limit_count
        FROM course_enrollments
    )
    SELECT 
        STRING_AGG(courseID, ',') as course_ids
    FROM course_enrollments ce
    CROSS JOIN total_and_limit t
    WHERE ROWID() <= t.limit_count
    """

    TRENDING_PROGRAMS_OVERALL = f"""
    WITH program_enrollments AS (
        SELECT 
            e.content_id as courseID,
            COUNT(*) as enrollment_count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
        INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
            ON e.content_id = c.content_id
        WHERE e.user_consumption_status IN ('not-started', 'in progress', 'completed')
        AND c.content_status = 'Live'
        AND c.content_type IN ('Blended Program', 'Curated Program')
        GROUP BY e.content_id
        ORDER BY enrollment_count DESC
    ),
    total_and_limit AS (
        SELECT 
            COUNT(*) as total_count,
            CEIL(COUNT(*) * 0.1) as limit_count
        FROM program_enrollments
    )
    SELECT 
        STRING_AGG(courseID, ',') as program_ids
    FROM program_enrollments pe
    CROSS JOIN total_and_limit t
    WHERE ROWID() <= t.limit_count
    """

    # ===== EVENT QUERIES (IF EVENT DATA EXISTS) =====
    TRENDING_EVENTS_BY_MDO = f"""
    WITH event_counts AS (
        SELECT 
            u.userOrgID,
            e.event_id,
            COUNT(*) as event_count,
            DENSE_RANK() OVER (PARTITION BY u.userOrgID ORDER BY COUNT(*) DESC) as rank
        FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') e
        JOIN read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet') u 
            ON e.user_id = u.userID
        JOIN read_parquet('{ParquetFileConstants.EVENT_PARQUET_FILE}') ed 
            ON e.event_id = ed.event_id
        WHERE ed.event_status = 'Live'
        GROUP BY u.userOrgID, e.event_id
    )
    SELECT 
        userOrgID,
        STRING_AGG(event_id, ',') as events
    FROM event_counts
    WHERE rank <= 100
    GROUP BY userOrgID
    """

    FEATURED_EVENTS_OVERALL = f"""
    WITH event_counts AS (
        SELECT 
            e.event_id,
            COUNT(*) as event_count
        FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') e
        JOIN read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet') u 
            ON e.user_id = u.userID
        JOIN read_parquet('{ParquetFileConstants.EVENT_PARQUET_FILE}') ed 
            ON e.event_id = ed.event_id
        WHERE ed.event_status = 'Live'
        GROUP BY e.event_id
        ORDER BY event_count DESC
        LIMIT 100
    )
    SELECT STRING_AGG(event_id, ',') as events
    FROM event_counts
    """

    # ===== CORE COMPETENCIES BY MDO =====
    CORE_COMPETENCIES_BY_MDO = BASE_DATA_COMPLETE + """,
    course_counts AS (
        SELECT 
            userOrgID,
            courseID,
            COUNT(*) as count
        FROM base_data
        WHERE courseStatus IN ('Live', 'Retired')
        AND userStatus = 1
        GROUP BY userOrgID, courseID
        ORDER BY count DESC
    )
    SELECT 
        userOrgID,
        STRING_AGG(courseID, ',') as courseIDs
    FROM course_counts
    GROUP BY userOrgID
    """

    # ===== COURSES COMPLETED AT LEAST ONCE BY MDO =====
    COURSES_COMPLETED_AT_LEAST_ONCE_BY_MDO = BASE_DATA_COMPLETE + """
    SELECT 
        userOrgID,
        COUNT(DISTINCT courseID) as count
    FROM base_data
    WHERE courseStatus = 'Live'
    AND category = 'Course'
    AND dbCompletionStatus = 2
    AND courseID IS NOT NULL
    AND courseID != ''
    GROUP BY userOrgID
    """

    # ===== NLW QUERIES =====
    NLW_EVENT_ENROLLMENTS = f"""
    SELECT COUNT(event_id) as event_count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') 
    WHERE enrolled_on_datetime >= {NLW_START_DATE} 
      AND enrolled_on_datetime <= {NLW_END_DATE}
    """

    NLW_CONTENT_ENROLLMENTS = f"""
    SELECT COUNT(*) as content_count
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
    INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
        ON e.userID = u.user_id
    INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
        ON e.content_id = c.content_id
    WHERE c.content_status IN ('Live', 'Retired')
    AND u.status = 1 
    AND e.enrolled_on IS NOT NULL
    AND e.enrolled_on != ''
    AND epoch(strptime(e.enrolled_on, '%Y-%m-%d %H:%M:%S')) >= {nlw_start_epoch}
    AND epoch(strptime(e.enrolled_on, '%Y-%m-%d %H:%M:%S')) <= {nlw_end_epoch}
    """

    TOTAL_EVENT_ENROLLMENTS = f"""
    SELECT COUNT(event_id) as total_event_count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}')
    """

    EVENTS_PUBLISHED_COUNT = f"""
    SELECT COUNT(DISTINCT event_id) as events_published_count
    FROM read_parquet('{ParquetFileConstants.EVENT_PARQUET_FILE}')
    """

    CONTENT_CERTIFICATES_YESTERDAY = f"""
    WITH yesterday_range AS (
        SELECT 
            strftime((CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '00:00:00', '%Y-%m-%d %H:%M:%S') as start_time,
            strftime((CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '23:59:59', '%Y-%m-%d %H:%M:%S') as end_time
    )
    SELECT COUNT(*) as certificate_count
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
    CROSS JOIN yesterday_range y
    INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
        ON e.userID = u.user_id
    INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
        ON e.content_id = c.content_id
    WHERE c.content_status IN ('Live', 'Retired')
    AND u.status = 1 
    AND e.first_completed_on IS NOT NULL
    AND e.first_completed_on != ''
    AND e.first_completed_on >= y.start_time
    AND e.first_completed_on <= y.end_time
    AND e.certificateID IS NOT NULL
    AND e.certificateID != ''
    """

    EVENT_CERTIFICATES_YESTERDAY = f"""
    WITH yesterday_range AS (
        SELECT 
            (CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '00:00:00' as start_time,
            (CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '23:59:59' as end_time
    )
    SELECT COUNT(DISTINCT certificate_id) as event_certificate_count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') e, yesterday_range y
    WHERE e.status = 'completed'
    AND e.certificate_id IS NOT NULL
    AND e.enrolled_on_datetime >= strftime(y.start_time, '%Y-%m-%d %H:%M:%S')
    AND e.enrolled_on_datetime <= strftime(y.end_time, '%Y-%m-%d %H:%M:%S')
    """

    EVENT_CERTIFICATES_NLW = f"""
    SELECT COUNT(DISTINCT certificate_id) as event_certificate_count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') 
    WHERE status = 'completed'
    AND certificate_id IS NOT NULL
    AND enrolled_on_datetime >= {NLW_START_DATE}
    """

    CONTENT_CERTIFICATES_NLW = f"""
    SELECT COUNT(*) as certificate_count,
           COUNT(DISTINCT e.userID) as unique_user_count
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') e
    INNER JOIN read_parquet('{ParquetFileConstants.USER_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') u
        ON e.userID = u.user_id
    INNER JOIN read_parquet('{ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE}/**.parquet') c
        ON e.content_id = c.content_id
    WHERE c.content_status IN ('Live', 'Retired')
    AND u.status = 1 
    AND e.first_completed_on IS NOT NULL
    AND e.first_completed_on != ''
    AND epoch(strptime(e.first_completed_on, '%Y-%m-%d %H:%M:%S')) >= {nlw_start_epoch}
    AND epoch(strptime(e.first_completed_on, '%Y-%m-%d %H:%M:%S')) <= {nlw_end_epoch}
    AND e.certificateID IS NOT NULL
    AND e.certificateID != ''
    """

    # ===== COURSES ENROLLED/COMPLETED AT LEAST ONCE =====
    COURSES_ENROLLED_AT_LEAST_ONCE = BASE_DATA_COMPLETE + """
    SELECT 
        COUNT(DISTINCT courseID) as courses_enrolled_count,
        STRING_AGG(DISTINCT courseID, ',') as course_id_list
    FROM base_data 
    WHERE live_retired_course_eligible = 'live_retired_course'
    AND courseStatus = 'Live'
    AND courseID != ''
    """

    COURSES_COMPLETED_AT_LEAST_ONCE = BASE_DATA_COMPLETE + """
    SELECT 
        COUNT(DISTINCT courseID) as courses_completed_count,
        STRING_AGG(DISTINCT courseID, ',') as course_id_list
    FROM base_data 
    WHERE live_retired_course_eligible = 'live_retired_course'
    AND courseStatus = 'Live'
    AND completion_category = 'completed'
    AND courseID != ''
    """

    # ===== QUERY LISTS FOR ORGANIZED EXECUTION =====
    ORG_BASED_LIST = [ORG_BASED_DESIGNATION_LIST, ORG_USER_COUNT_DATAFRAME_QUERY, ORG_BASED_MDO_ADMIN_COUNT]
    COURSE_BASED_LIST = [COURSE_COUNT_BY_STATUS_GROUP_BY_ORG]
    ENROLMENT_BASED_LIST = [OVERALL_METRICS, MDO_WISE_COMPREHENSIVE, CBP_WISE_COMPREHENSIVE]
    TOP_5_LIST = [TOP_5_USERS_BY_COMPLETION_BY_MDO, TOP_5_COURSES_BY_COMPLETION_BY_MDO,
                  TOP_5_CONTENT_BY_COMPLETION_BY_ORG, TOP_5_CONTENT_BY_ENROLLMENTS_BY_CBP,
                  TOP_5_COURSES_BY_RATING, TOP_5_MDO_BY_COMPLETION]
    TRENDING_LIST = [TRENDING_COURSES_BY_ORG, TRENDING_PROGRAMS_BY_ORG, MOST_ENROLLED_TAG,
                     TRENDING_COURSES_OVERALL, TRENDING_PROGRAMS_OVERALL]
    CERTIFICATION_LIST = [CERTIFICATIONS_TILL_TODAY, CERTIFICATIONS_OF_THE_WEEK,
                          TOP_10_CERTIFICATIONS, TOP_CERTIFICATIONS_BY_MDO]
    NLW_LIST = [NLW_EVENT_ENROLLMENTS, NLW_CONTENT_ENROLLMENTS, EVENT_CERTIFICATES_NLW,
                CONTENT_CERTIFICATES_NLW]
    EVENT_LIST = [TRENDING_EVENTS_BY_MDO, FEATURED_EVENTS_OVERALL, TOTAL_EVENT_ENROLLMENTS,
                  EVENTS_PUBLISHED_COUNT]


def main():
    print("✅ Complete QueryConstants loaded with CORRECTED warehouse joins")
    print(f"Total ORG queries: {len(QueryConstants.ORG_BASED_LIST)}")
    print(f"Total COURSE queries: {len(QueryConstants.COURSE_BASED_LIST)}")
    print(f"Total ENROLMENT queries: {len(QueryConstants.ENROLMENT_BASED_LIST)}")
    print(f"Total TOP 5 queries: {len(QueryConstants.TOP_5_LIST)}")
    print(f"Total TRENDING queries: {len(QueryConstants.TRENDING_LIST)}")
    print(f"Total CERTIFICATION queries: {len(QueryConstants.CERTIFICATION_LIST)}")
    print(f"Total NLW queries: {len(QueryConstants.NLW_LIST)}")
    print(f"Total EVENT queries: {len(QueryConstants.EVENT_LIST)}")


if __name__ == "__main__":
    main()