from pathlib import Path

class ParquetFileConstants:

    INPUT_DIR = Path(__file__).resolve().parents[1] / "data-res/pq_files/cache_pq"
    # Define the output directory
    OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
    OUTPUT_COMPUTED_DIR = Path(__file__).resolve().parents[1] / "output/computed"
    OUTPUT_COMPUTED_DIR.mkdir(parents=True, exist_ok=True)

    # Static constants for each Parquet file
    ACBP_PARQUET_FILE = str(INPUT_DIR / "acbp/**.parquet")
    BATCH_PARQUET_FILE = str(INPUT_DIR / "batch/**.parquet")
    ENROLMENT_PARQUET_FILE = str(INPUT_DIR / "enrolment/**.parquet")
    ESCONTENT_PARQUET_FILE = str(INPUT_DIR / "esContent/**.parquet")
    EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE = str(INPUT_DIR / "externalCourseEnrolments/**.parquet")
    EXTERNAL_CONTENT_PARQUET_FILE = str(INPUT_DIR / "externalContent/**.parquet")
    EVENT_PARQUET_FILE = str(INPUT_DIR / "eventDetails/**.parquet")
    EVENT_ENROLMENT_PARQUET_FILE = str(INPUT_DIR / "eventEnrolmentDetails/**.parquet")
    HIERARCHY_PARQUET_FILE = str(INPUT_DIR / "hierarchy/**.parquet")
    KCMV6_PARQUET_FILE = str(INPUT_DIR / "kcmV6/**.parquet")
    LEARNER_LEADERBOARD_PARQUET_FILE = str(INPUT_DIR / "learnerLeaderBoard/**.parquet")
    NLW_CONTENT_CERTIFICATE_GENERATED_COUNT_PARQUET_FILE = str(INPUT_DIR / "nlwContentCertificateGeneratedCount/**.parquet")
    NLW_CONTENT_LEARNING_HOURS_PARQUET_FILE = str(INPUT_DIR / "nlwContentLearningHours/**.parquet")
    ORG_PARQUET_FILE= str(INPUT_DIR / "org/**.parquet")
    ORG_COMPLETE_HIERARCHY_PARQUET_FILE = str(INPUT_DIR / "orgCompleteHierarchy/**.parquet")
    ORG_HIERARCHY_PARQUET_FILE = str(INPUT_DIR / "orgHierarchy/**.parquet")
    RATING_PARQUET_FILE = str(INPUT_DIR / "rating/**.parquet")
    RATING_SUMMARY_PARQUET_FILE = str(INPUT_DIR / "ratingSummary/**.parquet")
    ROLE_PARQUET_FILE = str(INPUT_DIR / "role/**.parquet")
    USER_PARQUET_FILE = str(INPUT_DIR / "user/**.parquet")
    CLAPS_PARQUET_FILE = str(INPUT_DIR / "weeklyClaps/**.parquet")
    USER_KARMA_POINTS_PARQUET_FILE = str(INPUT_DIR / "userKarmaPoints/**.parquet")
    USER_KARMA_POINTS_SUMMARY_PARQUET_FILE = str(INPUT_DIR / "userKarmaPointsSummary/**.parquet")
    USER_ASSESSMENT_PARQUET_FILE = str(INPUT_DIR / "userAssessment/**.parquet")
    OLD_ASSESSMENT_PARQUET_FILE = str(INPUT_DIR / "oldAssessmentDetails/**.parquet")
    USER_EXTENDED_PROFILE = str(INPUT_DIR / "userExtendedProfile/**.parquet")
    FINAL_ASSESSMENT_PARQUET_FILE = str(INPUT_DIR / "esFinalAssessment/**.parquet")
    COURSE_COMPLETION_SURVEY_PARQUET_FILE = str(INPUT_DIR / "courseCompletionSurvey/**.parquet")
    PEER_VALIDATION_FORMS_PARQUET_FILE = str(INPUT_DIR / "peerValidationSurveys/**.parquet")
    PEER_VALIDATION_ELIGIBLE_USERS_PARQUET_FILE = str(INPUT_DIR / "peerValidationEligibleUsers/**.parquet")
    QUESTIONSET_HIERARCHY_PARQUET_FILE = str(INPUT_DIR / "questionsetHierarchy/**.parquet")
    ASSESSMENT_DATA_RAW_PARQUET_FILE = str(INPUT_DIR / "userAssessmentRaw/**.parquet")
    UNENROLMENT_AUDIT_PARQUET_FILE = str(INPUT_DIR / "unenrolledUserAudit/**.parquet")
    ###
    ###  Computed Parquet File
    ###
    USER_SELECT_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "user-select/")
    ACBP_SELECT_FILE = str(OUTPUT_COMPUTED_DIR / "acbp-select/")
    ENROLMENT_SELECT_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "enrolment-select/")
    ENROLMENT_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "enrolment-computed/")
    EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "external-enrolment-computed/")
    EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "external-content-computed/")
    RATING_SELECT_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "rating-select/")
    CONTENT_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "content-computed/")
    USER_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "user-computed")
    ORG_SELECT_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "org-select/")
    ORG_HIERARCHY_SELECT_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "org-hierarchy-select/")
    ORG_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "org-computed/")
    USER_ORG_COMPUTED_FILE = str(OUTPUT_COMPUTED_DIR / "user-org-computed/")
    ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "all-course-program-computed/")
    ALL_ASSESSMENT_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "all-assessment-computed/")
    ACBP_COMPUTED_FILE = str(OUTPUT_COMPUTED_DIR / "acbp-computed")
    RATING_SUMMARY_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "rating-summary-computed/")
    RATING_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "rating-computed/")
    CONTENT_RATING_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "content-rating-computed/")
    TEMP_COMPUTE_FILE = str(OUTPUT_COMPUTED_DIR / "temp-computed/")
    CONTENT_HIERARCHY_SELECT_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "content-hierarchy-select/")
    BATCH_SELECT_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "batch-select/")
    OLD_ASSESSMENT_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "old-assessment/")
    ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "enrolment-warehouse-computed/")
    ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE= ENROLMENT_COMPUTED_PARQUET_FILE
    USER_WAREHOUSE_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "user-warehouse-computed/")
    CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "content-warehouse-computed/")
    GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "user_enrolment_badge_computed/")
    CONTENT_HIERARCHY_FLATTENED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "content_hierarchy_flattened")
    ###
    ###  Output CSV File
    ###
    USER_REPORT_CSV = str(OUTPUT_COMPUTED_DIR / "user-report/")
    USER_ENROLMENT_CSV = str(OUTPUT_COMPUTED_DIR / "user-enrolment/")

    DATE_TIME_WITH_AMPM_FORMAT = "yyyy-MM-dd HH:mm:ss a"
    DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
    DATE_TIME_WITH_MILLI_SEC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    DATE_FORMAT = "yyyy-MM-dd"
    TIME_FORMAT = "HH:mm:ss"
# Example Usage:
def main():
    print("Defined Static Parquet File Constants:")

if __name__ == "__main__":
    main()
