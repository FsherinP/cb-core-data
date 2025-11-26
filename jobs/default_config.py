import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

DEFAULT_CONFIG = {
    # Debug and Validation
    'debug': 'true',
    'validation': 'true',

    # Redis Configuration
    'redisHost': '192.168.3.249',
    'redisPort': '6379',
    'redisDB': '12',

    # Spark Connection Hosts
    'sparkCassandraConnectionHost': '192.168.3.211',
    'sparkDruidRouterHost': '192.168.3.91',
    'sparkElasticsearchConnectionHost': '192.168.3.211',
    'sparkElasticsearchAuditConnectionHost': '10.175.5.10',
    'sparkElasticsearchConnectionPort': '9200',
    'sparkMongoConnectionHost': '192.168.3.178',

    # External Service Configuration
    'fracBackendHost': 'frac-dictionary.karmayogi.nic.in',

    # ML/Survey Configuration
    'solutionIDs': '',
    'mlMongoDatabase': 'ml-survey',
    'mlSparkDruidRouterHost': '192.168.3.91',
    'mlSparkMongoConnectionHost': '192.168.3.178',
    'surveyCollection': 'solutions',
    'reportConfigCollection': 'dataProductConfigurations',
    'gracePeriod': '2',
    'baseUrlForEvidences': 'www.https://igotkarmayogi.gov.in/',
    'mlReportPath': 'standalone-reports/ml-report',
    'includeExpiredSolutionIDs': 'true',

    # Batch Size Configuration
    'SurveyQuestionReportBatchSize': '2000',
    'SurveyStatusReportBatchSize': '20000',
    'ObservationQuestionReportBatchSize': '2000',
    'ObservationStatusReportBatchSize': '15000',

    # PostgreSQL Application Database
    'appPostgresHost': '192.168.3.178:5432',
    'appPostgresUsername': 'sunbird',
    'appPostgresCredential': 'sunbird',
    'appPostgresSchema': 'sunbird',
    'appOrgHierarchyTable': 'org_hierarchy_v4',
    'postgresCompetencyTable': 'data_node',
    'postgresCompetencyHierarchyTable': 'node_mapping',

    # PostgreSQL Data Warehouse
    'dwPostgresHost': '192.168.3.211:5432',
    'dwPostgresUsername': 'postgres',
    'dwPostgresCredential': 'Password@12345678',
    'dwPostgresSchema': 'warehouse',
    'dwUserActivityTable': 'user_activity',
    'dwUserTable': 'user_detail',
    'dwCourseTable': 'content',
    'dwEnrollmentsTable': 'user_enrolments',
    'dwOrgTable': 'org_hierarchy',
    'dwAssessmentTable': 'assessment_detail',
    'dwBPEnrollmentsTable': 'bp_enrolments',
    'dwKcmDictionaryTable': 'kcm_dictionary',
    'dwKcmContentTable': 'kcm_content_mapping',
    'dwCBPlanTable': 'cb_plan',
    'dwContentResourceTable': 'content_resource',
    'dwEventsTable': 'events',
    'dwEventsEnrolmentTable': 'events_enrolment',
    'dwLearnerStatsTable': 'learner_stats',
    'dwSLWMdoLeaderboardTable': 'slw_mdo_leaderboard',
    'dwSLWMdoTopLearnerTable': 'slw_mdo_top_learners',
    'dwNLWUserLeaderboardTable': 'nlw_user_leaderboard',
    'dwAparCBPEnrollmentTable': 'apar_cbp_enrollment',

    # Cassandra Keyspaces
    'cassandraUserKeyspace': 'sunbird',
    'cassandraCourseKeyspace': 'sunbird_courses',
    'cassandraHierarchyStoreKeyspace': 'dev_hierarchy_store',
    'cassandraUserFeedKeyspace': 'sunbird_notifications',

    # Cassandra Core Host
    'lpCassandraHost': '192.168.3.211',

    # Cassandra Tables
    'cassandraUserTable': 'user',
    'cassandraUserRolesTable': 'user_roles',
    'cassandraOrgTable': 'organisation',
    'cassandraUserEnrolmentsTable': 'user_enrolments_v2',
    'cassandraContentHierarchyTable': 'content_hierarchy',
    'cassandraRatingSummaryTable': 'ratings_summary',
    'cassandraRatingsTable': 'ratings',
    'cassandraOrgHierarchyTable': 'org_hierarchy',
    'cassandraCourseBatchTable': 'course_batch',
    'cassandraLearnerStatsTable': 'learner_stats',
    'cassandraKarmaPointsTable': 'user_karma_points',
    'cassandraHallOfFameTable': 'mdo_karma_points',
    'cassandraUserAssessmentTable': 'user_assessment_data_v2',
    'cassandraKarmaPointsLookupTable': 'user_karma_points_credit_lookup',
    'cassandraKarmaPointsSummaryTable': 'user_karma_points_summary',
    'cassandraUserFeedTable': 'notification_feed',
    'cassandraAcbpTable': 'cb_plan_v2',
    'cassandraLearnerLeaderBoardLookupTable': 'learner_leaderboard_lookup',
    'cassandraLearnerLeaderBoardTable': 'learner_leaderboard',
    'cassandraOldAssesmentTable': 'user_assessment_master',
    'cassandraNLWMdoLeaderboardTable': 'nlw_mdo_leaderboard',
    'cassandraNLWUserLeaderboardTable': 'nlw_user_leaderboard',
    'cassandraPublicUserAssessmentDataTable': 'public_user_assessment_data',
    'cassandraUserEntityEnrolmentTable': 'user_entity_enrolments',
    'cassandraFrameworkHierarchyTable': 'framework_hierarchy',
    'cassandraGroupDesignationTable': 'kb_group_designation_content_data',
    'cassandraMDOLearnerLeaderboardTable': 'mdo_learner_leaderboard',
    'cassandraSLWMdoLeaderboardTable': 'slw_mdo_leaderboard',
    'cassandraSLWMdoTopLearnerTable': 'slw_mdo_top_learners',
    'cassandraUserExtendedProfileTable' : "user_extended_profile",

    # MongoDB Configuration
    'mongoDatabase': 'nodebb',
    'mongoDBCollection': 'objects',

    # Storage Configuration
    'key': 'aws_storage_key',
    'secret': 'aws_storage_secret',
    'store': 'gs',
    'container': 'igot',
    'bucket': 'igot',
    'storageKeyConfig': 'storage.key.config',
    'storageSecretConfig': 'storage.secret.config',
    'dpRawTelemetryBackupLocation': 'raw-telemetry-backup',

    # Survey and Assessment Configuration
    'platformRatingSurveyId': '1696404440829',
    'cutoffTime': '60.0',
    'reportSyncEnable': 'true',
    'mdoIDs': '',
    'anonymousAssessmentLoggedInUserContentIDs': 'do_1141533540853432321675,do_1141533857591132161321,do_1141525365329264641663,do_1141527106280980481664',
    'anonymousAssessmentNonLoggedInUserAssessmentIDs': 'do_11415336159226265611',

    # Report Paths
    'userReportPath': 'standalone-reports/user-report',
    'userEnrolmentReportPath': 'standalone-reports/user-enrollment-report',
    'courseReportPath': 'standalone-reports/course-report',
    'cbaReportPath': 'standalone-reports/cba-report',
    'taggedUsersPath': 'tagged-users/',
    'standaloneAssessmentReportPath': 'standalone-reports/user-assessment-report-cbp',
    'blendedReportPath': 'standalone-reports/blended-program-report',
    'orgHierarchyReportPath': 'standalone-reports/org-hierarchy-report',
    'acbpReportPath': 'standalone-reports/cbp-report',
    'acbpMdoEnrolmentReportPath': 'standalone-reports/cbp-report-mdo-enrolment',
    'acbpMdoSummaryReportPath': 'standalone-reports/cbp-report-mdo-summary',
    'kcmReportPath': 'standalone-reports/kcm-report',
    'commsConsoleReportPath': 'standalone-reports/comms-console',
    'validationReportPath': 'standalone-reports/validation-report',

    'blendedProgramReport' : 'BlendedProgramReport.csv',
    'cbaReport' : 'UserAssessmentReport.csv',
    'cbpEnrolmentReport' : 'CBPEnrollmentReport.csv',
    'cbpSummaryReport' : 'CBPUserSummaryReport.csv',
    'courseReport' : 'ContentReport.csv',
    'kcmReport' : 'ContentCompetencyMapping.csv',
    'userAssessmentReport' : 'StandaloneAssessmentReport.csv',
    'userEnrollmentReport' : 'ConsumptionReport.csv',
    'userReport' : 'UserReport.csv',

    # Communications Console Configuration
    'commsConsolePrarambhEmailSuffix': '.kb@karmayogi.in',
    'commsConsoleNumDaysToConsider': '15',
    'commsConsoleNumTopLearnersToConsider': '60',
    'commsConsolePrarambhTags': 'rojgaar,rozgaar,rozgar',
    'commsConsolePrarambhCbpIds': 'do_113882965067743232154,do_1137468666262241281756,do_1139032976499261441156',
    'commsConsolePrarambhNCount': '2',

    # Elasticsearch Configuration
    'esFormDataIds': '1718964921012,1720793361489',
    'esFormDataIndex': 'form_data',

    # Learning Week Configuration
    'nationalLearningWeekStart': '2024-10-19 00:00:00',
    'nationalLearningWeekEnd': '2024-10-27 23:59:59',
    'stateLearningWeekStart': '2024-10-19 00:00:00',
    'stateLearningWeekEnd': '2025-03-14 23:59:59',
    'sizeBucketString': '1-100-XS,101-500-S,501-1000-M,1001-10000-L,10001-25000-X,above 25000-XXL',
    'overridesForSlw': {},
    'rollupRequiredOrgs': [],

    # Zip Reports Configuration
    'prefixDirectoryPath': 'standalone-reports',
    'destinationDirectoryPath': 'standalone-reports/merged',
    'localReportDir': f'{BASE_DIR}/reports',
    'warehouseReportDir': f'{BASE_DIR}/warehouse',
    'baseCachePath': f'{BASE_DIR}/data-res/pq_files/cache_pq/',
    'bqScriptPath': f'{BASE_DIR}/bq-scripts.sh',
    'warehouseOutputDir': f'{BASE_DIR}/warehouse/fullReport/',
    'mdoReportSyncPath': 'standalone-reports/merged/',
    'fullReportSyncPath': 'standalone-reports/merged/fullReport/',
    'kcmSyncPath': 'standalone-reports/merged/kcm/',
    'unifiedParquetPath': 'airflowData/',
    'unifiedParquetLocalPath': '/home/analytics/pyspark/warehouse/unified/',
    'directoriesToSelect': ["blended-program-report-mdo","cbp-report-mdo-summary","course-report","cba-report","cbp-report-mdo-enrolment","user-report","user-enrollment-report"],
    'pysparkDirectoriesToSelect': ["blended-program-report-mdo","cbp-report-mdo-summary","course-report","cba-report","cbp-report-mdo-enrolment","user-report","user-enrollment-report"],
    'googleServiceAccountFilePath': '/home/analytics/pyspark/jobs/gcp_service_account.json',
    'gcpBucket': 'igotproddp',
    'password': '123456',

    # Job Configuration
    'parallelization': '16',
    'parallelizationSmall': '8',
    'modelParamsParallelization': '200',
    'apiVersion': 'v2',
    'deviceMapping': False,
    'reportSyncEnableSL': 'true',
    'reportZipSyncEnable': 'true',

    # Kafka/Messaging Configuration
    'brokerList': '192.168.3.249:9092',
    'topic': 'dev.dashboard.default',
    'compression': 'none',
}

# Side Output Configuration (from Scala sideOutput map)
DEFAULT_SIDE_OUTPUT = {
    'brokerList': '192.168.3.249:9092',
    'compression': 'none',
    'topics': {
        'roleUserCount': 'dev.dashboards.role.count',
        'orgRoleUserCount': 'dev.dashboards.org.role.count',
        'allCourses': 'dev.dashboards.course',
        'userCourseProgramProgress': 'dev.dashboards.user.course.program.progress',
        'fracCompetency': 'dev.dashboards.competency.frac',
        'courseCompetency': 'dev.dashboards.competency.course',
        'expectedCompetency': 'dev.dashboards.competency.expected',
        'declaredCompetency': 'dev.dashboards.competency.declared',
        'competencyGap': 'dev.dashboards.competency.gap',
        'userOrg': 'dev.dashboards.user.org',
        'org': 'dev.dashboards.org',
        'userAssessment': 'dev.dashboards.user.assessment',
        'assessment': 'dev.dashboards.assessment'
    }
}

# Add sideOutput to DEFAULT_CONFIG
DEFAULT_CONFIG['sideOutput'] = DEFAULT_SIDE_OUTPUT


class SimpleConfig:
    def __init__(self, config_dict, defaults=None):
        # Set defaults first
        if defaults:
            for key, value in defaults.items():
                setattr(self, key, value)

        # Override with actual config values
        for key, value in config_dict.items():
            # SIMPLE FIX: If value has {{ }} and we're local, use default instead
            if isinstance(value, str) and '{{' in value and '/Users/' in os.getcwd():
                # Use default value instead of template
                if hasattr(self, key):
                    # print(f"[LOCAL] Using default for {key}: {getattr(self, key)}")
                    continue  # Keep the default value, don't override

            setattr(self, key, value)

    def get(self, key, default=None):
        """Backward compatibility with .get() method"""
        return getattr(self, key, default)


def create_config(config_dict):
    """Create a SimpleConfig instance with default values"""
    return SimpleConfig(config_dict, DEFAULT_CONFIG)