# config.py - Environment Configuration
# This file contains all configuration values that will be dynamically updated during deployment
# Template variables will be replaced with actual values based on environment

# Database Configuration
DATABASE_CONFIG = {
    # Cassandra Configuration
    'cassandraCourseKeyspace': 'sunbird_courses',
    'cassandraUserKeyspace': 'sunbird',
    'cassandraHierarchyStoreKeyspace': 'prod_hierarchy_store',

    # Cassandra Core Host
    'sparkCassandraConnectionHost': '10.175.5.7',
    'lpCassandraHost': '10.175.5.7',

    # Cassandra Table Names
    'cassandraUserEnrolmentsTable': 'user_enrolments_v2',
    'cassandraCourseBatchTable': 'course_batch',
    'cassandraFrameworkHierarchyTable': 'framework_hierarchy',
    'cassandraUserAssessmentTable': 'user_assessment_data_v2',
    'cassandraContentHierarchyTable': 'content_hierarchy',
    'cassandraRatingSummaryTable': 'ratings_summary',
    'cassandraAcbpTable': 'cb_plan_v2',
    'cassandraRatingsTable': 'ratings',
    'cassandraUserRolesTable': 'user_roles',
    'cassandraOrgTable': 'organisation',
    'cassandraUserTable': 'user',
    'cassandraLearnerLeaderBoardTable': 'learner_leaderboard',
    'cassandraLearnerLeaderBoardLookupTable': 'learner_leaderboard_lookup',
    'cassandraKarmaPointsTable': 'user_karma_points',
    'cassandraKarmaPointsSummaryTable': 'user_karma_points_summary',
    'cassandraKarmaPointsLookupTable': 'user_karma_points_credit_lookup',
    'cassandraOldAssesmentTable': 'user_assessment_master',
    'cassandraLearnerStatsTable': 'learner_stats',
    'cassandraOrgHierarchyTable': 'org_hierarchy',
    'cassandraUserEntityEnrolmentTable': 'user_entity_enrolments',
    'cassandraPublicUserAssessmentDataTable': 'public_user_assessment_data',
    'cassandraUserFeedKeyspace': 'sunbird_notifications',
    'cassandraUserFeedTable': 'notification_feed',
    'cassandraHallOfFameTable': 'mdo_karma_points',
    'cassandraMDOLearnerLeaderboardTable': 'mdo_top_learners',
    'cassandraSLWMdoLeaderboardTable': 'slw_mdo_leaderboard',
    'cassandraSLWMdoTopLearnerTable': 'slw_mdo_top_learners',
    'cassandraNLWMdoLeaderboardTable': 'nlw_mdo_leaderboard',
    'cassandraNLWUserLeaderboardTable': 'nlw_user_leaderboard',
    'cassandraGroupDesignationTable': 'kb_group_designation_content_data',

    # PostgreSQL Application Database
    'appPostgresHost': '10.175.5.15:5432',
    'appPostgresSchema': 'sunbird',
    'appOrgHierarchyTable': 'org_hierarchy_v4',
    'appPostgresUsername': 'sunbird',
    'appPostgresCredential': 'sunbird',
    'postgresCompetencyTable': 'data_node',
    'postgresCompetencyHierarchyTable': 'node_mapping',
    
    # PostgreSQL Data Warehouse
    'dwPostgresHost': '10.175.5.62:5432',
    'dwPostgresSchema': 'warehouse',
    'dwPostgresUsername': 'postgres',
    'dwPostgresCredential': 'password123',
    'dwUserTable': 'user_detail',
    'dwCourseTable': 'content',
    'dwContentResourceTable': 'content_resource',
    'dwEventsTable': 'events',
    'dwEventsEnrolmentTable': 'events_enrolment',
    'dwEnrollmentsTable': 'user_enrolments',
    'dwOrgTable': 'org_hierarchy',
    'dwAssessmentTable': 'assessment_detail',
    'dwBPEnrollmentsTable': 'bp_enrolments',
    'dwKcmDictionaryTable': 'kcm_dictionary',
    'dwKcmContentTable': 'kcm_content_mapping',
    'dwCBPlanTable': 'cb_plan',
    'dwLearnerStatsTable': 'learner_stats',
    'dwSLWMdoLeaderboardTable': 'slw_mdo_leaderboard',
    'dwSLWMdoTopLearnerTable': 'slw_mdo_top_learners',
    'dwNLWUserLeaderboardTable': 'nlw_user_leaderboard',
    'dwAparCBPEnrollmentTable': 'apar_cbp_enrollment',
    
    # Elasticsearch Configuration
    'sparkElasticsearchConnectionHost': '10.175.5.10',
    'sparkElasticsearchAuditConnectionHost': '10.175.5.10',
    'sparkElasticsearchConnectionPort': '9200',
    'esFormDataIndex': 'fs-forms-data',
    'esFormDataIds': '1720793361489,1718964921012',

    # MongoDB Configuration
    'mongoDatabase': 'nodebb',
    'mongoDBCollection': 'objects',
    'mlSparkMongoConnectionHost': '10.175.5.38',
    'mlMongoDatabase': 'ml-survey',
    'surveyCollection': 'solutions',
    'reportConfigCollection': 'dataProductConfigurations',
}

# Spark Configuration
SPARK_CONFIG = {
    'sparkCassandraConnectionHost': '10.175.5.7',
    'sparkDruidRouterHost': '10.175.5.33',
    'mlSparkDruidRouterHost': '10.175.5.37',
    'sparkElasticsearchConnectionHost': '10.175.5.10',
    'sparkElasticsearchAuditConnectionHost': '10.175.5.10',
}

# Redis Configuration
REDIS_CONFIG = {
    'redisHost': '10.175.5.20',
    'redisPort': '6379',
    'redisDB': '12',
}

# Storage Configuration
STORAGE_CONFIG = {
    'bucket': 'igotproddp',
    'container': 'igotproddp',
    'key': 'storage.key.config',
    'secret': 'storage.secret.config',
    'storageKeyConfig': 'storage.key.config',
    'storageSecretConfig': 'storage.secret.config',
    'store': 'gs',
    'dpRawTelemetryBackupLocation': 'secor-prod/unique/raw/',
}

# Report Path Configuration
REPORT_PATHS = {
    'userReportPath': 'standalone-reports/user-report',
    'userEnrolmentReportPath': 'standalone-reports/user-enrollment-report',
    'courseReportPath': 'standalone-reports/course-report',
    'cbaReportPath': 'standalone-reports/cba-report',
    'standaloneAssessmentReportPath': 'standalone-reports/user-assessment-report-cbp',
    'taggedUsersPath': 'tagged-users/',
    'blendedReportPath': 'standalone-reports/blended-program-report',
    'orgHierarchyReportPath': 'standalone-reports/org-hierarchy-report',
    'commsConsoleReportPath': 'standalone-reports/comms-console',
    'acbpReportPath': 'standalone-reports/cbp-report',
    'acbpMdoEnrolmentReportPath': 'standalone-reports/cbp-report-mdo-enrolment',
    'acbpMdoSummaryReportPath': 'standalone-reports/cbp-report-mdo-summary',
    'kcmReportPath': 'standalone-reports/kcm-report',
    'mlReportPath': 'standalone-reports/ml-report',
    'bqScriptPath': '/home/analytics/pyspark/bq-scripts.sh',
}


# Kafka/Messaging Configuration
KAFKA_CONFIG = {
    'brokerList': '10.175.5.154:9092,10.175.5.155:9092,10.175.5.208:9092',
    'topic': 'prod.telemetry.derived',
    'compression': 'none',

    # Side Output Topics
    'sideOutput': {
        'brokerList': '10.175.5.154:9092,10.175.5.155:9092,10.175.5.156:9092',
        'compression': 'none',
        'topics': {
            'roleUserCount': 'prod.dashboards.role.count',
            'orgRoleUserCount': 'prod.dashboards.org.role.count',
            'allCourses': 'prod.dashboards.course',
            'userCourseProgramProgress': 'prod.dashboards.user.course.program.progress',
            'fracCompetency': 'prod.dashboards.competency.frac',
            'courseCompetency': 'prod.dashboards.competency.course',
            'expectedCompetency': 'prod.dashboards.competency.expected',
            'declaredCompetency': 'prod.dashboards.competency.declared',
            'competencyGap': 'prod.dashboards.competency.gap',
            'userOrg': 'prod.dashboards.user.org',
            'org': 'prod.dashboards.org',
            'userAssessment': 'prod.dashboards.user.assessment',
            'assessment': 'prod.dashboards.assessment',
            'acbpEnrolment': 'prod.dashboards.acbp.enrolment',
        }
    }
}

# Job Configuration Parameters
JOB_CONFIG = {
    'debug': 'false',
    'validation': 'false',
    'parallelization': 16,
    'parallelizationSmall': 8,
    'cutoffTime': '60.0',
    'deviceMapping': False,
    'apiVersion': 'v2',
    'modelParamsParallelization': 200,

    # Report Sync Configuration
    'reportSyncEnable': 'false',
    'reportSyncEnableSL': 'true',
    'reportZipSyncEnable': 'true',

    # Survey and Assessment Configuration
    'mdoIDs': '',
    'solutionIDs': '',
    'anonymousAssessmentNonLoggedInUserAssessmentIDs': 'do_114153388896780288112,do_11415336159226265611',
    'anonymousAssessmentLoggedInUserContentIDs': 'do_1141533540853432321675,do_1141533857591132161321,do_1141525365329264641663,do_1141527106280980481664',
    'platformRatingSurveyId': '1727072772658',
    'gracePeriod': '2',
    'baseUrlForEvidences': 'https://spv.igotkarmayogi.gov.in/apis/proxies/v8/cloud-services/mlcore/v1/files/download?file=',
    'includeExpiredSolutionIDs': 'True',

    # Batch Size Configuration
    'SurveyQuestionReportBatchSize': '2000',
    'SurveyStatusReportBatchSize': '40000',
    'ObservationQuestionReportBatchSize': '2000',
    'ObservationStatusReportBatchSize': '15000',

    # Learning Week Configuration
    'nationalLearningWeekStart': '2026-04-01 00:00:00',
    'nationalLearningWeekEnd': '2026-04-10 23:59:59',
    'stateLearningWeekStart': '2025-07-11 00:00:00',
    'stateLearningWeekEnd': '2025-07-18 23:59:59',

    # Communications Console Configuration
    'commsConsolePrarambhEmailSuffix': '.kb@karmayogi.in',
    'commsConsoleNumDaysToConsider': '15',
    'commsConsoleNumTopLearnersToConsider': '100000',
    'commsConsolePrarambhTags': 'rojgaar,rozgaar,rozgar',
    'commsConsolePrarambhCbpIds': 'do_11359618144357580811,do_113569878939262976132,do_1136364937253437441916,do_113474579909279744117,do_113651330692145152128,do_1134122937914327041177,do_113473120005832704152,do_1136364244148060161889',
    'commsConsolePrarambhNCount': '4',

    # Zip Reports Configuration
    'prefixDirectoryPath': 'standalone-reports',
    'destinationDirectoryPath': 'standalone-reports/merged',
    'directoriesToSelect': 'blended-program-report-mdo,cbp-report-mdo-summary,course-report,cba-report,cbp-report-mdo-enrolment,user-report,user-enrollment-report',
    'password': '123456',
}

# External Service Configuration
EXTERNAL_SERVICES = {
    'fracBackendHost': 'frac-dictionary.igotkarmayogi.gov.in',
}

# Search Configuration
SEARCH_CONFIG = {
    'search': {
        'type': 'gcloud',
        'queries': [{
            'bucket': 'igotproddp',
            'prefix': 'secor-prod/unique/raw/',
            'endDate': '',  # Will be set dynamically
            'delta': 0
        }]
    }
}

# Workflow Summary Model Configuration
WFS_MODEL_CONFIG = {
    'model': 'org.ekstep.analytics.model.WorkflowSummary',
    'modelParams': {
        'storageKeyConfig': 'storage.key.config',
        'storageSecretConfig': 'storage.secret.config',
        'apiVersion': 'v2',
        'parallelization': 200
    },
    'output': [{
        'to': 'kafka',
        'params': {
            'brokerList': '10.175.5.154:9092,10.175.5.155:9092,10.175.5.156:9092',
            'topic': 'prod.telemetry.derived',
            'compression': 'none'
        }
    }],
    'parallelization': 200,
    'appName': 'Workflow Summarizer',
    'deviceMapping': True
}


# Combined configuration for easy access
def get_config():
    """
    Returns the complete configuration dictionary
    """
    config = {}
    config.update(DATABASE_CONFIG)
    config.update(SPARK_CONFIG)
    config.update(REDIS_CONFIG)
    config.update(STORAGE_CONFIG)
    config.update(REPORT_PATHS)
    config.update(KAFKA_CONFIG)
    config.update(JOB_CONFIG)
    config.update(EXTERNAL_SERVICES)
    return config

# Environment-specific overrides
def get_environment_config():
    base_config = get_config()
    return base_config

# Utility functions for specific configurations
def getCassandraConfig():
    """Returns only Cassandra-related configuration"""
    config = get_config()
    return {k: v for k, v in config.items() if 'cassandra' in k.lower()}

def getPostgresConfig():
    """Returns only PostgreSQL-related configuration"""
    config = get_config()
    return {k: v for k, v in config.items() if 'postgres' in k.lower() or k.startswith('dw') or k.startswith('app')}

def getKafkaConfig():
    """Returns only Kafka-related configuration"""
    return KAFKA_CONFIG

def getReportPaths():
    """Returns only report path configuration"""
    return REPORT_PATHS


def buildJobConfig(jobType, endDate=None):
    """
    Builds a complete job configuration for a specific job type
    Args:
        jobType (str): The job type identifier
        endDate (str): Optional end date for the job
    Returns:
        dict: Complete job configuration
    """
    base_config = get_config()
    
    if jobType == 'wfs':
        config = {
            'search': {
                'type': 'gcloud',
                'queries': [{
                    'bucket': base_config.get('bucket'),
                    'prefix': base_config.get('dpRawTelemetryBackupLocation'),
                    'endDate': endDate or '',
                    'delta': 0
                }]
            },
            'modelParams': {
                'storageKeyConfig': base_config.get('storageKeyConfig'),
                'storageSecretConfig': base_config.get('storageSecretConfig'),
                'apiVersion': base_config.get('apiVersion'),
                'parallelization': base_config.get('modelParamsParallelization')
            },
            'output': [{
                'to': 'kafka',
                'params': {
                    'brokerList': base_config.get('brokerList'),
                    'topic': base_config.get('topic'),
                    'compression': base_config.get('compression')
                }
            }],
            'parallelization': base_config.get('modelParamsParallelization'),
            'deviceMapping': True
        }
    else:
        # For other job types, build a standard configuration
        config = {
            'search': {'type': 'none'},
            'modelParams': base_config,
            'output': [],
            'parallelization': base_config.get('parallelization'),
            'deviceMapping': base_config.get('deviceMapping')
        }
    
    return config