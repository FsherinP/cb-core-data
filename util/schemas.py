from pyspark.sql.functions import col, from_json, explode_outer, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType, FloatType, DateType,LongType

# Define sub-schemas
profile_competency_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("competencyType", StringType(), True),
    StructField("competencySelfAttestedLevel", StringType(), True),  # can be string/int, keep as string
    StructField("competencySelfAttestedLevelValue", StringType(), True)
])

professionalDetailsSchema = StructType([
    StructField("designation", StringType(), True),
    StructField("group", StringType(), True)
])

employment_details_schema = StructType([
    StructField("departmentName", StringType(), True),
    StructField("employeeCode", StringType(), True)
])

personal_details_schema = StructType([
    StructField("phoneVerified", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("pincode", StringType(), True),
    StructField("category", StringType(), True),
    StructField("mobile", StringType(), True),
    StructField("primaryEmail", StringType(), True)
])

additionalPropertiesSchema = StructType([
    StructField("tag", ArrayType(StringType()), True),
    StructField("externalSystemId", StringType(), True),
    StructField("externalSystem", StringType(), True)
])

profile_schema = StructType([
    StructField("professionaldetails", professionalDetailsSchema),
    StructField("personaldetails", personal_details_schema)
])

cadre_details_schema = StructType([
    StructField("cadreName", StringType(), True),
    StructField("civilServiceId", StringType(), True),
    StructField("cadreId", StringType(), True),
    StructField("civilServiceTypeId", StringType(), True),
    StructField("civilServiceType", StringType(), True),
    StructField("cadreControllingAuthorityName", StringType(), True),
    StructField("civilServiceName", StringType(), True),
    StructField("cadreBatch", StringType(), True),
])

hierarchySchema = StructType([
    StructField("children", ArrayType(StructType([
        StructField("identifier", StringType(), True),
        StructField("primaryCategory", StringType(), True),
        StructField("name", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("expectedDuration", StringType(), True),
        StructField("mimeType", StringType(), True),
        StructField("children", ArrayType(StructType([
            StructField("identifier", StringType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("name", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("mimeType", StringType(), True),
            StructField("expectedDuration", StringType(), True)
        ]), True), True)
    ]), True), True)
])

# Method to build the profileDetails schema
def makeProfileDetailsSchema(
    competencies=False,
    additionalProperties=False,
    professionalDetails=False,
    cadreDetails= False
) -> StructType:
    fields = [
        StructField("verifiedKarmayogi", BooleanType(), True),
        StructField("mandatoryFieldsExists", BooleanType(), True),
        StructField("profileImageUrl", StringType(), True),
        StructField("personalDetails", personal_details_schema, True),
        StructField("employmentDetails", employment_details_schema, True),
        StructField("profileStatus", StringType(), True)
    ]

    if competencies:
        fields.append(StructField("competencies", ArrayType(profile_competency_schema), True))

    if additionalProperties:
        fields.append(StructField("additionalProperties", additionalPropertiesSchema, True))
        fields.append(StructField("additionalPropertis", additionalPropertiesSchema, True))

    if additionalProperties:
        fields.append(StructField("professionalDetails", ArrayType(professionalDetailsSchema), True))

    if cadreDetails:
        fields.append(StructField("cadreDetails", cadre_details_schema, True))

    return StructType(fields)


# courseCompetenciesSchema
course_competencies_schema = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    # StructField("description", StringType(), True),
    # StructField("source", StringType(), True),
    StructField("competencyType", StringType(), True),
    # StructField("competencyArea", StringType(), True),
    # StructField("selectedLevelId", StringType(), True),
    # StructField("selectedLevelName", StringType(), True),
    # StructField("selectedLevelSource", StringType(), True),
    StructField("selectedLevelLevel", StringType(), True)
    # StructField("selectedLevelDescription", StringType(), True)
]))

# expectedCompetencySchema
expected_competency_schema = StructType([
    StructField("orgID", StringType(), True),
    StructField("workOrderID", StringType(), True),
    StructField("userID", StringType(), True),
    StructField("competencyID", StringType(), True),
    StructField("expectedCompetencyLevel", IntegerType(), True)
])

# activeUsersSchema
active_users_schema = StructType([
    StructField("orgID", StringType(), True),
    StructField("activeCount", LongType(), True)
])

# monthlyActiveUsersSchema
monthly_active_users_schema = StructType([
    StructField("DAUOutput", LongType(), True)
])

# timeSpentSchema
time_spent_schema = StructType([
    StructField("orgID", StringType(), True),
    StructField("timeSpent", FloatType(), True)
])

custom_field_value_schema = StructType([
            StructField("attributeName", StringType(), True),
            StructField("value", StringType(), True),
            StructField("level", IntegerType(), True)
        ])
        
custom_field_schema = StructType([
            StructField("customFieldId", StringType(), True),
            StructField("type", StringType(), True),
            StructField("attributeName", StringType(), True),
            StructField("value", StringType(), True),
            StructField("values", ArrayType(custom_field_value_schema), True)
        ])
        
context_data_schema = StructType([
            StructField("organisationId", StringType(), True),
            StructField("customFieldValues", ArrayType(custom_field_schema), True)
        ])

# fracCompetencySchema
frac_competency_schema = StructType([
    StructField("competencyID", StringType(), True),
    StructField("competencyName", StringType(), True),
    StructField("competencyStatus", StringType(), True)
])
# Function to define child schema (recursive)
def make_hierarchy_child_schema(include_children=False):
    fields = [
        StructField("identifier", StringType(), True),
        StructField("name", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("primaryCategory", StringType(), True),
        StructField("leafNodesCount", IntegerType(), True),
        StructField("contentType", StringType(), True),
        StructField("objectType", StringType(), True),
        StructField("showTimer", StringType(), True),
        StructField("allowSkip", StringType(), True)
    ]
    if include_children:
        # Recursive reference
        fields.append(StructField("children", ArrayType(make_hierarchy_child_schema(False)), True))
    return StructType(fields)

# Function to define main hierarchy schema
def make_hierarchy_schema(include_children=False, include_competencies=False, include_l2_children=False):
    fields = [
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("reviewStatus", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("primaryCategory", StringType(), True),
        StructField("leafNodesCount", IntegerType(), True),
        StructField("leafNodes", ArrayType(StringType()), True),
        StructField("publish_type", StringType(), True),
        StructField("isExternal", BooleanType(), True),
        StructField("contentType", StringType(), True),
        StructField("objectType", StringType(), True),
        StructField("userConsent", StringType(), True),
        StructField("visibility", StringType(), True),
        StructField("createdOn", StringType(), True),
        StructField("lastUpdatedOn", StringType(), True),
        StructField("lastPublishedOn", StringType(), True),
        StructField("lastSubmittedOn", StringType(), True),
        StructField("lastStatusChangedOn", StringType(), True),
        StructField("createdFor", ArrayType(StringType()), True)
    ]
    if include_children:
        fields.append(StructField("children", ArrayType(make_hierarchy_child_schema(include_l2_children)), True))
    if include_competencies:
        fields.append(StructField("competencies_v3", StringType(), True))
    return StructType(fields)

# Assessment related schemas
assessment_read_response_schema = StructType([
    StructField("name", StringType(), True),
    StructField("objectType", StringType(), True),
    StructField("version", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("totalQuestions", IntegerType(), True),
    StructField("maxQuestions", IntegerType(), True),
    StructField("expectedDuration", IntegerType(), True),
    StructField("primaryCategory", StringType(), True),
    StructField("maxAssessmentRetakeAttempts", IntegerType(), True)
])

submit_assessment_request_schema = StructType([
    StructField("courseId", StringType(), False),
    StructField("batchId", StringType(), False),
    StructField("primaryCategory", StringType(), False),
    StructField("isAssessment", BooleanType(), False),
    StructField("timeLimit", IntegerType(), False)
])

submit_assessment_response_schema = StructType([
    StructField("result", FloatType(), False),
    StructField("total", IntegerType(), False),
    StructField("blank", IntegerType(), False),
    StructField("correct", IntegerType(), False),
    StructField("incorrect", IntegerType(), False),
    StructField("pass", BooleanType(), False),
    StructField("overallResult", FloatType(), False),
    StructField("passPercentage", FloatType(), False)
])

# Batch attribute schemas
batch_attrs_session_details_v2_facilitator_details_schema = StructType([
    StructField("name", StringType(), True),
    StructField("id", StringType(), True),
    StructField("email", StringType(), True)
])

batch_attrs_session_details_v2_schema = StructType([
    StructField("sessionId", StringType(), True),
    StructField("sessionType", StringType(), True),
    StructField("sessionDuration", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("startDate", StringType(), True),
    StructField("startTime", StringType(), True),
    StructField("endTime", StringType(), True),
    StructField("facilatorDetails", ArrayType(batch_attrs_session_details_v2_facilitator_details_schema), True)
])

batch_attrs_schema = StructType([
    StructField("batchLocationDetails", StringType(), True),
    StructField("latlong", StringType(), True),
    StructField("currentBatchSize", StringType(), True),
    StructField("sessionDetails_v2", ArrayType(batch_attrs_session_details_v2_schema), True)
])

# Logged In User Schemas
logged_in_mobile_user_schema = StructType([
    StructField("userID", StringType(), True),
    StructField("userLoginFromMobile", BooleanType(), True)
])

logged_in_web_user_schema = StructType([
    StructField("userID", StringType(), True),
    StructField("userLoginFromWeb", BooleanType(), True)
])

user_actual_time_spent_learning_schema = StructType([
    StructField("userID", StringType(), True),
    StructField("userActualTimeSpentLearning", FloatType(), True)
])

users_platform_engagement_schema = StructType([
    StructField("userid", StringType(), True),
    StructField("platformEngagementTime", FloatType(), True),
    StructField("sessionCount", IntegerType(), True)
])

nps_user_ids_schema = StructType([
    StructField("userid", StringType(), True)
])

# Learning Hours Schemas
total_learning_hours_schema = StructType([
    StructField("userOrgID", StringType(), True),
    StructField("totalLearningHours", StringType(), True)
])

enrolment_count_by_user_schema = StructType([
    StructField("userID", StringType(), True),
    StructField("count", StringType(), True)
])

average_nps_schema = StructType([
    StructField("avgNps", IntegerType(), True)
])

learning_hours_by_user_schema = StructType([
    StructField("userID", StringType(), True),
    StructField("totalLearningHours", StringType(), True)
])

event_enrolment_count_by_user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_count", StringType(), True)
])

event_learning_hours_by_user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("totalEventLearningHours", StringType(), True)
])

# CBPlan Draft Data Schema
cbplan_draft_data_schema = StructType([
    StructField("name", StringType(), True),
    StructField("assignmentType", StringType(), True),
    StructField("assignmentTypeInfo", ArrayType(StringType()), True),
    StructField("endDate", StringType(), True),
    StructField("contentList", ArrayType(StringType()), True)
])

#access control schema
accessControlSchema = StructType([
    StructField("accessControl", StructType([
        StructField("userGroups", ArrayType(StructType([
            StructField("userGroupId", StringType(), True),
            StructField("userGroupName", StringType(), True),
            StructField("userGroupCriteriaList", ArrayType(StructType([
                StructField("criteriaKey", StringType(), True),
                StructField("criteriaValue", ArrayType(StringType()), True)
            ]), True), True)
        ]), True), True),
        StructField("version", IntegerType(), True)
    ]), True)
])

# Anonymous Assessment Content Access User Count
anonymous_assessment_content_access_user_count_schema = StructType([
    StructField("user_count", IntegerType(), True)
])

# User Day Count Wall Of Fame Schema
user_day_count_wall_of_fame_schema = StructType([
    StructField("day_count", IntegerType(), True),
    StructField("actor_id", StringType(), True)
])

# Mobile Versions Schema
mobile_versions_schema = StructType([
    StructField("user_count", IntegerType(), True),
    StructField("context_pdata_ver", StringType(), True),
    StructField("context_pdata_pid", StringType(), True)
])

# Unique Solution Ids Data Schema
unique_solution_ids_data_schema = StructType([
    StructField("solutionIds", StringType(), True)
])

# Solutions End Date Data Schema
solutions_end_date_data_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("endDate", DateType(), True)
])

# Observation Status Completed Data Schema
observation_status_completed_data_schema = StructType([
    StructField("completedAt", StringType(), True),
    StructField("observationSubmissionId", StringType(), True)
])

# Observation Status In Progress Data Schema
observation_status_in_progress_data_schema = StructType([
    StructField("inprogressAt", StringType(), True),
    StructField("observationSubmissionId", StringType(), True)
])

# Event Progress Detail Schema
event_progress_detail_schema = StructType([
    StructField("max_size", StringType(), False),
    StructField("mimeType", StringType(), False),
    StructField("duration", IntegerType(), True),
    StructField("stateMetaData", IntegerType(), True)
])

# KCM v6 Schema
kcm_schema = StructType([
    StructField("categories", ArrayType(StructType([
        StructField("code", StringType(), False),
        StructField("terms", ArrayType(StructType([
            StructField("name", StringType(), False),
            StructField("description", StringType(), False),
            StructField("refId", StringType(), False),
            StructField("category", StringType(), False),
            StructField("associations", ArrayType(StructType([
                StructField("name", StringType(), False),
                StructField("refType", StringType(), False),
                StructField("description", StringType(), False),
                StructField("refId", StringType(), False),
                StructField("category", StringType(), False)
            ])), False)
        ])), False)
    ])), False)
])

cios_data_schema = StructType([
        StructField("content", StructType([
            StructField("name", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("lastUpdatedOn", StringType(), True),
            StructField("contentPartner", StructType([
                StructField("id", StringType(), True),
                StructField("contentPartnerName", StringType(), True)
            ]), True)
        ]), True)
    ])

def get_hierarchy_schema():
        level3 = StructType([
            StructField("identifier", StringType(), True),
            StructField("name", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("leafNodesCount", IntegerType(), True),
            StructField("contentType", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("showTimer", StringType(), True),
            StructField("allowSkip", StringType(), True)
        ])

        level2 = StructType([
            StructField("identifier", StringType(), True),
            StructField("name", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("leafNodesCount", IntegerType(), True),
            StructField("contentType", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("showTimer", StringType(), True),
            StructField("allowSkip", StringType(), True),
            StructField("children", ArrayType(level3), True)
        ])

        level1 = StructType([
            StructField("identifier", StringType(), True),
            StructField("name", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("leafNodesCount", IntegerType(), True),
            StructField("contentType", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("showTimer", StringType(), True),
            StructField("allowSkip", StringType(), True),
            StructField("children", ArrayType(level2), True)
        ])

        return StructType([
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("reviewStatus", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("leafNodesCount", IntegerType(), True),
            StructField("leafNodes", ArrayType(StringType()), True),
            StructField("publish_type", StringType(), True),
            StructField("isExternal", BooleanType(), True),
            StructField("contentType", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("userConsent", StringType(), True),
            StructField("visibility", StringType(), True),
            StructField("createdOn", StringType(), True),
            StructField("lastUpdatedOn", StringType(), True),
            StructField("lastPublishedOn", StringType(), True),
            StructField("lastSubmittedOn", StringType(), True),
            StructField("lastStatusChangedOn", StringType(), True),
            StructField("createdFor", ArrayType(StringType()), True),
            StructField("children", ArrayType(level1), True),
            StructField("competencies_v3", StringType(), True)
        ])