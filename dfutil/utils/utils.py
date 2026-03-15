from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col,when, lit, transform, struct, concat_ws, size)
import requests
from requests.auth import HTTPBasicAuth
import json
from typing import Optional
from google.cloud import storage
import os
import zipfile
import shutil
from confluent_kafka import Producer
from kafka import KafkaProducer


def druidDFOption(query: str, host: str, result_format: str = "object", limit: int = 10000,
                  spark: SparkSession = None) -> Optional[DataFrame]:
    """
    PySpark version of druidDFOption function
    """
    if spark is None:
        spark = SparkSession.getActiveSession()

    # Try using Spark Druid connector if available
    try:
        # Check if Druid connector is available
        df = spark.read \
            .format("druid") \
            .option("url", f"http://{host}:8888/druid/v2/sql") \
            .option("query", query) \
            .option("resultFormat", result_format) \
            .option("limit", limit) \
            .load()

        if df.count() == 0:
            print("ERROR: Druid connector returned empty dataframe")
            return None

        return df.persist(StorageLevel.MEMORY_ONLY)

    except Exception as e:
        print(f"Druid connector not available or failed")
        print("Falling back to HTTP API approach")

        # Fallback to HTTP API approach
        result = druidSQLAPI(query, host, result_format, limit).strip()

        # return None if result is an empty string
        if result == "":
            print("ERROR: druidSQLAPI returned empty string")
            return None

        df = dataframe_from_json_string(result, spark).persist(StorageLevel.MEMORY_ONLY)

        if df.count() == 0:
            print("ERROR: druidSQLAPI json parse result is empty")
            return None

        # return None if there is an `error` field in the json
        if has_column(df, "error"):
            print(f"ERROR: druidSQLAPI returned error response, response={result}")
            return None

        # now that error handling is done, proceed with business as usual
        return df


def druidSQLAPI(query: str, host: str, result_format: str = "object", limit: int = 10000) -> str:
    """
    PySpark version of druidSQLAPI function
    """
    # TODO: tech-debt, use proper spark druid connector when available
    url = f"http://{host}:8888/druid/v2/sql"
    request_body = {
        "resultFormat": result_format,
        "header": False,
        "context": {"sqlOuterLimit": limit},
        "query": query
    }

    return api("POST", url, json.dumps(request_body))


def api(method: str, url: str, body: str) -> str:
    """
    Simple HTTP API call function
    """
    try:
        if method.upper() == "POST":
            response = requests.post(
                url,
                data=body,
                headers={'Content-Type': 'application/json'}
            )
        elif method.upper() == "GET":
            response = requests.get(url)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()
        return response.text

    except requests.exceptions.RequestException as e:
        print(f"ERROR: API call failed: {e}")
        return ""


def sync_reports(local_path, remote_path, config):
    """
    Upload all files from `local_path` to GCS path: gs://<container>/<remote_path> using GCP service account.

    Parameters:
        local_path (str): Local directory to upload.
        remote_path (str): GCS path under the bucket (like 'reports/standalone-reports/merged').
        conf (object): Configuration object with attributes:
            - conf.container (str): GCS bucket name
            - conf.store (str): Expected to be 'gcs'
            - conf.gcp_service_account_key (str): Path to GCP credentials JSON
    """
    print(f"REPORT: Syncing reports from {local_path} to gs://{config.gcpBucket}/{remote_path} ...")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.googleServiceAccountFilePath
    client = storage.Client()
    bucket = client.bucket(config.gcpBucket)

    if os.path.isfile(local_path):   # single file case
        filename = os.path.basename(local_path)   # keep whatever CSV name was generated
        gcs_blob_path = os.path.join(remote_path, filename).replace("\\", "/")
        blob = bucket.blob(gcs_blob_path)
        blob.upload_from_filename(local_path)
        print(f"✅ Synced: {local_path} → gs://{config.gcpBucket}/{gcs_blob_path}")

    else:   # directory case (unchanged)
        for root, _, files in os.walk(local_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_path)
                gcs_blob_path = os.path.join(remote_path, relative_path).replace("\\", "/")
                blob = bucket.blob(gcs_blob_path)
                blob.upload_from_filename(local_file_path)
                print(f"✅ Synced: {local_file_path} → gs://{config.gcpBucket}/{gcs_blob_path}")

    print(f"REPORT: Finished syncing reports from {local_path} to gs://{config.gcpBucket}/{remote_path}")

def zip_and_sync_reports(complete_path: str, report_path: str,config):
    """
    Zip report folder and sync to blob storage.
    Instance method version that can access self.sync_reports
    
    Args:
        complete_path: Complete local path to the report folder
        report_path: Remote path for blob storage sync
    """
    try:
        print(f"Starting zip and sync for: {complete_path}")
        
        folder = Path(complete_path)
        zip_file_path = f"{complete_path}.zip"
        
        # Step 1: Delete existing .zip file if it exists
        report_name = folder.name
        existing_zip_file = folder / f"{report_name}.zip"
        
        if existing_zip_file.exists():
            print(f"Deleting existing zip file: {existing_zip_file}")
            existing_zip_file.unlink()
        
        # Step 2: Delete .crc files (Hadoop checksum files)
        if folder.exists() and folder.is_dir():
            crc_files = list(folder.glob("*.crc"))
            if crc_files:
                print(f"Deleting {len(crc_files)} .crc files")
                for crc_file in crc_files:
                    crc_file.unlink()
        
        # Step 3: Zip the folder
        print(f"Creating zip file: {zip_file_path}")
        
        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
            for root, dirs, files in os.walk(complete_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, os.path.dirname(complete_path))
                    zipf.write(file_path, arcname)
        
        print(f"Zip file created successfully: {zip_file_path}")
        
        # Step 4: Clean directory contents
        if folder.exists() and folder.is_dir():
            print(f"Cleaning directory: {complete_path}")
            for item in folder.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
        
        # Step 5: Move zip file inside the parent directory
        zip_file_name = Path(zip_file_path).name
        destination_zip_file_path = folder / zip_file_name
        
        print(f"Moving zip file to: {destination_zip_file_path}")
        shutil.move(zip_file_path, str(destination_zip_file_path))
        
        # Step 6: Sync to blob storage
        print(f"Syncing to blob storage: {report_path}")
        sync_reports(complete_path, report_path,config)
        
        print(f"Successfully zipped and synced: {complete_path}")
        
    except Exception as e:
        print(f"Error in zip_and_sync_reports: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise

def dataframe_from_json_string(json_str: str, spark: SparkSession) -> DataFrame:
    """
    Convert JSON string to DataFrame
    """
    try:
        # Parse JSON string to get list of records
        data = json.loads(json_str)

        # Create DataFrame from JSON data
        if isinstance(data, list) and len(data) > 0:
            df = spark.read.json(spark.sparkContext.parallelize([json.dumps(record) for record in data]))
            return df
        else:
            # Return empty DataFrame with no schema
            return spark.createDataFrame([], schema=None)

    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON: {e}")
        return spark.createDataFrame([], schema=None)
    except Exception as e:
        print(f"ERROR: Failed to create DataFrame from JSON: {e}")
        return spark.createDataFrame([], schema=None)


def read_cassandra_table(spark, keyspace: str, table: str) -> "DataFrame":
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()


def has_column(df: DataFrame, column_name: str) -> bool:
    """
    Check if DataFrame has a specific column
    """
    return column_name in df.columns


def read_elasticsearch_data(spark: SparkSession, host: str, port: str, index: str, query: str, fields: list,
                            array_fields: list) -> "DataFrame":
    """Read data from Elasticsearch"""
    dfr = spark.read.format("org.elasticsearch.spark.sql") \
        .option("es.read.metadata", "false") \
        .option("es.nodes", host) \
        .option("es.port", port) \
        .option("es.index.auto.create", "false") \
        .option("es.nodes.wan.only", "true") \
        .option("es.nodes.discovery", "false")

    # Add array field handling if specified
    if array_fields:
        dfr = dfr.option("es.read.field.as.array.include", ",".join(array_fields))

    # Add query and load data
    df = dfr.option("query", query).load(index)

    # Select only the specified fields that actually exist
    if fields:
        # Filter to only existing fields
        existing_fields = [f for f in fields if f in df.columns]
        missing_fields = [f for f in fields if f not in df.columns]

        # Select existing fields
        df = df.select(*[col(f) for f in existing_fields])

        # Add missing fields as null
        for field in missing_fields:
            df = df.withColumn(field, lit(None).cast(StringType()))

    if 'responses' in df.columns:
        df = df.withColumnRenamed("responses", "responses_raw")
        df = df.withColumn(
            "responses",
            transform(
                col("responses_raw"),
                lambda x: struct(
                    x.question.alias("question"),
                    when(
                        x.answer.isNull() | (size(x.answer) == 0),
                        lit("NA")
                    ).otherwise(concat_ws(",", x.answer)).alias("answer"),
                    x.questionId.alias("questionId"),
                    x.answerType.alias("answerType")
                )
            )
        )
    # Persist with MEMORY_ONLY storage level for performance
    df = df.persist(StorageLevel.MEMORY_ONLY)

    # Force evaluation to ensure data is loaded
    count = df.count()
    print(f"Successfully loaded {count} rows from Elasticsearch index: {index}")

    return df

def writeToCassandra(df, keyspace: str, table: str, mode: str = "append"):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .mode(mode) \
        .save()

def dispatch_df_to_kafka(df, topic: str, broker_list: str):
    if not topic:
        print("ERROR: topic is blank, skipping kafka dispatch")
        return
    if not broker_list:
        print("ERROR: broker list is blank, skipping kafka dispatch")
        return

    def send_partition(rows_iter):
        producer = KafkaProducer(
            bootstrap_servers=broker_list.split(","),
            value_serializer=lambda v: v.encode("utf-8"),
        )
        try:
            for row in rows_iter:
                # adjust serialization as needed
                value = json.dumps(row.asDict())
                producer.send(topic, value)
            producer.flush()
        finally:
            producer.close()

    df.foreachPartition(send_partition)


def read_elasticsearch_data_scroll(spark: SparkSession, host: str, port: str, index: str, fields: list = None, scroll_size: int = 1000, scroll_timeout: str = "2m", query: dict = None, es_user: str = None, es_pass: str = None) -> DataFrame:
    """
    Fetch all data from Elasticsearch index using scroll API.

    Parameters:
        spark (SparkSession): The Spark session.
        host (str): Elasticsearch host.
        port (str): Elasticsearch port.
        index (str): Index name.
        fields (list, optional): List of fields to fetch.
        scroll_size (int, optional): Number of docs per scroll batch. Default 1000.
        scroll_timeout (str, optional): Scroll context timeout (e.g. "2m"). Default "2m".
        query (dict, optional): Query DSL dict. Default match_all.
        es_user (str, optional): Elasticsearch username.
        es_pass (str, optional): Elasticsearch password.

    Returns:
        DataFrame: All documents from the index as a Spark DataFrame.
    """

    all_docs = []
    url = f"http://{host}:{port}/{index}/_search?scroll={scroll_timeout}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "size": scroll_size,
        "query": query if query else {"match_all": {}},
    }
    if fields:
        payload["_source"] = fields
    auth = HTTPBasicAuth(es_user, es_pass) if es_user and es_pass else None
    # Initial search
    resp = requests.post(url, headers=headers, data=json.dumps(payload), auth=auth)
    resp.raise_for_status()
    data = resp.json()
    scroll_id = data.get("_scroll_id")
    hits = data.get("hits", {}).get("hits", [])
    all_docs.extend([hit["_source"] for hit in hits])
    # Scroll loop
    while scroll_id and hits:
        scroll_url = f"http://{host}:{port}/_search/scroll"
        scroll_payload = {"scroll": scroll_timeout, "scroll_id": scroll_id}
        resp = requests.post(scroll_url, headers=headers, data=json.dumps(scroll_payload), auth=auth)
        resp.raise_for_status()
        data = resp.json()
        scroll_id = data.get("_scroll_id")
        hits = data.get("hits", {}).get("hits", [])
        all_docs.extend([hit["_source"] for hit in hits])
        if not hits:
            break
    # Convert to DataFrame
    if not all_docs:
        print(f"No documents found in index: {index}")
        return spark.createDataFrame([], schema=None)
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(doc) for doc in all_docs]))
    print(f"Loaded {df.count()} documents from ES index '{index}' using scroll API.")
    return df
