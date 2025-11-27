from datetime import datetime, timedelta
import subprocess
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum

IST = pendulum.timezone("Asia/Kolkata")

# Default arguments for the L2 Assessment DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the separate L2 Assessment DAG
new_org_hierarchy_dag = DAG(
    'new_org_hierarchy_computation_pipeline',
    default_args=default_args,
    description='New Org Hierarchy Computation Pipeline - Runs at 8:30 AM',
    schedule="30 08 * * *",  # 8:00 AM daily
    start_date=datetime(2024, 12, 16, tzinfo=IST),
    catchup=False,
    tags=['python', 'new_org_hierarchy', 'etl'],
)

def run_python_script(script_path, date_param, task_name):
    """
    Generic function to run Python scripts with date parameter
    """
    try:
        print(f"Starting {task_name}")
        print(f"Script path: {script_path}")
        print(f"Date parameter: {date_param}")
        
        # Check if script exists
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")
        
        # Prepare command
        # check if date param is null or empty
        if not date_param:
            cmd = [sys.executable, script_path]
        else:
            cmd = [sys.executable, script_path, '--date', date_param]
        print(f"Executing command: {' '.join(cmd)}")
        
        # Execute the script
        result = subprocess.run(
            cmd,
            cwd=os.path.dirname(script_path),
            capture_output=True,
            text=True,
            check=True
        )
        
        print(f"Script output:\n{result.stdout}")
        if result.stderr:
            print(f"Script stderr:\n{result.stderr}")
            
        print(f"Completed {task_name} successfully")
        return result.returncode
        
    except subprocess.CalledProcessError as e:
        print(f"Error executing {task_name}: {e}")
        print(f"Return code: {e.returncode}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise
    except Exception as e:
        print(f"Unexpected error in {task_name}: {str(e)}")
        raise

def run_new_org_hierarchy_computation(**context):
    """New Org Hierarchy Computation"""
    execution_date = context['ds']
    script_path = '/home/analytics/pyspark/adhoc_test_jobs/stage-2/org_hierarchy_export/org_hierarchy_new.py'
    return run_python_script(script_path, execution_date, "new_org_hierarchy_computation")

def run_new_org_hierarchy_parquet_creation(**context):
    """New Org Hierarchy parquet Creation"""
    execution_date = context['ds']
    script_path = '/home/analytics/pyspark/adhoc_test_jobs/stage-2/org_hierarchy_export/postgres_to_parquet.py'
    return run_python_script(script_path, execution_date, "new_org_hierarchy_parquet_creation")

# Task definitions
new_org_hierarchy_start = EmptyOperator(
    task_id='start_new_org_hierarchy_pipeline',
    dag=new_org_hierarchy_dag,
)

new_org_hierarchy_computation_task = PythonOperator(
    task_id='new_org_hierarchy_computation',
    python_callable=run_new_org_hierarchy_computation,
    dag=new_org_hierarchy_dag,
)

new_org_hierarchy_parquet_creation_task = PythonOperator(
    task_id='new_org_hierarchy_parquet_creation',
    python_callable=run_new_org_hierarchy_parquet_creation,
    dag=new_org_hierarchy_dag,
)

upload_to_bq_task = BashOperator(
    task_id='upload_parquet_to_BQ',
    bash_command=(
        'bash /home/analytics/pyspark/adhoc_test_jobs/stage-2/org_hierarchy_export/bq-push.sh'
    ),
    dag=new_org_hierarchy_dag,
)


end_org_hierarchy_pipeline = EmptyOperator(
    task_id='new_org_hierarchy_pipeline_complete',
    dag=new_org_hierarchy_dag,
)

# Simple dependency chain
new_org_hierarchy_start >> new_org_hierarchy_computation_task >> new_org_hierarchy_parquet_creation_task >> upload_to_bq_task >> end_org_hierarchy_pipeline

