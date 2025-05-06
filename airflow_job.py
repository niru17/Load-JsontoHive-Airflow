from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'start_date':datetime(2025,5,5),
}

dag=DAG(
    'Load_json_to_hive',
    default_args=default_args,
    description='Loading JSON from GCS Bucket to Hive',
    catchup=False,
    tags=['dev'],
    schedule_interval=timedelta(days=1),
)

bucketname="spark_ex_airflow"
objectname="assignment2/data/Employee.json"
filename="/tmp/employee.json"

gcs_task=GCSToLocalFilesystemOperator(
    task_id="Download_File",
    bucket=bucketname,
    object_name=objectname,
    filename=filename,
    dag=dag,
)

CLUSTER_NAME='dataproc-hive'
PROJECT_ID='fit-legacy-454720-g4'
REGION='us-central1'

hive_task=DataprocSubmitJobOperator(
    task_id="Upload_to_Hive",
    job={
        "placement":{'cluster_name':CLUSTER_NAME},
        "pyspark_job":{"main_python_file_uri":f"gs://{bucketname}/assignment2/spark_job/spark_job.py"},
    },
    region=REGION,
    project_id=PROJECT_ID,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

gcs_task>>hive_task
