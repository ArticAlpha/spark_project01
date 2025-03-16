from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the script paths (mounted in Docker at /opt/airflow/scripts)
scripts_dir = "/opt/airflow/scripts"
scripts = {
    "download_file": os.path.join(scripts_dir, "download/aws_file_download.py"),
    "cleaning": os.path.join(scripts_dir, "data_cleaning/data_cleaning.py"),
    "transformation": os.path.join(scripts_dir, "data_transformation/transformation.py"),
    "dimension": os.path.join(scripts_dir, "dimension_modeling/dimension.py"),
    "custom_dimension": os.path.join(scripts_dir, "dimension_modeling/custom_dimensions.py"),
    "fact": os.path.join(scripts_dir, "dimension_modeling/fact.py"),
    "upload_file": os.path.join(scripts_dir, "utility/S3_utilities/upload.py"),
}

with DAG(
        'spark_pipeline_dag',
        default_args=default_args,
        description='DAG to execute Spark scripts',
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
) as dag:
    # Define tasks
    download_file_task = BashOperator(
        task_id='download_file_from_s3',
        bash_command=f'python {scripts["download_file"]}',
    )

    cleaning_task = BashOperator(
        task_id='clean_data',
        bash_command=f'python {scripts["cleaning"]}',
    )

    transformation_task = BashOperator(
        task_id='transform_data',
        bash_command=f'python {scripts["transformation"]}',
    )

    dimension_task = BashOperator(
        task_id='run_dimension_modeling',
        bash_command=f'python {scripts["dimension"]}',
    )

    custom_dimension_task = BashOperator(
        task_id='run_custom_dimension_modeling',
        bash_command=f'python {scripts["custom_dimension"]}',
    )

    fact_task = BashOperator(
        task_id='run_fact_modeling',
        bash_command=f'python {scripts["fact"]}',
    )

    upload_file_task = BashOperator(
        task_id='upload_to_s3',
        bash_command=f'python {scripts["upload_file"]}',
    )

    # Set task dependencies
    (
            download_file_task
            >> cleaning_task
            >> transformation_task
            >> dimension_task
            >> custom_dimension_task
            >> fact_task
            >> upload_file_task
    )
