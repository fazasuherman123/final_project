from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
from notify import task_success_callback, task_failure_callback
import os

# Default args
default_args = {
    "owner": "faza",
    "depends_on_past": False
}

# Common mounts for all DockerOperator tasks
dbt_mounts = [
    Mount(source="//d/Purwadhika/final_project/final_project", target="/opt/airflow/final_project", type="bind"),
    Mount(source="//d/Purwadhika/final_project/auth", target="/opt/airflow/auth", type="bind"),
]

with DAG(
    dag_id="final_project",
    default_args=default_args,
    description="ELT Final Project",
    start_date=datetime(2025, 8, 1),
    schedule='0 0 1 * *',
    catchup=False,
    tags=["jcdeol005"],
) as dag:

    # Start
    start = EmptyOperator(task_id="start")

    # Step 1: Data generator
    data_generator = BashOperator(
        task_id="create_data",
        bash_command="python /opt/airflow/scripts/data_generator.py",
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    # Step 2: Load to BigQuery
    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command="python /opt/airflow/scripts/load.py",
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    # Step 3: Create datasets
    create_datasets = BashOperator(
        task_id="create_datasets",
        bash_command="python /opt/airflow/scripts/create_dataset.py",
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    # Step 4: Transform staging data with dbt
    transform_data = DockerOperator(
        task_id="transform",
        image="xemuliam/dbt:1.10-bigquery",
        command=(
            "dbt run "
            "--project-dir /opt/airflow/final_project "
            "--profiles-dir /opt/airflow/final_project "
            "--select staging"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=dbt_mounts,
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/auth/purwadika.json",
            "DISCORD_WEBHOOK_URL": os.getenv("DISCORD_WEBHOOK_URL")
        },
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    # Step 5: Create star schema models
    model_task = DockerOperator(
        task_id="creating_star_schema",
        image="xemuliam/dbt:1.10-bigquery",
        command=(
            "dbt run "
            "--project-dir /opt/airflow/final_project "
            "--profiles-dir /opt/airflow/final_project "
            "--select model"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=dbt_mounts,
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/auth/purwadika.json",
            "DISCORD_WEBHOOK_URL": os.getenv("DISCORD_WEBHOOK_URL")
        },
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    # Step 6: Create data marts
    marts_task = DockerOperator(
        task_id="creating_data_marts",
        image="xemuliam/dbt:1.10-bigquery",
        command=(
            "dbt run "
            "--project-dir /opt/airflow/final_project "
            "--profiles-dir /opt/airflow/final_project "
            "--select marts"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=dbt_mounts,
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/auth/purwadika.json",
            "DISCORD_WEBHOOK_URL": os.getenv("DISCORD_WEBHOOK_URL")
        },
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    # End
    end = EmptyOperator(task_id="end")

    # DAG dependencies
    start >> data_generator >> load_to_bigquery >> create_datasets >> transform_data >> model_task >> marts_task >> end
