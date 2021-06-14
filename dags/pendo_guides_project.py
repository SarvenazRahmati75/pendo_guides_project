from airflow import DAG
from datafy.operators import DatafyContainerOperator
from datetime import datetime, timedelta
from airflow.models import Variable

env = Variable.get("environment")
env_role = env if env == 'prd' else 'dev'
ingest_role = f"datafy-dp-{env_role}/cdo-pendoguides-ingest-{env_role}-role"
transform_role = f"datafy-dp-{env_role}/cdo-pendoguides-clean-{env_role}-role"
master_role = f"datafy-dp-{env_role}/cdo-pendoguides-master-{env_role}-role"

default_args = {
    "owner": "Datafy",
    "depends_on_past": False,
    "start_date": datetime(year=2021, month=5, day=26),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}
image = "{{ macros.datafy.image('pendo_guides_project') }}"

dag = DAG(
    "pendo_guides_project", default_args=default_args, schedule_interval="@daily", max_active_runs=1
)

extract = DatafyContainerOperator(
    dag=dag,
    task_id="extract",
    name="extract",
    image=image,
    arguments=[
        "/app/src/pendoguidesproject/app.py",
        "--date",
        "{{ ds }}",
        "--jobs",
        "ingest",
        "--env",
        "{{ macros.datafy.env() }}",
    ],
    annotations={
        "iam.amazonaws.com/role": ingest_role
    },
    instance_type="mx_xlarge",
    cmds=["python3"],
)

transform = DatafyContainerOperator(
    dag=dag,
    task_id="transform",
    name="transform",
    image=image,
    arguments=["--date", "{{ ds }}", "--jobs", "transform", "--env", "{{ macros.datafy.env() }}"],
    annotations={"iam.amazonaws.com/role": transform_role},
    resources={"request_memory": "8G", "request_cpu": "2", "limit_cpu": "2",},
)


def get_export_task(
    table_name,
):
    task_id = "load"
    return DatafyContainerOperator(
        dag=dag,
        task_id=task_id,
        name=task_id,
        env_vars={"AWS_REGION": "us-east-1", "environment": env},
        image=image,
        resources={
            "request_cpu": "1",
            "limit_memory": "2G",
        },
        annotations={"iam.amazonaws.com/role": master_role},
        cmds=["python3"],
        arguments=[
            "/app/src/pendoguidesproject/jobs/load.py",
            "--dataset",
            f"{table_name}",
        ]
    )

load = get_export_task("pendoguides")

extract >> transform >> load
