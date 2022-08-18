import datetime
import configparser
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from app.etl_funcs import spotify_etl

config = configparser.ConfigParser()
config.read("dag_config.ini")


WORKFLOW_DAG_ID = config["main_dag"].get("dag_id")
WORKFLOW_START_DATE = datetime.datetime(2022, 8, 18)
WORKFLOW_SCHEDULE_INTERVAL = datetime.timedelta(hours=1)
WORKFLOW_EMAIL = [config["main_dag"].get("workflow_email")]

WORKFLOW_DEFAULT_ARGS = {
    "owner": "merlin.schaefer",
    "depends_on_past": False,
    "start_date": WORKFLOW_START_DATE,
    "email": WORKFLOW_EMAIL,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=10),
}

# Initialize DAG
dag = DAG(
    dag_id=WORKFLOW_DAG_ID,
    description="spotify etl from spotify api to postgres",
    default_args=WORKFLOW_DEFAULT_ARGS,
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    catchup=False,
    tags=["spotify", "etl"],
)

job_spotify_etl_operator = PythonOperator(
    task_id="job_spotify_etl",
    python_callable=spotify_etl,
    dag=dag,
)

job_spotify_etl_operator
