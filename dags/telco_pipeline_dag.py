from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

@dag(
    dag_id="telco_daily_pipeline",
    start_date=datetime(2026, 7, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False,
    tags=["telecom", "production"]
)
def telco_daily_pipeline():

    # 1. Perhatikan tambahan /script/ di path-nya
    # 2. Perhatikan tambahan argumen cwd="/opt/airflow/dags"
    
    check_files = BashOperator(
        task_id="check_raw_files",
        bash_command="bash /opt/airflow/dags/project/script/check_files.sh ",
        cwd="/opt/airflow/dags"
    )

    clean_data = BashOperator(
        task_id="clean_cdr_data",
        bash_command="python /opt/airflow/dags/project/script/clean_data.py ",
        cwd="/opt/airflow/dags"
    )

    load_db = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /opt/airflow/dags/project/script/load_to_db.py ",
        cwd="/opt/airflow/dags"
    )

    analytic_mart = BashOperator(
        task_id="generate_data_mart",
        bash_command="python /opt/airflow/dags/project/script/query_analytic.py ",
        cwd="/opt/airflow/dags"
    )

    # Atur urutan rantai kerjanya
    check_files >> clean_data >> load_db >> analytic_mart

telco_daily_pipeline()