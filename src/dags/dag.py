from unicodedata import name
from airflow.decorators import task
from airflow.decorators import dag
from datetime import datetime, timedelta
from utils import *


@task
def fetch_group_log() -> None:
    get_file(name="group_log.csv")


@task
def create_staging() -> None:
    path = f'{sql_files_path}/create_staging.sql'
    query = read_sql_query_from_a_file(path=path)
    run_sql_query(query=query)


@task
def load_group_log():
    query = query_copy_data.format(
        name='group_log',
        schema=STG
    )
    run_sql_query(query=query)


@task
def create_links() -> None:
    path = f'{sql_files_path}/create_links.sql'
    query = read_sql_query_from_a_file(path=path)
    run_sql_query(query=query)


@task
def load_links() -> None:
    path = f'{sql_files_path}/load_links.sql'
    query = read_sql_query_from_a_file(path=path)
    run_sql_query(query=query)

@task
def create_satelites() -> None:
    path = f'{sql_files_path}/create_satelites.sql'
    print(path)
    query = read_sql_query_from_a_file(path=path)
    print(query)
    run_sql_query(query=query)


@task
def load_satelites() -> None:
    path = f'{sql_files_path}/load_satelites.sql'
    query = read_sql_query_from_a_file(path=path)
    run_sql_query(query=query)


@dag(
    default_args={
        "owner": "student",
        "email": ["student@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "s3_conn_id": "s3_hook"
    },
    schedule_interval=None,
    start_date=datetime(2022, 7, 29),
    catchup=False,
    tags=["social network"]
)
def etl_group_log():
    create_staging() >> fetch_group_log() >> load_group_log() >> create_links() >> load_links() >> create_satelites() >> load_satelites()


t = etl_group_log()
