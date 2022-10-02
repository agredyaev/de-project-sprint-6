import boto3
import vertica_python
import os
import pathlib

# from sqlalchemy import text

# is used for education purposes, better options are: airflow connection settings and ariflow variables
AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
LOGIN = 'NEYBYARU'
STG = 'NEYBYARU__STAGING'
ENDPOIN_URL = 'https://storage.yandexcloud.net'
SERVICE_NAME = 's3'


# connection info
conn_info = {'host': '51.250.75.20',
             'port': '5433',
             'user': 'NEYBYARU',
             'password': '2jbeEkvqK2VWZmU',
             'database': 'dwh',
             'unicode_error': 'strict',
             'connection_load_balance': True,
             'autocommit': True
             }

# query which loads data from the local srote to the db
query_copy_data = """
        COPY {schema}.{name}
        FROM LOCAL '/data/{name}.csv'
        DELIMITER ',';
    """


def get_file(name: str) -> None:
    """Gets file from s3

    Args:
        name (str): name of the requested file
    """
    session = boto3.session.Session()
    s3_client = session.client(
        service_name=SERVICE_NAME,
        endpoint_url=ENDPOIN_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key=name,
        Filename=f'/data/{name}'
    )

sql_files_path = os.path.join(pathlib.Path(__file__).parent.absolute(), 'sql')

def read_sql_query_from_a_file(path: str) -> str:
    """Reads sql query from a file

    Args:
        path (str): path to sql file

    Returns:
        str: result
    """
    with open(path) as file:
        return file.read()


def run_sql_query(query: str) -> None:
    """Runs sql query

    Args:
        query (str): sql query
    """
    with vertica_python.connect(**conn_info) as con:
        with con.cursor() as cur:
            cur.execute(query)
        con.commit()
