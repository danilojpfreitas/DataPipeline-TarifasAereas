from datetime import datetime,date, timedelta
import pandas as pd
from io import BytesIO
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio
from sqlalchemy.engine import create_engine
import os


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
}

dag = DAG('etl_data', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

path_dataJSON = 'data/maceioDistCapitals.json'
path_dataCSV2019 = 'data/price2019/*.csv'
path_dataCSV2020 = 'data/price2020/*.csv'
path_dataCSV2021 = 'data/price2021/*.csv'
path_dataCSV2022 = 'data/price2022/*.csv'


