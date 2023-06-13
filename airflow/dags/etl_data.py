from datetime import datetime,date, timedelta
import pandas as pd
from io import BytesIO
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio
import os

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, IntegerType 
from pyspark.sql import functions as f

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

client = Minio(
        data_lake_server,
        access_key=data_lake_login,
        secret_key=data_lake_password,
        secure=False  
      )

#Start Spark
findspark.init()

spark = SparkSession.builder.master('local[*]').getOrCreate()

def extract_dist():
  pathDist = "dataMinio/landing/dist.json"

  #extraindo dados a partir do Data Lake
  obj = client.fget_object(
                "landing",
                "maceioDistCapitals.json",
                pathDist
  )

  dist = spark.read.option("multiline", "true").json(pathDist)

  dist.show()

def extract_price():

  pricePath = [["price2019/2019", "dataMinio/landing/price2019" , 2019], ["price2020/2020", "dataMinio/landing/price2020", 2020],
                ["price2021/2021", "dataMinio/landing/price2021", 2021], ["price2022/2022", "dataMinio/landing/price2022", 2022]]

  #extraindo dados a partir do Data Lake
  for pathOrigin, pathDest, year in pricePath:
    for i in range(1, 13):
      if i < 10:
        pathOriginNumber = f"{pathOrigin}0{i}.CSV"
        pathDestNumber = f"{pathDest}/{year}0{i}.csv"
      else:
        pathOriginNumber = f"{pathOrigin}{i}.CSV"
        pathDestNumber = f"{pathDest}/{year}{i}.csv"

      print(pathOriginNumber)
      print(pathDestNumber)
      print(i)

      obj = client.fget_object(
                "landing",
                pathOriginNumber,
                pathDestNumber
      )

  price2019 = spark.read.csv("dataMinio/landing/price2019/*.csv", sep=';', inferSchema=True)
  price2020 = spark.read.csv("dataMinio/landing/price2020/*.csv", sep=';', inferSchema=True)
  price2021 = spark.read.csv("dataMinio/landing/price2021/*.csv", sep=';', inferSchema=True)
  price2022 = spark.read.csv("dataMinio/landing/price2022/*.csv", sep=';', inferSchema=True)

  price2019.show()
  price2020.show()
  price2021.show()
  price2022.show()

def extract_icao():
  pathICAO = "dataMinio/landing/icoa.json"

  #extraindo dados a partir do Data Lake
  obj = client.fget_object(
                "landing",
                "airports.json",
                pathICAO
  )

  icao = spark.read.option("multiline", "true").json(pathICAO)

  icao.printSchema()

  #transform!

  icao.select("icao", "city", "country").where('country=="Brazil"').limit(5).show()

  icaoAirportBrazil = icao.select("icao", "city", "country").where('country=="Brazil"')

  icaoAirportBrazilNotNull = icaoAirportBrazil.where('icao!=""').limit(60).show()

  icaoAirportBrazil.printSchema()

def transform_price():
  price2019 = spark.read.csv("dataMinio/landing/price2019/*.csv", sep=';', inferSchema=True)
  price2020 = spark.read.csv("dataMinio/landing/price2020/*.csv", sep=';', inferSchema=True)
  price2021 = spark.read.csv("dataMinio/landing/price2021/*.csv", sep=';', inferSchema=True)
  price2022 = spark.read.csv("dataMinio/landing/price2022/*.csv", sep=';', inferSchema=True)

  print("Renomenado o Header")
  newHeaderColNames = ['year', 'month', 'company', 'origin', 'destination', 'price', 'seats']

  for index, colName in enumerate(newHeaderColNames):
    price2019 = price2019.withColumnRenamed(f"_c{index}", colName)
    price2020 = price2020.withColumnRenamed(f"_c{index}", colName)
    price2021 = price2021.withColumnRenamed(f"_c{index}", colName)
    price2022 = price2022.withColumnRenamed(f"_c{index}", colName)
  
  price2020.limit(5).show()

  price2019 = price2019.where('price!="Tarifa-N"')
  price2020 = price2020.where('price!="Tarifa-N"')
  price2021 = price2021.where('price!="Tarifa-N"')
  price2022 = price2022.where('price!="Tarifa-N"')

  price2022.limit(5).show()

  print("Selecionando somente o aeroporto de Maceió como origem")
  price2019 = price2019.where('origin=="SBMO"')
  price2020 = price2020.where('origin=="SBMO"')
  price2021 = price2021.where('origin=="SBMO"')
  price2022 = price2022.where('origin=="SBMO"')

  price2020.limit(5).show()

  print("Convertendo: String to Integer and String to Double")
  print("2019")
  price2019.printSchema()

  price2019 = price2019\
    .withColumn(
    "year",
    price2019["year"].cast(IntegerType())
    )\
    .withColumn(
    "month",
    price2019["month"].cast(IntegerType())
    )\
    .withColumn(
    "seats",
    price2019["seats"].cast(IntegerType())
    )\
    .withColumn(
    "price",
    f.regexp_replace('price', ',', '.')
    )
  
  price2019 = price2019\
    .withColumn(
    "price",
    price2019["price"].cast(DoubleType())
    )

  price2019.printSchema()

  price2019.limit(10).show()

  print("2020")
  price2020 = price2020\
    .withColumn(
    "year",
    price2020["year"].cast(IntegerType())
    )\
    .withColumn(
    "month",
    price2020["month"].cast(IntegerType())
    )\
    .withColumn(
    "seats",
    price2020["seats"].cast(IntegerType())
    )\
    .withColumn(
    "price",
    f.regexp_replace('price', ',', '.')
    )
  
  price2020 = price2020\
  .withColumn(
  "price",
  price2020["price"].cast(DoubleType())
  )

  price2020.printSchema()

  print("2021")
  price2021 = price2021\
    .withColumn(
    "year",
    price2021["year"].cast(IntegerType())
    )\
    .withColumn(
    "month",
    price2021["month"].cast(IntegerType())
    )\
    .withColumn(
    "seats",
    price2021["seats"].cast(IntegerType())
    )\
    .withColumn(
    "price",
    f.regexp_replace('price', ',', '.')
    )

  price2021 = price2021\
    .withColumn(
    "price",
    price2021["price"].cast(DoubleType())
    )

  price2021.printSchema()

  print("2022")
  price2022 = price2022\
    .withColumn(
    "year",
    price2022["year"].cast(IntegerType())
    )\
    .withColumn(
    "month",
    price2022["month"].cast(IntegerType())
    )\
    .withColumn(
    "seats",
    price2022["seats"].cast(IntegerType())
    )\
    .withColumn(
    "price",
    f.regexp_replace('price', ',', '.')
    )

  price2022 = price2022\
    .withColumn(
    "price",
    price2022["price"].cast(DoubleType())
    )

  price2022.printSchema()


extract_task_dist = PythonOperator(
  task_id='extract_file_dist_from_data_lake',
  provide_context=True,
  python_callable=extract_dist,
  dag=dag
)

extract_task_price = PythonOperator(
  task_id='extract_file_price_from_data_lake',
  provide_context=True,
  python_callable=extract_price,
  dag=dag
)

extract_task_icao = PythonOperator(
  task_id='extract_file_icao_from_data_lake',
  provide_context=True,
  python_callable=extract_icao,
  dag=dag
)

transform_task_price = PythonOperator(
  task_id='transform_file_price',
  provide_context=True,
  python_callable=transform_price,
  dag=dag
)

extract_task_dist >> extract_task_price >> extract_task_icao >> transform_task_price


