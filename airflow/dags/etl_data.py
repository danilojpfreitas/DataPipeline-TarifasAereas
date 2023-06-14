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

def transform_dist():
  pathDist = "dataMinio/landing/dist.json"

  dist = spark.read.option("multiline", "true").json(pathDist)

  #transform to parquet
  distParquet = "dataMinio/processing/dist/dist.parquet"

  dist.write.parquet(
    distParquet,
    mode = 'overwrite'
  )

  dist_parquet = spark.read.parquet(distParquet)

  dist_parquet.show()

  # result = client.fput_object(
  #   "processing",
  #   "dist.parquet",
  #   "/opt/airflow/dataMinio/processing/dist/dist.parquet"
  # )

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

  #tranform to Parquet
  #2019
  pathProcessingZone2019 = "dataMinio/processing/2019/2019.parquet"

  price2019.write.parquet(
    pathProcessingZone2019,
    mode = 'overwrite'
  )

  price2019Parquet = spark.read.parquet(
    pathProcessingZone2019
  )

  price2019Parquet.limit(5).show()

  #2020
  pathProcessingZone2020 = "dataMinio/processing/2020/2020.parquet"

  price2020.write.parquet(
    pathProcessingZone2020,
    mode = 'overwrite'
  )

  price2020Parquet = spark.read.parquet(
    pathProcessingZone2020
  )

  price2020Parquet.limit(5).show()

  #2021
  pathProcessingZone2021 = "dataMinio/processing/2021/2021.parquet"

  price2021.write.parquet(
    pathProcessingZone2021,
    mode = 'overwrite'
  )

  price2021Parquet = spark.read.parquet(
    pathProcessingZone2021
  )

  price2021Parquet.limit(5).show()

  #2022
  pathProcessingZone2022 = "dataMinio/processing/2022/2022.parquet"

  price2022.write.parquet(
    pathProcessingZone2022,
    mode = 'overwrite'
  )

  price2022Parquet = spark.read.parquet(
    pathProcessingZone2022
  )

  price2022Parquet.limit(5).show()

def transform_icao():
  pathICAO = "dataMinio/landing/icoa.json"

  icao = spark.read.option("multiline", "true").json(pathICAO)

  icao.select("icao", "city", "country").where('country=="Brazil"').limit(5).show()

  icaoAirportBrazil = icao.select("icao", "city", "country").where('country=="Brazil"')

  icaoAirportBrazilNotNull = icaoAirportBrazil.where('icao!=""').limit(60).show()

  icaoAirportBrazil.printSchema()

  #Transform to Parquet
  icaoParquet = "dataMinio/processing/icao/icao.parquet"

  icaoAirportBrazil.write.parquet(
    icaoParquet,
    mode = 'overwrite'
  )

  icao_parquet = spark.read.parquet(
    icaoParquet
  )

  icao_parquet.show()

def join_tables():
  price2019Path = "dataMinio/processing/2019/2019.parquet"
  price2020Path = "dataMinio/processing/2020/2020.parquet"
  price2021Path = "dataMinio/processing/2021/2021.parquet"
  price2022Path = "dataMinio/processing/2022/2022.parquet"
  distPath = "dataMinio/processing/dist/dist.parquet"
  icaoPath = "dataMinio/processing/icao/icao.parquet"

  price2019 = spark.read.parquet(price2019Path)
  price2020 = spark.read.parquet(price2020Path)
  price2021 = spark.read.parquet(price2021Path)
  price2022 = spark.read.parquet(price2022Path)
  dist = spark.read.parquet(distPath)
  icao = spark.read.parquet(icaoPath)

  #Price Views
  price2019.createOrReplaceTempView("2019View")
  price2020.createOrReplaceTempView("2020View")
  price2021.createOrReplaceTempView("2021View")
  price2022.createOrReplaceTempView("2022View")

  #ICAO View
  icao.createOrReplaceTempView("icaoView")

  #Dist View
  dist.createOrReplaceTempView("distView")

  #Join tables by year
  #2019
  data2019 = spark.sql("""
        SELECT 
        A.year, A.month, A.company, B.city, A.destination, A.price, A.seats, C.distLine AS distance
        FROM
        2019View A
        INNER JOIN
        icaoView B
        ON A.destination = B.icao
        INNER JOIN
        distView C
        ON B.city = C.city
        """)

  path2019Curated = "dataMinio/curated/2019.csv"

  data2019.write.option("header",True).option("delimiter", ",").mode("overwrite").csv(path2019Curated)

  #2020
  data2020 = spark.sql("""
        SELECT 
        A.year, A.month, A.company, B.city, A.destination, A.price, A.seats, C.distLine AS distance
        FROM
        2020View A
        INNER JOIN
        icaoView B
        ON A.destination = B.icao
        INNER JOIN
        distView C
        ON B.city = C.city
        """)
  
  path2020Curated = "dataMinio/curated/2020.csv"

  data2020.write.option("header",True).option("delimiter", ",").mode("overwrite").csv(path2020Curated)

  #2021
  data2021 = spark.sql("""
        SELECT 
        A.year, A.month, A.company, B.city, A.destination, A.price, A.seats, C.distLine AS distance
        FROM
        2021View A
        INNER JOIN
        icaoView B
        ON A.destination = B.icao
        INNER JOIN
        distView C
        ON B.city = C.city
        """)

  path2021Curated = "dataMinio/curated/2021.csv"

  data2021.write.option("header",True).option("delimiter", ",").mode("overwrite").csv(path2021Curated)

  #2022
  data2022 = spark.sql("""
          SELECT 
          A.year, A.month, A.company, B.city, A.destination, A.price, A.seats, C.distLine AS distance
          FROM
          2022View A
          INNER JOIN
          icaoView B
          ON A.destination = B.icao
          INNER JOIN
          distView C
          ON B.city = C.city
          """)

  path2022Curated = "dataMinio/curated/2022.csv"

  data2022.repartition(1).write.option("header",True).option("delimiter", ",").mode("overwrite").csv(path2022Curated)

  os.system('docker cp airflow:/opt/airflow/dataMinio/curated .')

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

transform_task_dist = PythonOperator(
  task_id='transform_file_dist',
  provide_context=True,
  python_callable=transform_dist,
  dag=dag
)

transform_task_price = PythonOperator(
  task_id='transform_file_price',
  provide_context=True,
  python_callable=transform_price,
  dag=dag
)

transform_task_icao = PythonOperator(
  task_id='transform_file_icao',
  provide_context=True,
  python_callable=transform_icao,
  dag=dag
)

join_tables_curated = PythonOperator(
  task_id='join_tables_curated',
  provide_context=True,
  python_callable=join_tables,
  dag=dag
)

extract_task_dist >> transform_task_dist
extract_task_price >> transform_task_price  
extract_task_icao >> transform_task_icao

[transform_task_dist, transform_task_price, transform_task_icao] >> join_tables_curated