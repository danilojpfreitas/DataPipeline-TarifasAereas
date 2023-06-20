from datetime import datetime,date, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType 
from pyspark.sql import functions as f
import findspark

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

spark = SparkSession.builder.appName("Snowflake-Connection").master('local[*]').config("spark.jars", "/opt/airflow/path/spark-snowflake_2.12-2.9.3-spark_3.1.jar,/opt/airflow/path/snowflake-jdbc-3.13.10.jar").getOrCreate()
spark.sparkContext.addPyFile("/opt/airflow/path/spark-snowflake_2.12-2.9.3-spark_3.1.jar")
spark.sparkContext.addPyFile("/opt/airflow/path/snowflake-jdbc-3.13.10.jar")

SNOWFLAKE_SOURCE_NAME= "net.snowflake.spark.snowflake"

#snowflake
sfOptions = {
  "sfURL" : "***",
  "sfAccount" : "***",
  "sfUser" : "***",
  "sfPassword" : "***",
  "sfDatabase" : "***",
  "sfSchema" : "***",
  "sfWarehouse" : "***"
}

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
  pathICAO = "dataMinio/landing/icao/icao.csv"

  #extraindo dados a partir do Data Lake
  obj = client.fget_object(
                "landing",
                "icao/icao.csv",
                pathICAO
  )

def extract_statistics():
  pathStatistics = 'dataMinio/landing/statistics/Dados_Estatisticos.csv'

  #extraindo dados a partir do Data Lake
  obj = client.fget_object(
                "landing",
                "statistics/Dados_Estatisticos.csv",
                pathStatistics
  )

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

  #2020
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

  #2021
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

  #2022
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

  #Tranform to Parquet
  #2019
  pathProcessingZone2019 = "dataMinio/processing/2019"

  price2019.write.parquet(
    pathProcessingZone2019,
    mode = 'overwrite'
  )

  price2019Parquet = spark.read.parquet(
    pathProcessingZone2019
  )

  price2019Parquet.limit(5).show()

  #2020
  pathProcessingZone2020 = "dataMinio/processing/2020"

  price2020.write.parquet(
    pathProcessingZone2020,
    mode = 'overwrite'
  )

  price2020Parquet = spark.read.parquet(
    pathProcessingZone2020
  )

  price2020Parquet.limit(5).show()

  #2021
  pathProcessingZone2021 = "dataMinio/processing/2021"

  price2021.write.parquet(
    pathProcessingZone2021,
    mode = 'overwrite'
  )

  price2021Parquet = spark.read.parquet(
    pathProcessingZone2021
  )

  price2021Parquet.limit(5).show()

  #2022
  pathProcessingZone2022 = "dataMinio/processing/2022"

  price2022.write.parquet(
    pathProcessingZone2022,
    mode = 'overwrite'
  )

  price2022Parquet = spark.read.parquet(
    pathProcessingZone2022
  )

  price2022Parquet.limit(5).show()

def transform_icao():
  pathICAO = "dataMinio/landing/icao"

  icao = spark.read.csv(pathICAO, sep=',', header=True)

  #Transform to Parquet
  icaoParquet = "dataMinio/processing/icao"

  icao.write.parquet(
      icaoParquet,
      mode = 'overwrite'
  )

  icao_parquet = spark.read.parquet(
      icaoParquet
  )

  icao_parquet.show()

def transform_statistics():
  pathStatistics = 'dataMinio/landing/statistics'

  estatisticos = spark.read.csv(pathStatistics, sep=';', header=True)

  estatisticos.printSchema()

  estatisticos.createOrReplaceTempView('estatisticosSQLView')

  #Select some Columns
  estatisticosSelect = spark.sql("""
        SELECT
        EMPRESA_SIGLA AS company,
        EMPRESA_NOME AS companyName,
        EMPRESA_NACIONALIDADE AS nationality,
        ANO AS year,
        MES AS month,
        AEROPORTO_DE_ORIGEM_SIGLA AS origin,
        AEROPORTO_DE_DESTINO_SIGLA AS destination,
        DECOLAGENS AS takeOffs,
        DISTANCIA_VOADA_KM AS distance,
        ASSENTOS AS seats,
        HORAS_VOADAS AS time,
        PASSAGEIROS_PAGOS AS paidPassengers,
        PASSAGEIROS_GRATIS AS noPaidPassengers
        FROM
        estatisticosSQLView
  """)

  estatisticosSelect.limit(5).show()

  estatisticosSelect = estatisticosSelect\
    .withColumn(
    "year",
    estatisticosSelect["year"].cast(IntegerType())
    )\
    .withColumn(
    "month",
    estatisticosSelect["month"].cast(IntegerType())
    )\
    .withColumn(
    "takeOffs",
    estatisticosSelect["takeOffs"].cast(IntegerType())
    )\
    .withColumn(
    "distance",
    estatisticosSelect["distance"].cast(IntegerType())
    )\
    .withColumn(
    "seats",
    estatisticosSelect["seats"].cast(IntegerType())
    )\
    .withColumn(
    "time",
    f.regexp_replace('time', ',', '.')
    )\
    .withColumn(
    "paidPassengers",
    estatisticosSelect["paidPassengers"].cast(IntegerType())
    )\
    .withColumn(
    "noPaidPassengers",
    estatisticosSelect["noPaidPassengers"].cast(IntegerType())
    )
  
  estatisticosSelect.printSchema()

  #Select years between 2019 to 2022
  estatisticos2019To2022MCZ = estatisticosSelect.where('year >= 2019').where('year <= 2022').where('origin == "SBMO"')

  estatisticos2019To2022MCZ.count()

  #Transform to Parquet
  statisticsParquet = "dataMinio/processing/statistics"

  estatisticos2019To2022MCZ.write.parquet(
    statisticsParquet,
    mode = 'overwrite'
  )


def to_snowflake():
  price2019Path = "dataMinio/processing/2019/*.parquet"
  price2020Path = "dataMinio/processing/2020/*.parquet"
  price2021Path = "dataMinio/processing/2021/*.parquet"
  price2022Path = "dataMinio/processing/2022/*.parquet"
  statisticsPath = "dataMinio/processing/statistics/*.parquet"
  icaoPath = "dataMinio/processing/icao/*.parquet"

  price2019 = spark.read.parquet(price2019Path)
  price2020 = spark.read.parquet(price2020Path)
  price2021 = spark.read.parquet(price2021Path)
  price2022 = spark.read.parquet(price2022Path)
  statistics = spark.read.parquet(statisticsPath)
  icao = spark.read.parquet(icaoPath)

  #Price Views
  price2019.createOrReplaceTempView("2019View")
  price2020.createOrReplaceTempView("2020View")
  price2021.createOrReplaceTempView("2021View")
  price2022.createOrReplaceTempView("2022View")

  #Statistics View
  statistics.createOrReplaceTempView("statisticsView")

  #ICAO View
  icao.createOrReplaceTempView("icaoView")

  #Join tables by year
  #2019
  avgPrice2019 = spark.sql("""
        SELECT
        year, month, company, origin, destination, 
        ROUND((SUM(price*seats)/SUM(seats)), 2) AS priceAVG, SUM(seats) AS seats
        FROM
        2019View    
        GROUP BY year, month, company, origin, destination
        ORDER BY month
        """)
  
  avgPrice2019.createOrReplaceTempView("avgPrice2019View")

  data2019 = spark.sql("""
        SELECT
        A.year, A.month, A.company, B.companyName, C.city,
        C.state, A.destination, A.priceAVG, A.seats AS seatsAVG, 
        B.takeOffs, B.distance, B.seats AS seatsAll,
        B.time
        FROM
        avgPrice2019View A
        LEFT JOIN
        (SELECT * FROM statisticsView WHERE year == 2019) B
        ON A.month = B.month AND A.company = B.company AND A.destination == B.destination
        LEFT JOIN
        icaoView C
        ON A.destination = C.icao
        """)
  
  data2019 = data2019.where(data2019.time.isNotNull())

  data2019.limit(5).show()

  data2019.count()

  path2019Curated = "dataMinio/curated/2019.csv"

  data2019.write.option("header",True).option("delimiter", ",").mode("overwrite").csv(path2019Curated)

  #Airflow
  data2019.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "data2019").mode("overwrite").save()


  #2020
  avgPrice2020 = spark.sql("""
          SELECT
          year, month, company, origin, destination, 
          ROUND((SUM(price*seats)/SUM(seats)), 2) AS priceAVG, SUM(seats) AS seats
          FROM
          2020View    
          GROUP BY year, month, company, origin, destination
          ORDER BY month
  """)

  avgPrice2020.createOrReplaceTempView("avgPrice2020View")

  data2020 = spark.sql("""
        SELECT
        A.year, A.month, A.company, B.companyName, C.city,
        C.state, A.destination, A.priceAVG, A.seats AS seatsAVG, 
        B.takeOffs, B.distance, B.seats AS seatsAll,
        B.time
        FROM
        avgPrice2020View A
        LEFT JOIN
        (SELECT * FROM statisticsView WHERE year == 2020) B
        ON A.month = B.month AND A.company = B.company AND A.destination == B.destination
        LEFT JOIN
        icaoView C
        ON A.destination = C.icao
  """)

  data2020 = data2020.where(data2020.time.isNotNull())
  
  path2020Curated = "dataMinio/curated/2020.csv"

  data2020.write.option("header",True).option("delimiter", ",").mode("overwrite").csv(path2020Curated)

  #Airflow
  data2020.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "data2020").mode("overwrite").save()


  #2021
  avgPrice2021 = spark.sql("""
          SELECT
          year, month, company, origin, destination, 
          ROUND((SUM(price*seats)/SUM(seats)), 2) AS priceAVG, SUM(seats) AS seats
          FROM
          2021View    
          GROUP BY year, month, company, origin, destination
          ORDER BY month
  """)

  avgPrice2021.createOrReplaceTempView("avgPrice2021View")

  data2021 = spark.sql("""
        SELECT
        A.year, A.month, A.company, B.companyName, C.city,
        C.state, A.destination, A.priceAVG, A.seats AS seatsAVG, 
        B.takeOffs, B.distance, B.seats AS seatsAll,
        B.time
        FROM
        avgPrice2021View A
        LEFT JOIN
        (SELECT * FROM statisticsView WHERE year == 2021) B
        ON A.month = B.month AND A.company = B.company AND A.destination == B.destination
        LEFT JOIN
        icaoView C
        ON A.destination = C.icao
  """)

  data2021 = data2021.where(data2021.time.isNotNull())

  path2021Curated = "dataMinio/curated/2021.csv"

  data2021.write.option("header",True).option("delimiter", ",").mode("overwrite").csv(path2021Curated)

  #Airflow
  data2021.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "data2021").mode("overwrite").save()


  #2022
  avgPrice2022 = spark.sql("""
          SELECT
          year, month, company, origin, destination, 
          ROUND((SUM(price*seats)/SUM(seats)), 2) AS priceAVG, SUM(seats) AS seats
          FROM
          2022View    
          GROUP BY year, month, company, origin, destination
          ORDER BY month
  """)

  avgPrice2022.createOrReplaceTempView("avgPrice2022View")

  data2022 = spark.sql("""
        SELECT
        A.year, A.month, A.company, B.companyName, C.city,
        C.state, A.destination, A.priceAVG, A.seats AS seatsAVG, 
        B.takeOffs, B.distance, B.seats AS seatsAll,
        B.time
        FROM
        avgPrice2022View A
        LEFT JOIN
        (SELECT * FROM statisticsView WHERE year == 2022) B
        ON A.month = B.month AND A.company = B.company AND A.destination == B.destination
        LEFT JOIN
        icaoView C
        ON A.destination = C.icao
  """)

  data2022 = data2022.where(data2022.time.isNotNull())

  path2022Curated = "dataMinio/curated/2022.csv"

  data2022.write.option("header",True).option("delimiter", ",").mode("overwrite").csv(path2022Curated)

  #Airflow
  data2022.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "data2022").mode("overwrite").save()

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

extract_task_statistics = PythonOperator(
  task_id='extract_file_statistics_from_data_lake',
  provide_context=True,
  python_callable=extract_statistics,
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

transform_task_statistics = PythonOperator(
  task_id='transform_file_statistics',
  provide_context=True,
  python_callable=transform_statistics,
  dag=dag
)

spark_to_snowflake = PythonOperator(
  task_id='to_snowflake',
  provide_context=True,
  python_callable=to_snowflake,
  dag=dag
)

extract_task_price >> transform_task_price  
extract_task_icao >> transform_task_icao
extract_task_statistics >> transform_task_statistics

[transform_task_price, transform_task_icao, transform_task_statistics] >> spark_to_snowflake