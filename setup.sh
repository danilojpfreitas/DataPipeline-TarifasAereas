install() {

    echo "Install Minio Container"
    docker run --name minio -d -p 9000:9000 -p 9001:9001 -v "$PWD/datalake:/data" minio/minio server /data --console-address ":9001"

    echo "Install Airflow Docker Container..."
    docker run -d -p 8080:8080 -v "$PWD/airflow/dags:/opt/airflow/dags/" --entrypoint=/bin/bash --name airflow apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password admin --firstname Danilo --lastname Lastname --role Admin --email admin@example.org); airflow webserver & airflow scheduler'

    #Conectando ao Container do Airflow
    docker container exec -it airflow bash
    #Instalando as bibliotecas para não ter erros nas Dags
    pip install pymysql xlrd openpyxl minio
    #Adicionando as variaveis no Airflow
    # data_lake_server = 172.17.0.1:9000
    # data_lake_login = minioadmin
    # data_lake_password = minioadmin
    # database_server = 172.17.0.3 ( Use o comando inspect para descobrir o ip do
    # container: docker container inspect mysqlbd1 - localizar o atributo IPAddress)
    # database_login = root
    # database_password = stack
    # database_name = employees

    echo "Install Metabase..."
    mkdir metabase
    cd metabase
    wget https://raw.githubusercontent.com/danilojpfreitas/DataPipeline-airbyte-dbt-airflow-snowflake-metabase/main/metabase/docker-compose.yaml
    docker-compose up
    cd ..

    echo "Access Minio at http://localhost:9001 to Dashboard Minio."
  
    echo "Access Airflow at http://localhost:8080 to kick off your Airbyte sync DAG."  

    echo "Access Metabase at http://localhost:3000 and set up a connection with Snowflake."
}