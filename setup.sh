install() {

    echo "Install Minio Container"
    docker run --name minio -d -p 9000:9000 -p 9001:9001 -v "$PWD/datalake:/data" minio/minio server /data --console-address ":9001"

    echo "Install Airflow..."
    mkdir airflow
    cd airflow
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.1/docker-compose.yaml'
    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    docker-compose up airflow-init
    docker compose up
    wget https://raw.githubusercontent.com/danilojpfreitas/DataPipeline-airbyte-dbt-airflow-snowflake-metabase/main/airflow/.gitignore
    cd ..

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