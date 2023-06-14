install() {

    echo "Install Minio Container"
    docker run --name minio -d -p 9000:9000 -p 9001:9001 -v "$PWD/datalake:/data" minio/minio server /data --console-address ":9001"

    echo "Install Airflow Docker Container..."
    docker run -d -p 8080:8080 -v "$PWD/airflow/dags:/opt/airflow/dags/" --entrypoint=/bin/bash --name airflow apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password admin --firstname Danilo --lastname Lastname --role Admin --email admin@example.org); airflow webserver & airflow scheduler'

    #Conectando ao Container do Airflow
    docker container exec -it airflow bash
    #Instalando as bibliotecas para não ter erros nas Dags
    pip install pymysql xlrd openpyxl minio
    pip install pyspark findspark install-jdk
    #Instalando Java no Airflow
    docker container exec -it -u root airflow bash
    apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;
    #Adicionando as variaveis no Airflow
    # data_lake_server = 
    # data_lake_login = minioadmin
    # data_lake_password = minioadmin

    echo "Install Anaconda Docker Container..."
    docker run -i -t -p 8888:8888 continuumio/anaconda3 /bin/bash -c "\
    conda install jupyter -y --quiet && \
    mkdir -p /opt/notebooks && \
    jupyter notebook \
    --notebook-dir=/opt/notebooks --ip='*' --port=8888 \
    --no-browser --allow-root"
    #Para acessar o Jupyter Notebook com os arquivos locais
    jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root

    #Docker move files Airflow to Root
    docker cp airflow:/opt/airflow/dataMinio/curated .

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