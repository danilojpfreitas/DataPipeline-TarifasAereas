# Data Pipeline ETL com as principais features (Airflow, PySpark, Snowflake)
## Toda a descrição (passo a passo) deste projeto está disponibilizado neste post: [Medium]()

---

Neste projeto foi construido um Data Pipeline ETL completo para a avaliação dos preços das passagens áreas de Maceió para outras cidades do Brasil → Inclusive todo o processo pode ser replicado para outras cidades do Brasil e do Mundo!

## :bulb: Data Pipeline

![O Data Pipeline de ponta a ponta](img/dataPipeline.png)

# :memo: Etapas do projeto:
## :file_folder: Extração dos dados

Os dados utilizados foram extraídos diretamente do acervo público disponível pela Agência Nacional de Aviação Civil (ANAC).

![Extração dos dados](img/precoPassagensAereas.png)

## :fast_forward: Transformação dos dados (Airflow, Minio e PySpark)

Para a modelagem/transformação dos dados foi desenvolvido dentro de um notebook (ambiente de desenvolvimento) os comandos com o PySpark para a modelagem das tabelas. Realizando o tratamento dos Schemas, junção de tabelas e seleção dos dados utilizados por esse projeto.

![Jupyter Notebook](img/notebookJupyter.png)

O Data Lake é composto pelas seguintes etapas:

![Transformação dos dados](img/passagensAereasProjectTransform.png)

1. Landing: Todos as tabelas foram extraídos com o PySpark;
2. Processing: Os Schemas e suas propriedades foram ajusatadas e todas as tabelas foram depositadas no formato Parquet;
3. Curated: Transformação/Modelagem das tabelas para o seu envio ao Snowflake (Data Warehouse).

Após a etapa de desenvolvimento do código PySpark, foi construido dentro de uma DAG do Airflow a orquestração do processo. Da captação das tabelas em diferentes formatos (CSV e JSON), transformação/modelagem até o seu depósito no Snowflake.

Ambiente Airflow com a orquestração do processo ETL:

![Transformação dos dados](img/airflow.png)

## :snowflake: Snowflake

Após a realização da extração e transformação, 4 tabelas foram carregadas e disponibilizadas no Snowflake.

![Carregamento dos dados](img/passagensAereasProjectLoading.png)

![Etapa Snowflake](img/snowflake.png)

## :eye: Visualização dos dados pelo Metabase

Com a chegada de todos os dados no Snowflake (Data Warehouse), as tabelas foram conectadas com o Metabase para a criação de um ambiente de visualização.

![Visualização dos dados](img/passagensAereasProjectVisualisation.png)

![Metabse](img/metabase.png)

## :rocket: Conclusão

A partir da construção desse Data Pipeline ETL é possível extrair importantes insights sobre os preços e outras variáveis das passagens aéreas pelo Brasil ou porque não pelo Mundo!

Todo processo é orquestrado pelo Airflow permitindo a atualização constante dos dados ao passar dos anos. Além disso o ambiente do Snowflake permite uma rápida conexão para que outros estudiosos de dados possam criar mais insights!

---
