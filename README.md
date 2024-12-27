# DW Big Query - Arquitetura Medalhão

## Table of Contents
- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)
- [Getting Started](#getting-started)
- [License](#license)

## Overview

📊 Arquitetura do Projeto: O fluxo inicia com o Apache Airflow orquestrando o processo e acionando o Airbyte, responsável por extrair dados de diversas tabelas do Oracle e carregá-los na camada Bronze do Google BigQuery. Em seguida, o Airflow processa os dados nas camadas Silver e Gold, deixando-os disponíveis para análises em dashboards dinâmicos criados com o Google Looker.

## System Architecture

![System Architecture](https://github.com/rodrigofjorge77/DWBQ/blob/main/assets/architecture.png)

## Data Source Model in Oracle

![System Architecture](https://github.com/rodrigofjorge77/DWSnowflake/blob/main/Assets/schema%20das%20tabelas%20na%20orgiem.png)

## Airflow Pipeline

![System Architecture](https://github.com/rodrigofjorge77/DWBQ/blob/main/assets/airflow_airbyte2.png)

## Looker

![System Architecture](https://github.com/rodrigofjorge77/DWBQ/blob/main/assets/looker.png)

## Technologies

- Docker
- Oracle
- Airflow
- Airbyte
- Big Query
- Looker

## Getting Started

1. Clone the repository:
    ```bash
    https://github.com/rodrigofjorge77/DWBQ.git
    ```

2. Install Airbyte:  

    https://www.restack.io/docs/airbyte-knowledge-airbyte-docker-container-guide

3. Install Airflow:  

    https://airbyte.com/tutorials/how-to-use-airflow-and-airbyte-together
   
## License

This project is licensed under the MIT License

