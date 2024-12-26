from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# Configurações gerais da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'execute_bq_procedure',
    default_args=default_args,
    description='DAG to execute BigQuery procedure dw_silver.proc_CATEGORIES()',
    schedule_interval=None,  # Define o agendamento como manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Tarefa para executar a procedure no BigQuery
    execute_bq_procedure = BigQueryInsertJobOperator(
        task_id='call_bigquery_procedure',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_CATEGORIES()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    execute_bq_procedure
