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
    silver_Categories = BigQueryInsertJobOperator(
        task_id='silver_Categories',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_CATEGORIES()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    silver_Customers = BigQueryInsertJobOperator(
        task_id='silver_Customers',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_CUSTOMERS()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    silver_EmployeesTerritories = BigQueryInsertJobOperator(
        task_id='silver_EmployeesTerritories',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_EMPLOYEE_TERRITORIES()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    silver_Employees = BigQueryInsertJobOperator(
        task_id='silver_Employees',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_EMPLOYEES()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    silver_Products = BigQueryInsertJobOperator(
        task_id='silver_Products',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_PRODUCTS()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    silver_Region = BigQueryInsertJobOperator(
        task_id='silver_Region',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_REGION()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    silver_Shippers = BigQueryInsertJobOperator(
        task_id='silver_Shippers',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_SHIPPERS()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    silver_Territories = BigQueryInsertJobOperator(
        task_id='silver_Territories',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_TERRITORIES()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    silver_Orders = BigQueryInsertJobOperator(
        task_id='silver_Orders',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_ORDERS()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

    silver_OrdersDetails = BigQueryInsertJobOperator(
        task_id='silver_OrdersDetails',
        configuration={
            "query": {
                "query": "CALL `dwbq-445617`.dw_silver.proc_ORDER_DETAILS()",
                "useLegacySql": False,  # Define o uso de SQL padrão
            }
        },
        location="us",  # Substitua pela região onde o BigQuery está configurado
        gcp_conn_id="google_cloud_default",  # Nome da conexão configurada no Airflow
    )

silver_Categories>>silver_Customers>>silver_EmployeesTerritories>>silver_Employees>>silver_Products>>silver_Region>>silver_Shippers>>silver_Territories>>silver_Orders>>silver_OrdersDetails
