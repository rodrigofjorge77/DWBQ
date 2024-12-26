from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy_operator import DummyOperator
import pendulum

AIRBYTE_CONNECTION_ID = '4403bff2-913c-4387-8d0c-38e380add0f8'

with DAG(dag_id='DW_Big_Query_Airbyte_Orchestration',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

        trigger_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_trigger_sync',
        airbyte_conn_id='airbyte_conn',
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=False
        )

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

wait1 = DummyOperator(task_id='wait1', trigger_rule='all_success')
wait2 = DummyOperator(task_id='wait2', trigger_rule='all_success')

trigger_airbyte_sync >> wait1 >> [silver_Categories,silver_Customers,silver_EmployeesTerritories,silver_Employees,silver_Products] >> wait2 >> [silver_Region,silver_Shippers,silver_Territories,silver_Orders,silver_OrdersDetails]

    