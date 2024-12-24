from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.sensors.filesystem import FileSensor
import pendulum

AIRBYTE_CONNECTION_ID = '9e9d48ff-ef0a-453a-9448-561216d67933'
#RAW_PRODUCTS_FILE = '/tmp/airbyte_local/json_from_faker/_airbyte_raw_products.jsonl'
#COPY_OF_RAW_PRODUCTS = '/tmp/airbyte_local/json_from_faker/moved_raw_products.jsonl'

with DAG(dag_id='airbyte_example_airflow_dag',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

   trigger_airbyte_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_sync',
       airbyte_conn_id='airbyte_conn',
       connection_id=AIRBYTE_CONNECTION_ID,
       asynchronous=True
   )

   trigger_airbyte_sync