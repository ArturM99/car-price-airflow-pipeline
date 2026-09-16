import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = os.path.expanduser('~/car_price_airflow_pipeline')
# Add the project code path to the environment variable so it's available to the python process
os.environ['PROJECT_PATH'] = path
os.environ['PYTHONPATH'] = path
# Add the project code path to $PATH to import functions
sys.path.insert(0, path)


from modules.pipeline import pipeline
from modules.predict import predict

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule="00 15 * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
    )

    predict_task = PythonOperator(
        task_id='predict',
        python_callable=predict,
    )

    pipeline >> predict_task

