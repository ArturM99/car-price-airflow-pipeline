import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = '/opt/airflow/project'
os.environ['PROJECT_PATH'] = path
os.environ['PYTHONPATH'] = path
sys.path.insert(0, path)

def run_pipeline():
    from modules.pipeline import pipeline
    pipeline()

def run_predict():
    from modules.predict import predict
    predict()

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
    pipeline_task = PythonOperator(
        task_id='pipeline',
        python_callable=run_pipeline,
    )

    predict_task = PythonOperator(
        task_id='predict',
        python_callable=run_predict,
    )

    pipeline_task >> predict_task