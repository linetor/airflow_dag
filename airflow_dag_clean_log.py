from airflow import DAG
from airflow.models import DagBag
from airflow.models.variable import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pendulum
import requests
import os
import logging


vault_addr = Variable.get("VAULT_ADDR")
vault_token = Variable.get("VAULT_TOKEN")

def get_vault_configuration(endpoint):
    endpoint = f"{vault_addr}/v1/kv/data/{endpoint}"

    # HTTP GET 요청을 통해 데이터를 가져옵니다.
    headers = {"X-Vault-Token": vault_token}
    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data['data']['data']

    else:
        # 에러 응답의 경우 예외를 발생시킵니다.
        response.raise_for_status()

log_loc =  get_vault_configuration('airflow')['log_loc']
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'linetor',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 25, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': True,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_airflow_dag_clean_log',
    default_args=default_args,
    catchup=False,
    description='clean airflow log',
    schedule_interval='00 22 * * 1-6',
)

def get_all_dag_ids():
    dagbag = DagBag()
    return dagbag.dag_ids

def process_dag_logs(dag_id):
    logging.info(f"There is {dag_id} on airflow")
    # 각 DAG의 로그 처리 로직 구현
    pass

dag_ids = get_all_dag_ids()

for dag_id in dag_ids:
    process_task = PythonOperator(
        task_id=f'process_{dag_id}_logs',
        python_callable=process_dag_logs,
        op_args=[dag_id],
        dag=dag,
    )