# loglotate 사용으로 변경됨 - dag 삭제

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
# log_loc = "/opt/airflow/logs"
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

import os
def get_folder_list(log_loc):
    """
    지정된 경로에서 폴더만 리스트로 반환합니다.

    Args:
        path (str): 탐색할 경로

    Returns:
        list: 폴더 이름 리스트
    """
    folders = []
    for item in os.listdir(log_loc):
        item_path = os.path.join(log_loc, item)
        if os.path.isdir(item_path):
            folders.append(item_path)
    return folders

from datetime import datetime, timedelta

# 현재 날짜와 시간 가져오기
now = datetime.now()
# delete  날짜 계산
delete_days_ago = now - timedelta(days=14)
# "YYYY-MM-DD" 형식으로 변환
delete_date_string = delete_days_ago.strftime("%Y-%m-%d")

import shutil
def process_dag_logs(log_loc):
    folder_list = get_folder_list(log_loc)
    for folder in folder_list:
        logging.info(f"There is {folder} on airflow logs")
        try:
            if "dag_id=" in folder.split("/")[-1]:
                for check_folder in get_folder_list(folder):
                    if check_folder.split("/")[-1].split("__")[1][:10] <delete_date_string:
                        shutil.rmtree(check_folder)
            elif folder.split("/")[-1]=="scheduler":
                for check_folder in get_folder_list(folder):
                    if check_folder.split("/")[-1] <delete_date_string:
                        shutil.rmtree(check_folder)
            logging.info(f"{check_folder} 폴더가 성공적으로 삭제되었습니다.")
        except FileNotFoundError:
            logging.info(f"{check_folder} 폴더를 찾을 수 없습니다.")
        except PermissionError:
            logging.info(f"{check_folder} 폴더에 대한 삭제 권한이 없습니다.")

clean_log_task = PythonOperator(
    task_id="process_dag_logs",
    python_callable=process_dag_logs,
    op_kwargs={"log_loc": log_loc},
    dag=dag,
)

import pytz
current_time = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d_%H:%M')

def send_slack_message():
    import json
    url = get_vault_configuration('slack_alarm')['url']
    headers = {'Content-type': 'application/json'}

    data = {
        "text": f"airflow_dag_clean_log is done at {current_time} "
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))

send_slack_message_task = PythonOperator(
    task_id='send_slack_message',
    python_callable=send_slack_message,
    dag=dag,
)

clean_log_task >> send_slack_message_task
