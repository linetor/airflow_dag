from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
import pendulum
import requests
import os

def get_vault_configuration(endpoint):
    vault_addr = os.environ.get("VAULT_ADDR")
    vault_token = os.environ.get("VAULT_TOKEN")
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

script_loc =  get_vault_configuration('ebs_radio')['scrip_loc']

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'linetor',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 19, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_ebs_recording_scrip',
    default_args=default_args,
    catchup=False,
    description='Run ebs_recording Python script on rasp 3 server',
    schedule_interval='20 06 * * 1-6 | 00 06 * * 1-6 | 40 06 * * 1-6 | 40 07 * * 1-6',
)



run_script_task = SSHOperator(
    task_id='run_script',
    ssh_conn_id='ssh_rasp3',  # Airflow Connection에서 설정한 SSH 연결 ID 입력
    command=f'python {script_loc}',  # 실행할 Python 스크립트 경로 입력
    dag=dag,
)