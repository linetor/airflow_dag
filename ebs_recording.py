from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models.variable import Variable
from datetime import datetime, timedelta
import pendulum
import requests
import os

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

script_loc =  get_vault_configuration('ebs_radio')['scrip_loc']
python_loc =  get_vault_configuration('ebs_radio')['python_loc']

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'linetor',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 19, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': True,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_ebs_recording_scrip',
    default_args=default_args,
    catchup=False,
    description='Run ebs_recording Python script on rasp 3 server',
    schedule_interval='20 06 * * 1-6 ',
)

import pytz
current_time = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d_%H:%M')

run_script_task = SSHOperator(
    task_id='run_script',
    ssh_conn_id='ssh_rasp4',  # Airflow Connection에서 설정한 SSH 연결 ID 입력
    command=f'{python_loc} {script_loc} --start_time_str="{current_time}"',  # 실행할 Python 스크립트 경로 입력
    environment={
        'VAULT_ADDR': vault_addr,
        'VAULT_TOKEN' : vault_token,
    },
    cmd_timeout=1800,     
    dag=dag,
)
