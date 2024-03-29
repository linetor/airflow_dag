from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from configparser import ConfigParser
import pendulum

import datetime
date = datetime.datetime.now()
date_str = date.strftime('%Y-%m-%d')

import sys
#todo : need to check path

local_tz = pendulum.timezone("Asia/Seoul")
#args = {'owner': 'linetor', 'start_date': days_ago(n=1)}
args = {'owner': 'linetor', 'start_date': datetime.datetime(2021, 11, 16, tzinfo=local_tz)}

dag  = DAG(dag_id='ebs_radio_recording_easy_writing',
           default_args=args,
           catchup=False,
           schedule_interval="00 06 * * 1-6")

configparser = ConfigParser()
#todo : need to check path
import os
configparser.read( os.environ["AIRFLOW_HOME"]+'/../airflow/dags/airflow_dag/ebs_radio_cron/.config')
import sys
sys.path.append(os.environ["AIRFLOW_HOME"] + '/../airflow/dags/airflow_dag/')

radio_address = configparser.get('ebs_address', 'ebs_fm')
recording_loc = configparser.get('recording_loc', 'recording_loc')
record_mins = str(20*60)

program_name = "EASY_WRITING"
ori_file = recording_loc + date_str + '_' + program_name + '.mp3'


recoring_command = f"ffmpeg -t {record_mins} -y -i {radio_address}  {ori_file}"

print( recoring_command)

recording_task = BashOperator(task_id='recording',
                              bash_command=recoring_command,
                              dag=dag)


"""
recording_task = BashOperator(task_id='recording',
                              bash_command=recoring_command,
                              dag=dag)


format_command = f"ffmpeg -i {ori_file} -vn -acodec copy  {m4a_file}"

print(format_command)

formatting_task = BashOperator(task_id='formatting',
                               bash_command=format_command,
                               dag=dag)
"""

api_token = configparser.get('dropbox', 'api_token')
upload_loc = configparser.get('dropbox', 'upload_loc')
move_loc = configparser.get('dropbox', 'move_loc')

import dropbox
dbx = dropbox.Dropbox(api_token)

from ebs_radio_cron import recoding_by_shell
doing_dropbox = PythonOperator(
    task_id = 'doing_dropbox',
    python_callable = recoding_by_shell.upload_to_dropbox,
    op_kwargs={
        'dbx_variable':dbx ,
        'upload_loc_var':upload_loc ,
        'date_str_variable':date_str ,
        'move_loc_var': move_loc,
        'program_name_var': program_name,
        'current_loc_var':recording_loc,
    },
    dag = dag
)

delete_command = f"rm  {ori_file}*"
delete_task = BashOperator(task_id='delete_command',
                           bash_command=delete_command,
                           dag=dag)

from airflow.operators.dummy_operator import DummyOperator
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

#start_task >> recording_task >> formatting_task >> doing_dropbox >> delete_task >> end_task
start_task >> recording_task  >> doing_dropbox >> delete_task >> end_task



