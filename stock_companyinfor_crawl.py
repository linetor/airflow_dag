from airflow.models import DAG
from airflow.operators.python import PythonOperator
from configparser import ConfigParser
import pendulum

import datetime
date = datetime.datetime.now()
date_str = date.strftime('%Y-%m-%d')

import sys
#todo : need to check path

local_tz = pendulum.timezone("Asia/Seoul")
#args = {'owner': 'linetor', 'start_date': days_ago(n=1)}
args = {'owner': 'linetor', 'start_date': datetime.datetime(2021, 12, 3, tzinfo=local_tz)}

dag  = DAG(dag_id='stock_companyinfor_crawl',
           default_args=args,
           catchUp=False,
           schedule_interval="0 8 * * 0")

configparser = ConfigParser()
#todo : need to check path

import os
import sys
sys.path.append(os.environ["AIRFLOW_HOME"] + '/../airflow/dags/airflow_dag/')


from stock_crawl.StockCrawl import CrawlCompanyInfor

def call_crawl_companyinfor():
    crawlCompanyInfor = CrawlCompanyInfor()
    crawlCompanyInfor.insertCompanyInfor()
    del crawlCompanyInfor

stock_companyinfor_crawl_task = PythonOperator(
    task_id = 'stock_companyinfor_crawl_task',
    python_callable = call_crawl_companyinfor,
    dag = dag
)

from airflow.operators.dummy_operator import DummyOperator
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


start_task  >> stock_companyinfor_crawl_task  >> end_task



