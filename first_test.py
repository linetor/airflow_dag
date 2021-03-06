#reference : https://m.blog.naver.com/PostView.naver?isHttpsRedirect=true&blogId=wideeyed&logNo=221565240108
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

args = {'owner': 'linetor', 'start_date': days_ago(n=1)}
dag  = DAG(dag_id='my_first_dag',
           default_args=args,
           catchup=False,
           schedule_interval='@daily')

t1 = BashOperator(task_id='print_date',
                  bash_command='date',
                  dag=dag)

t2 = BashOperator(task_id='sleep',
                  bash_command='sleep 3',
                  dag=dag)

t3 = BashOperator(task_id='print_whoami',
                  bash_command='whoami',
                  dag=dag)

t1 >> t2 >> t3
