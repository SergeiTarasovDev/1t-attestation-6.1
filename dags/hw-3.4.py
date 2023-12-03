from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import modules.functions as func

default_args = {
    'start_date': '2023-12-02',
    'owner': 'starasov'
}

dag = DAG(
    dag_id='hw-3.4',
    default_args=default_args,
    schedule_interval=None
    # schedule_interval='*/10 * * * *'
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

task_bash = BashOperator(
    task_id='task_bash',
    bash_command=f'echo "Good morning my diggers"',
    dag=dag
)

create_rate_table = PostgresOperator(
    task_id='create_rate_table',
    postgres_conn_id='conn1',
    sql="sql\create_rate.sql",
    dag=dag
)

load_data_psql = PythonOperator(
    task_id='load_data_psql',
    python_callable=func.load_data_psql,
    dag=dag
)

start >> task_bash >> create_rate_table >> load_data_psql >> end
