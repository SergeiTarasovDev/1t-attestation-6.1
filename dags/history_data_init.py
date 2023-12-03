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
    dag_id='history_data_init',
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

create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='conn1',
    sql="sql/create_stock_quotes.sql",
    dag=dag
)

load_data_psql = PythonOperator(
    task_id='load_data_psql',
    python_callable=func.load_data_psql,
    dag=dag
)

start >> create_tables >> load_data_psql >> end
