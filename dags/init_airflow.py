from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'start_date': '2023-11-05',
    'owner': 'starasov'
}

dag = DAG(
    dag_id='airflow_init',
    default_args=default_args,
    schedule_interval=None
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

set_variables = BashOperator(
    task_id='set_variables',
    bash_command='airflow variables import /usr/local/airflow/input/variables.json',
    dag=dag
)

set_connections = BashOperator(
    task_id='set_connections',
    bash_command='airflow connections add --conn-host "host.docker.internal" --conn-login "postgres" --conn-password "postgres" --conn-port 5434 --conn-schema "quotes" --conn-type "postgres" conn1',
    dag=dag
)

start >> set_variables >> set_connections >> end
