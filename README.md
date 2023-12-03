К сожалению мне не удалось завести Airflow 2ой версии на своем ПК, поэтому работу сделал с использованием Airflow 1.10.15

Connections:
{
  "conn1": {
    "conn_type": "postgres",
    "description": "",
    "login": "postgres",
    "password": "postgres",
    "host": "host.docker.internal",
    "port": 5434,
    "schema": "quotes",
    "extra": "{}"
  }
}

Variables:
{
    "conn_name": "conn1",
    "API_KEY": "https://randomuser.me/api/",
    "RATE_BASE": "BTC",
    "RATE_TARGET": "USD",
    "RATE_URL_API": "9320603ab554ad114bd43b3d4b5905c0"
}