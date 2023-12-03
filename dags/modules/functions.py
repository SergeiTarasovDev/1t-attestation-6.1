import requests
from datetime import date, datetime, timedelta
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

conn_id = Variable.get("conn_name")
API_URL = Variable.get("API_URL")
API_KEY = Variable.get("API_KEY")
symbols = {'IBM': 'International Business Machines Corp',
           'INTC': 'Intel Corp',
           'TSLA': 'Tesla Inc',
           'NFC.DEX': 'Netflix Inc'}
interval = '5min'  # 1min, 5min, 15min, 30min, 60min


def get_list():
    return requests.get(API_URL, params={'function': 'SYMBOL_SEARCH',
                                         'apikey': API_KEY,
                                         'keywords': 'Netflix'})


def get_url(symbol, month):
    api_method = 'query'
    url = API_URL + api_method

    return requests.get(url, params={'function': 'TIME_SERIES_INTRADAY',
                                     'symbol': symbol,
                                     'interval': interval,
                                     'outputsize': 'full',
                                     'month': '2023-10',
                                     'apikey': API_KEY})


def load_data_psql():
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    for symbol in symbols.keys():
        print(symbol)
        load_data_by_time(conn, symbol)
    print("Данные записаны")


def load_data_by_time(conn, symbol):
    time = date.today().replace(day=1)
    while time > date(1999, 12, 1):
        month = time.strftime("%Y-%m")
        load_data_by_symbol(conn, symbol, month)
        time = (time - timedelta(days=1)).replace(day=1)
        break


def load_data_by_symbol(conn, symbol, month):
    try:
        cursor = conn.cursor()
        print(cursor)
        response = get_url(symbol, month)
        print(response)
        data = response.json()
        print(data)
        if f'Time Series ({interval})' in data:
            for quote in data[f'Time Series ({interval})']:
                time = quote
                quote_vl = list(data[f'Time Series ({interval})'][quote].values())
                quote_open = quote_vl[0]
                quote_close = quote_vl[1]
                quote_high = quote_vl[2]
                quote_low = quote_vl[3]
                volume = quote_vl[4]

                query = f"""
                                INSERT INTO stock_quotes (datetime, symbol, quote_open, quote_close, quote_high, quote_low, volume)
                                VALUES ('{time}', '{symbol}', '{quote_open}', '{quote_close}', '{quote_high}', '{quote_low}', '{volume}');
                        """

                cursor.execute(query)
                conn.commit()
        cursor.close()
    except Exception as error:
        conn.rollback()
        raise Exception(f'Ошибка записи данных: {error}')
