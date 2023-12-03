import requests
from datetime import date, timedelta
from modules import constants as c

def raw_init_db():
    cursor = c.connection.cursor()
    query = '''CREATE SCHEMA IF NOT EXISTS raw;'''
    cursor.execute(query)

    query = ''' CREATE TABLE IF NOT EXISTS raw.stock_quotes(
                    quote_id             bigserial primary key,
                    quote_time           timestamp,
                    symbol               varchar(15),
                    quote_interval       varchar(10),
                    quote_open           float,
                    quote_close          float,
                    quote_high           float,
                    quote_low            float,
                    volume               float
                    );'''
    cursor.execute(query)
    query = ''' CREATE TABLE IF NOT EXISTS raw.symbols(
                    symbol_id       bigserial primary key,
                    symbol_code     varchar(15),
                    symbol_name     varchar(70),
                    symbol_type     varchar(30),
                    region          varchar(40),
                    market_open     varchar(10),
                    market_close    varchar(10),
                    timezone        varchar(10),
                    currency        varchar(10),
                    match_score     float
                    );'''
    cursor.execute(query)
    cursor.close()
    c.connection.commit()
    print("Таблицы БД инициализированы")


def get_symbols(keywords):
    return requests.get(c.API_URL, params={'function': 'SYMBOL_SEARCH',
                                         'apikey': c.API_KEY,
                                         'keywords': keywords})


def raw_extract_symbols():
    symbol_list = []
    for symbol in c.symbols:
        response = get_symbols(symbol)
        data = response.json()
        print(data)
        print(data['bestMatches'][0])
        print(data['bestMatches'][0].values())
        if 'bestMatches' in data:
            for entry in data['bestMatches']:
                print(entry)
                values = list(entry.values())
                symbol_code = values[0]
                symbol_name = values[1]
                symbol_type = values[2]
                region = values[3]
                market_open = values[4]
                market_close = values[5]
                timezone = values[6]
                currency = values[7]
                match_score = values[8]
                symbol_list.append(
                    tuple((
                        symbol_code,
                        symbol_name,
                        symbol_type,
                        region,
                        market_open,
                        market_close,
                        timezone,
                        currency,
                        match_score
                    ))
                )
    return symbol_list


def raw_load_symbols_test():
    cursor = c.connection.cursor()
    query = '''INSERT INTO raw.symbols 
        (symbol_code, symbol_name, symbol_type, region, market_open, market_close, timezone, currency, match_score)
        VALUES ('IBM', 'IBM name', 'IBM type', 'region', 'market_op', 'market_cl', 'timezone', 'currency', 0.223)'''
    cursor.execute(query)
    cursor.close()
    c.connection.commit()
    print("Данные загружены")


def raw_load_symbols(symbol_list):
    cursor = c.connection.cursor()
    query = (
        "INSERT INTO raw.symbols "
        "(symbol_code, symbol_name, symbol_type, region, market_open, market_close, timezone, currency, match_score) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    cursor.executemany(query, symbol_list)
    cursor.close()
    c.connection.commit()
    print("Данные загружены")


def get_quotes(symbol, month):
    return requests.get(c.API_URL, params={'function': 'TIME_SERIES_INTRADAY',
                                         'symbol': symbol,
                                         'interval': c.interval,
                                         'outputsize': 'full',
                                         'month': month,
                                         'apikey': c.API_KEY})


# Перебираю все акции и получаю по ним котировки
def raw_extract_quotes(month):
    for symbol in c.symbols:
        print(symbol)
        extract_quotes_by_symbol(symbol, month)
    print("Данные получены")


# API позволяет получать исторические данные по месяцам,
# тут перебираю разные периоды и получаю по ним котировки
# Не стал использовать из-за ограничения API в 25 запросов в день.
def extract_quotes_by_time(symbol):
    time = date.today().replace(day=1)
    while time > date(1999, 12, 1):
        month = time.strftime("%Y-%m")
        extract_quotes_by_symbol(symbol, month)
        time = (time - timedelta(days=1)).replace(day=1)
        break


# Подключаюсь к API, получаю данные и записываю их в БД
def extract_quotes_by_symbol(symbol, month):
    quote_list = []
    response = get_quotes(symbol, month)
    data = response.json()
    if f'Time Series ({c.interval})' in data:
        for quote_time in data[f'Time Series ({c.interval})']:
            quote_vl = list(data[f'Time Series ({c.interval})'][quote_time].values())
            quote_open = quote_vl[0]
            quote_close = quote_vl[1]
            quote_high = quote_vl[2]
            quote_low = quote_vl[3]
            volume = quote_vl[4]
            quote_list.append(
                tuple((
                    quote_time,
                    symbol,
                    c.interval,
                    quote_open,
                    quote_close,
                    quote_high,
                    quote_low,
                    volume
                ))
            )
    return quote_list


def raw_load_quotes_test():
    cursor = c.connection.cursor()
    query = '''INSERT INTO raw.stock_quotes 
        (quote_time, symbol, quote_interval, quote_open, quote_close, quote_high, quote_low, volume)
        VALUES ('2023-10-15 22:31:00', 'IBM', '5min', 2200.00, 2300.00, 2350.00, 2150.00, 10000),
        ('2023-10-15 21:31:00', 'IBM', '5min', 2250.00, 2400.00, 2500.00, 2300.00, 9500),
        ('2023-10-15 11:31:00', 'IBM', '5min', 2600.00, 2200.00, 2100.00, 2800.00, 8800),
        ('2023-10-15 08:31:00', 'IBM', '5min', 2300.00, 1900.00, 1500.00, 5000.00, 20000)'''
    cursor.execute(query)
    cursor.close()
    c.connection.commit()
    print("Данные загружены")


def raw_load_quotes(quote_list):
    cursor = c.connection.cursor()
    query = ("INSERT INTO raw.stock_quotes "
             "(quote_time, symbol, quote_interval, quote_open, quote_close, quote_high, quote_low, volume) "
             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    cursor.executemany(query, quote_list)
    cursor.close()
    c.connection.commit()
    print("Данные загружены")