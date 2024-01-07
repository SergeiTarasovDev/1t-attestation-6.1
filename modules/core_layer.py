from modules import repository as r
import psycopg2 as ps


def init_db():
    cursor = r.connection.cursor()
    query = '''CREATE SCHEMA IF NOT EXISTS core;'''
    cursor.execute(query)

    query = ''' CREATE TABLE IF NOT EXISTS core.f_quotes(
                    quote_id             bigserial primary key,
                    symbol_id            bigint,
                    quote_time           timestamp,
                    quote_interval       varchar(10),
                    quote_open           float,
                    quote_close          float,
                    quote_high           float,
                    quote_low            float,
                    volume               float
                    );'''
    cursor.execute(query)
    query = ''' CREATE TABLE IF NOT EXISTS core.d_symbols(
                    symbol_id       bigint primary key,
                    symbol_code     varchar(15),
                    symbol_name     varchar(70)
                    );'''
    cursor.execute(query)
    cursor.close()
    r.connection.commit()
    print("Init tables")


def extract_load_symbols():
    try:
        cursor = r.connection.cursor()
        query = ("INSERT INTO core.d_symbols (symbol_id, symbol_code, symbol_name) "
                 "SELECT symbol_id, symbol_code, symbol_name "
                 "FROM raw.symbols")
        cursor.execute(query)
        cursor.close()
        r.connection.commit()
        print("Data loaded")
    except ps.errors.UniqueViolation:
        print("the entry already exists")


def extract_load_quotes():
    try:
        cursor = r.connection.cursor()
        query = ("INSERT INTO core.f_quotes "
                 "(symbol_id, quote_time, quote_interval, quote_open, quote_close, quote_high, quote_low, volume) "
                 "SELECT s.symbol_id, q.quote_time, q.quote_interval, q.quote_open, q.quote_close, q.quote_high, q.quote_low, q.volume "
                 "FROM raw.stock_quotes AS q "
                 "LEFT JOIN raw.symbols AS s ON s.symbol_code = q.symbol")
        cursor.execute(query)
        cursor.close()
        r.connection.commit()
        print("Data loaded")
    except ps.errors.UniqueViolation:
        print("the entry already exists")
