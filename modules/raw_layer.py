import psycopg2 as ps
from modules import repository as r


def init_db():
    print("---> init_db")
    try:
        cursor = r.connection.cursor()

        query = '''SELECT COUNT(*) FROM raw.stock_quotes;'''
        cursor.execute(query)
        result = cursor.fetchall()
        print(result)
        print("raw layer is isset")

    except ps.errors.UndefinedTable:
        print("init raw layer")
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
        r.connection.commit()
        print("success")


def load_symbols(symbol_list):
    print("---> load_symbols")
    cursor = r.connection.cursor()
    query = (
        "INSERT INTO raw.symbols "
        "(symbol_code, symbol_name, symbol_type, region, market_open, market_close, timezone, currency, match_score) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    cursor.executemany(query, symbol_list)
    cursor.close()
    r.connection.commit()
    print("Table symbols success")


def load_quotes(quote_list):
    print("---> load_quotes")
    cursor = r.connection.cursor()

    query = ("INSERT INTO raw.stock_quotes "
             "(quote_time, symbol, quote_interval, quote_open, quote_close, quote_high, quote_low, volume) "
             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    cursor.executemany(query, quote_list)
    cursor.close()
    r.connection.commit()
    print("Table stock_quotes success")

def load_quote(quote):
    cursor = r.connection.cursor()
    query = ("INSERT INTO raw.stock_quotes "
             "(quote_time, symbol, quote_interval, quote_open, quote_close, quote_high, quote_low, volume) "
             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    cursor.execute(query, quote)
    cursor.close()
    r.connection.commit()