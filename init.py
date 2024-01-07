from modules import raw_layer as raw
from modules import core_layer as core
from modules import mart_layer as mart
from modules import repository as r
from modules import api

try:
    ##### raw слой

    # Инициализация таблиц
    raw.init_db()

    # Загрузка данных из CSV
    r.extract_raw_from_csv("stock_quotes",
                           "quote_time, symbol, quote_interval, quote_open, quote_close, quote_high, quote_low, volume",
                           "%s,%s,%s,%s,%s,%s,%s,%s")
    r.extract_raw_from_csv("symbols",
                           "symbol_code, symbol_name, symbol_type, region, market_open, market_close, timezone, currency, match_score",
                           "%s,%s,%s,%s,%s,%s,%s,%s,%s")

    # Извлечение данных по API
    #quote_list = api.extract_quotes("2023-11", 'api')
    #symbol_list = api.extract_symbols()

    # Загрузка данных в БД
    #raw.load_quotes(quote_list)
    #raw.load_symbols(symbol_list)

    # Запись полученных данных в CSV
    #r.save_raw_to_csv("stock_quotes")
    #r.save_raw_to_csv("symbols")

except Exception as e:
    print('ERROR: Ошибка в raw слое: '
          + str(e.__class__) + ': ' + str(e))

try:
    ##### core слой
    print("Create core layer")

    # Инициализация таблиц
    core.init_db()

    # Загрузка данных в БД
    core.extract_load_symbols()
    core.extract_load_quotes()
except Exception as e:
    print('ERROR: Ошибка в core слое: '
          + str(e.__class__) + ': ' + str(e))

try:
    ##### Mart слой
    print("Create Mart layer")

    # Инициализация таблиц
    mart.init_db()

    # Загрузка данных в БД
    mart.extract_load_quotes()
except Exception as e:
    print('ERROR: Ошибка в mart слое: '
          + str(e.__class__) + ': ' + str(e))

r.connection.close()
