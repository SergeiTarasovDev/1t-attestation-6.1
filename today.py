#!/usr/local/bin python3
from datetime import datetime, timedelta

from modules import raw_layer as raw
from modules import core_layer as core
from modules import mart_layer as mart
from modules import repository as r
from modules import api

today = datetime.now()
yesterday = today - timedelta(days=1)
current_month = yesterday.strftime("%Y-%m-%d %H:%M:%S") + "\r\n"
print('')
print('==================================================================================')
print('today.py ' + current_month)
with open("/app/file.txt", "a") as file:
    file.write(current_month)
current_month = yesterday.strftime("%Y-%m")
print(current_month)


# Извлечение новых данных по API
try:
    ##### raw слой
    quote_list = api.extract_quotes(current_month, 'api')
    reversed_list = quote_list[::-1]
    last_values = r.get_last_values()[0]

    for quote in reversed_list:
        date_format = '%Y-%m-%d %H:%M:%S'
        quote_date = datetime.strptime(quote[0], date_format)
        if (quote[1] == last_values[0]) and (quote_date > last_values[1]):
            # Загрузка данных в БД
            raw.load_quote(quote)

    # Запись полученных данных в CSV
    r.save_raw_to_csv("stock_quotes")
except TypeError as e:
    print('ERROR: Пустой список: '
          + str(e.__class__) + ': ' + str(e))
except Exception as e:
    print('ERROR: Ошибка в raw слое: '
          + str(e.__class__) + ': ' + str(e))

try:
    ##### core слой
    print("Create core layer")

    # Инициализация таблиц
    core.init_db()

    # Загрузка данных в БД
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
