from modules import raw_layer as r
from modules import core_layer as c
from modules import mart_layer as m
from modules import constants as const

##### raw слой
print("Create raw layer")

# Инициализация таблиц
r.raw_init_db()

# Извлечение данных по API
#symbol_list = r.raw_extract_symbols()
#quote_list = r.raw_extract_quotes("2023-11")

# # Загрузка данных в БД
#r.raw_load_symbols(symbol_list)
#r.raw_load_quotes(quote_list)
# Тестовые данные, использовал т.к. кончились бесплатные запросы к API
if 'symbol_list' not in globals():
    r.raw_load_symbols_test()
if 'quote_list' not in globals():
    r.raw_load_quotes_test()

##### core слой
print("Create core layer")

# Инициализация таблиц
c.core_init_db()

# Загрузка данных в БД
c.core_extract_load_symbols()
c.core_extract_load_quotes()

##### Mart слой
print("Create Mart layer")

# Инициализация таблиц
m.mart_init_db()

# Загрузка данных в БД
m.mart_extract_load_quotes()

const.connection.close()
