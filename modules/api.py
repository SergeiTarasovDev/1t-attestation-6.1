import time
import json
import requests
from datetime import date, timedelta
from modules import constants as c


def get_symbols(keywords):
    return requests.get(c.API_URL, params={'function': 'SYMBOL_SEARCH',
                                           'apikey': c.API_KEY,
                                           'keywords': keywords})


def extract_symbols():
    symbol_list = []
    for symbol in c.symbols:
        response = get_symbols(symbol)
        data = response.json()
        print(data)
        if 'bestMatches' in data:
            for entry in data['bestMatches']:
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


def get_quotes(symbol, month):
    print("---> get_quotes_history")
    res = requests.get(c.API_URL, params={'function': 'TIME_SERIES_INTRADAY',
                                           'symbol': symbol,
                                           'interval': c.interval,
                                           'outputsize': 'full',
                                           'month': month,
                                           'apikey': c.API_KEY})
    print(res.url)
    save_json_to_file(res.json())
    return res.json()


# Перебираю все акции и получаю по ним котировки
def extract_quotes(month, source):
    print("---> extract_quotes")
    result = []
    for symbol in c.symbols:
        print(symbol)
        result += extract_quotes_by_symbol(symbol, month, source)
    return result


# Подключаюсь к API, получаю данные и записываю их в БД
def extract_quotes_by_symbol(symbol, month, source='file'):
    print("---> extract_quotes_by_symbol")
    quote_list = []
    if source == 'api':
        data = get_quotes(symbol, month)
        save_json_to_file(data)
    elif source == 'file':
        data = extract_json_from_file()
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

def extract_json_from_file():
    print("---> extract_json_from_file")
    with open('/app/backup/data.json') as f:
        data = json.load(f)
    return data

def save_json_to_file(data):
    print("---> save_json_to_file")
    with open('/app/backup/data.json', 'w') as f:
        json.dump(data, f)