import psycopg2 as ps

connection = ps.connect(
    host='postgres',
    port='5432',
    user='postgres',
    password='postgres',
    database='quotes',
)
connection.autocommit = True

API_URL = "https://www.alphavantage.co/query"
API_KEY = "XFXOO5DWCE1OF11E"
symbols = ('IBM', 'INTC', 'TSLA', 'NFC.DEX')
interval = '5min'  # 1min, 5min, 15min, 30min, 60min