import time
import csv
import psycopg2 as ps

while True:
    print("try connection")
    try:
        connection = ps.connect(
            host='postgres',
            port='5432',
            user='postgres',
            password='postgres',
            database='quotes',
        )
        print("connection success")
        break
    except ps.OperationalError:
        print("connection error")
        time.sleep(5)

connection.autocommit = True


def save_raw_to_csv(table_name):
    print("---> save_raw_to_csv")
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM raw." + table_name)
    with open("/app/backup/" + table_name + ".csv", "w", newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()


def extract_raw_from_csv(table_name, fields, values):
    print("---> extract_raw_from_csv")
    cursor = connection.cursor()
    with open("/app/backup/" + table_name + ".csv", 'r') as file:
        csv_data = csv.reader(file)
        next(csv_data)
        for row in csv_data:
            del row[0]
            cursor.execute("INSERT INTO raw." + table_name + " (" + fields + ") VALUES (" + values + ")", tuple(row))
        connection.commit()
    cursor.close()


def get_last_values():
    cursor = connection.cursor()
    query = '''
        SELECT
            symbol, 
            MAX(quote_time) as last_time
        FROM raw.stock_quotes
        GROUP BY symbol;
    '''
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result
