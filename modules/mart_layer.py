from modules import constants as c

def mart_init_db():
    cursor = c.connection.cursor()
    query = '''CREATE SCHEMA IF NOT EXISTS mart;'''
    cursor.execute(query)

    query = ''' CREATE TABLE IF NOT EXISTS mart.dm_quotes_analyze(
                    quotes_analyze_id    bigserial primary key,
                    symbol_name          varchar(15),
                    quote_date           date,
                    volume_sum           float,
                    quote_start          float,
                    quote_end            float,
                    quote_diff           float,
                    period_max_volume    varchar(255),
                    period_max_quote     varchar(255),
                    period_min_quote     varchar(255)
                    );'''
    cursor.execute(query)
    cursor.close()
    c.connection.commit()
    print("Init tables")


def mart_extract_load_quotes():
    cursor = c.connection.cursor()
    query = '''TRUNCATE mart.dm_quotes_analyze'''
    cursor.execute(query)

    query = '''
        WITH
        sort_in_date AS (
            SELECT 
                *,
                DATE_TRUNC('DAY', q.quote_time)::DATE AS quote_date,
                ROW_NUMBER() OVER win AS date_order,
                MAX(volume) OVER win AS max_volume,
                MAX(quote_open) OVER win AS max_quote,
                MIN(quote_open) OVER win AS min_quote
            FROM core.f_quotes AS q 
            LEFT JOIN core.d_symbols AS s 
                ON s.symbol_id = q.symbol_id
            WINDOW win AS (PARTITION BY symbol_code, DATE_TRUNC('DAY', quote_time))
        ),
        start_end_in_date AS (
            SELECT 
                *,
                MAX(date_order) OVER (PARTITION BY symbol_code, DATE_TRUNC('DAY', quote_time)) AS end_time,
                MIN(date_order) OVER (PARTITION BY symbol_code, DATE_TRUNC('DAY', quote_time)) AS start_time
            FROM sort_in_date
        ),
        start_end_quotes_in_date AS (
            SELECT 
                symbol_code,
                quote_date,
                SUM(volume) AS volume_sum,
                MAX(
                    CASE 
                        WHEN end_time = date_order
                        THEN quote_close
                    END) AS quote_end,
                MAX(
                    CASE 
                        WHEN start_time = date_order
                        THEN quote_open
                    END) AS quote_start,
                MAX(
                    CASE
                        WHEN max_volume = volume
                        THEN quote_time
                    END
                ) AS period_max_volume,
                MAX(
                    CASE 
                        WHEN max_quote = quote_open
                        THEN quote_time
                    END) AS preiod_max_quote,
                MAX(
                    CASE 
                        WHEN min_quote = quote_open
                        THEN quote_time
                    END) AS period_min_quote
            FROM start_end_in_date
            GROUP BY symbol_code, quote_date
        )
        INSERT INTO mart.dm_quotes_analyze (symbol_name, quote_date, volume_sum, quote_start, quote_end, quote_diff, period_max_volume, period_max_quote, period_min_quote)
        SELECT 
            symbol_code,
            quote_date,
            volume_sum,
            quote_start,
            quote_end,
            CASE 
                WHEN quote_start != 0
                THEN ROUND(((quote_end - quote_start) / quote_start * 100)::DECIMAL, 2)
                ELSE 0
            END AS quote_diff,
            period_max_volume,
            preiod_max_quote,
            period_min_quote
        FROM start_end_quotes_in_date        
    '''
    cursor.execute(query)
    cursor.close()