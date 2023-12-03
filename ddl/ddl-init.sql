CREATE DATABASE IF NOT EXISTS course_1t;
CREATE TABLE IF NOT EXISTS course_1t.stock_quotes
(
	quote_id 			 Int32,
	datetime             Date,
	symbol               String,
	quote_open           Float32,
	quote_close          Float32,
	quote_high           Float32,
	quote_low            Float32,
	volume               Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (symbol, datetime)
PRIMARY KEY (quote_id)