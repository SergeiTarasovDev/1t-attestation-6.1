CREATE TABLE IF NOT EXISTS stock_quotes
(
	quote_id bigserial   primary key,
	datetime             timestamp,
	symbol               varchar(15),
	quote_open           float,
	quote_close          float,
	quote_high           float,
	quote_low            float,
	volume               float
)