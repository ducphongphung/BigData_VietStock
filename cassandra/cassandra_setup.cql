CREATE KEYSPACE stock_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

USE stock_data;

CREATE TABLE intraday (
    ticker text,
    time timestamp,
    order_type text,
    volume bigint,
    price decimal,
    prev_price_change decimal,
    real_time_price_change decimal,
    PRIMARY KEY (ticker, time)
) WITH CLUSTERING ORDER BY (time DESC);