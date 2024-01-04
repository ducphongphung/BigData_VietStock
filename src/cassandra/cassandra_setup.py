from cassandra.cluster import Cluster


cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("CREATE KEYSPACE bigdata_vnstock\
    WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1}")

session.execute("USE stock_data")

session.execute("CREATE TABLE stock_data (\
    ticker text,\
    time timestamp,\
    order_type text,\
    volume bigint,\
    price decimal,\
    prev_price_change decimal,\
    real_time_price_change decimal,\
    PRIMARY KEY (ticker, time)\
) WITH CLUSTERING ORDER BY (time DESC)")


session.shutdown()