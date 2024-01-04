import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

def process_message(message):
    try:
        data = json.loads(message)
        spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()
        df = spark.createDataFrame([data])

        # Ví dụ: Lưu DataFrame vào Cassandra
        df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="your_cassandra_table", keyspace="bigdata_vnstock") \
          .mode("append") \
          .save()

    except json.JSONDecodeError as e:
        print(f"Lỗi giải mã JSON: {e}")

def main():
    kafka_config = {
        'kafka.bootstrap.servers': 'kafka1:9092,kafka2:9093,kafka3:9094',
        'subscribe': 'vnstock-data',
        'startingOffsets': 'earliest'
    }

    spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config['kafka.bootstrap.servers'])
        .option("subscribe", kafka_config['subscribe'])
        .option("startingOffsets", kafka_config['startingOffsets'])
        .load()
    )

    schema = StructType().add("your_column_name", StringType())  # Điều chỉnh schema dựa trên dữ liệu của bạn

    parsed_df = (
        df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json("value", schema).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_df
        .writeStream
        .outputMode("append")
        .foreach(lambda message: process_message(spark, message))
        .start()
    )

    query.awaitTermination()

if __name__ == '__main__':
    main()
