from confluent_kafka import Consumer, KafkaError
import subprocess
import json
from cassandra.cluster import Cluster

# Kết nối đến Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('stock_data')

def consume_messages(consumer, topic):
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(timeout=1000)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            message_value = msg.value().decode('utf-8')
            process_message(message_value)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def process_message(message_value):
    print(f"Received message: {message_value}")
    print(type(message_value))
    data_insert = json.loads(message_value)
    insert_data_to_cassandra(data_insert)

def insert_data_to_cassandra(data):
    query = """INSERT INTO list_companies (ticker, comgroupcode, organname, organshortname, organtypecode, comtypecode, icbname, icbnamepath, sector, industry, group, subgroup, icbcode) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    session.execute(query, (data['ticker'], data['comGroupCode'], data['organName'], data['organShortName'],
                            data['organTypeCode'], data['comTypeCode'], data['icbName'], data['icbNamePath'],
                            data['sector'], data['industry'], data['group'], data['subgroup'],
                            data['icbCode']))
    print("Sent data to Cassandra successfully")

def main():
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'python-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    kafka_topic = 'stock-data'
    consumer = Consumer(kafka_config)

    consume_messages(consumer, kafka_topic)


if __name__ == '__main__':
    main()
