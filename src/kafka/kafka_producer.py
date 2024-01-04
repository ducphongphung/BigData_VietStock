from confluent_kafka import Producer
import csv
import json
from kafka import KafkaProducer

from vnstock import *
import warnings

# def crawl_vnstock():
#     df_company = listing_companies()
#     df_company.to_csv(
#         './src/kafka/list_companies.csv', index=False)
#     df_ratio = company_fundamental_ratio(symbol=df_company['ticker'][0], mode='simplify', missing_pct=0.8)
#     n = len(df_company['ticker'])
#     for i in range(1, n):
#         df = company_fundamental_ratio(symbol=df_company['ticker'][i], mode='simplify', missing_pct=0.8)
#         df_ratio = pd.concat([df_ratio, df])
#
#     df_ratio.to_csv(
#         './src/kafka/company_fundamental_ratio.csv', index=False)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def read_csv(file_path):
    data = []
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def produce_messages(producer, topic, data):
    for row in data:
        message_value = json.dumps(row)
        producer.produce(topic, value=message_value, callback=delivery_report)

def main():
    kafka_config = {
        'bootstrap.servers': 'kafka1:9092,kafka2:9093,kafka3:9094',
        'client.id': 'python-producer'
    }

    kafka_topic = 'vnstock-data'
    producer = Producer(kafka_config)

    csv_file_path = 'D:\Java\Bigdata\BigData_VietStock\out.csv'
    data = read_csv(csv_file_path)

    produce_messages(producer, kafka_topic, data)

    producer.flush()

if __name__ == '__main__':
    main()
