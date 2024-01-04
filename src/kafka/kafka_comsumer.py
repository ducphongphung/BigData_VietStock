from confluent_kafka import Consumer, KafkaError
import subprocess
import json

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

    # Example: Save the message to Hadoop HDFS
    hadoop_cmd = [
        'hadoop', 'fs', '-put', '-', '/user/hadoop/input/out.csv'
    ]

    subprocess.run(hadoop_cmd, input=message_value.encode('utf-8'))

def main():
    kafka_config = {
        'bootstrap.servers': 'kafka1:9092,kafka2:9093,kafka3:9094',
        'group.id': 'python-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    kafka_topic = 'vnstock-data'
    consumer = Consumer(kafka_config)

    consume_messages(consumer, kafka_topic)

if __name__ == '__main__':
    main()
