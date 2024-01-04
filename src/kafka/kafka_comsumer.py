from confluent_kafka import Consumer, KafkaError


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
