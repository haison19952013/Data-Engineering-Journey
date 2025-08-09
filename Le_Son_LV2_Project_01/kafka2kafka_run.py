from pipeline import Kafka2Kafka, KafkaConnector

if __name__ == "__main__":
    remote_kafka = KafkaConnector("remote_kafka")
    local_kafka = KafkaConnector("local_kafka")
    
    pipeline = Kafka2Kafka(remote_kafka=remote_kafka, local_kafka=local_kafka, batch_size=100)
    consume_from_beginning = False  # Set to True to consume from the beginning of the topic
    pipeline.run(consume_from_beginning=consume_from_beginning)