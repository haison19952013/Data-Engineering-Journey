from pipeline import Kafka2Mongo, KafkaConnector, MongoDBConnector

if __name__ == "__main__":
    local_kafka = KafkaConnector("local_kafka")
    local_mongo = MongoDBConnector("local_mongodb")

    pipeline = Kafka2Mongo(local_kafka=local_kafka, local_mongo=local_mongo, batch_size=100)
    consume_from_beginning = False  # Set to True to consume from the beginning of the topic
    pipeline.run(consume_from_beginning=consume_from_beginning)