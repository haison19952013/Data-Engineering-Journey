from pymongo import MongoClient
from utils import Logger
from datetime import datetime
from config import load_config
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from pymongo.errors import ConnectionFailure, BulkWriteError
import json

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/{timestamp}_streaming.log"
logger = Logger(logger_level = 'INFO',log_file = log_filename)

class KafkaConnector:
    """
    A connector class for managing Kafka operations including consumer, producer, and admin client connections.
    
    This class provides methods to establish connections to Kafka brokers with SASL authentication,
    create topics, and manage consumer groups with optional reset capabilities.
    
    Attributes:
        kafka_config (dict): Configuration dictionary loaded from database.ini file
        
    Example:
        >>> kafka_conn = KafkaConnector("remote_kafka")
        >>> consumer = kafka_conn.conn_kafka_consumer(topic="my-topic", group_id="my-group")
        >>> producer = kafka_conn.conn_kafka_producer()
    """
    def __init__(self, db_name):
        """
        Initialize KafkaConnector with configuration from specified database section.
        
        Args:
            db_name (str): Section name in database.ini file containing Kafka configuration
        """
        self.kafka_config = load_config(filename="database.ini", section=db_name)
    
    @logger.log_errors(logger)
    def conn_kafka_consumer(self, topic = None, group_id = None, reset_to_beginning=False):
        """
        Create and configure a Kafka consumer with SASL authentication.
        
        Args:
            topic (str, optional): Topic to subscribe to. Uses default from config if None.
            group_id (str, optional): Consumer group ID. Uses default from config if None.
            reset_to_beginning (bool): If True, creates a fresh consumer group to consume from beginning.
                                     Defaults to False.
        
        Returns:
            Consumer: Configured Kafka consumer instance subscribed to the specified topic.
            
        Note:
            When reset_to_beginning=True, a unique suffix is added to group_id to ensure
            consumption starts from the earliest available offset.
        """
        if group_id is None:
            group_id = self.kafka_config['default_consumer_group_id']
        if topic is None:
            topic = self.kafka_config['default_consumer_topic']
            
        # If reset_to_beginning is True, create a unique group ID to start fresh
        if reset_to_beginning:
            import uuid
            unique_suffix = str(uuid.uuid4())[:8]
            group_id = f"{group_id}_fresh_{unique_suffix}"
            logger.info(f"Using fresh consumer group: {group_id} to consume from beginning")
            
        consumer_config = {
            'bootstrap.servers': self.kafka_config['servers'],
            'security.protocol': 'SASL_PLAINTEXT',       # or 'SASL_SSL' if using SSL
            'sasl.mechanism': 'PLAIN',
            'sasl.username': self.kafka_config['username'],
            'sasl.password': self.kafka_config['password'],
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # This will work for new consumer groups
            'enable.auto.commit': False
            }
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        return consumer
    
    @logger.log_errors(logger)
    def conn_kafka_producer(self):
        """
        Create and configure a Kafka producer with SASL authentication.
        
        Returns:
            Producer: Configured Kafka producer instance with 'acks=all' for durability.
            
        Note:
            Producer is configured with 'acks=all' to ensure all replicas acknowledge
            the write before considering it successful.
        """
        producer_config = {
            'bootstrap.servers': self.kafka_config['servers'],
            'security.protocol': 'SASL_PLAINTEXT',       # or 'SASL_SSL' if using SSL
            'sasl.mechanism': 'PLAIN',
            'sasl.username': self.kafka_config['username'],
            'sasl.password': self.kafka_config['password'],
            'acks': 'all'
            }
        producer = Producer(producer_config)
        return producer
    
    @logger.log_errors(logger)
    def conn_kafka_admin(self):
        """
        Create and configure a Kafka admin client for topic management operations.
        
        Returns:
            AdminClient: Configured Kafka admin client for topic creation and management.
        """
        admin_config = {
            'bootstrap.servers': self.kafka_config['servers'],
            'security.protocol': 'SASL_PLAINTEXT',       # or 'SASL_SSL' if using SSL
            'sasl.mechanism': 'PLAIN',
            'sasl.username': self.kafka_config['username'],
            'sasl.password': self.kafka_config['password']
        }
        admin_client = AdminClient(admin_config)
        return admin_client
    
    @staticmethod
    @logger.log_errors(logger)
    def create_topic(admin_client, topic, num_partitions, replication_factor):
        """
        Create a new Kafka topic if it doesn't already exist.
        
        Args:
            admin_client (AdminClient): Kafka admin client instance.
            topic (str): Name of the topic to create.
            num_partitions (int): Number of partitions for the topic.
            replication_factor (int): Replication factor for the topic.
            
        Raises:
            Exception: If topic creation fails for reasons other than topic already existing.
            
        Note:
            This method is idempotent - it will not fail if the topic already exists.
        """
        # Check if topic already exists
        metadata = admin_client.list_topics(timeout=10)
        if topic in metadata.topics:
            logger.info(f"Topic '{topic}' already exists, skipping creation.")
            return
        
        # Create topic if it doesn't exist
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
        try:
            admin_client.create_topics([new_topic])
            logger.info(f"Topic '{topic}' created successfully.")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"Topic '{topic}' already exists.")
            else:
                logger.error(f"Failed to create topic '{topic}': {e}")
                raise

class MongoDBConnector:
    """
    A connector class for managing MongoDB operations including connection, collection creation, and document insertion.
    
    This class provides methods to establish connections to MongoDB, create collections with optional indexing,
    and perform both single and bulk document insertions with proper error handling.
    
    Attributes:
        mongo_config (dict): Configuration dictionary loaded from database.ini file
        client (MongoClient): MongoDB client instance, initialized when connection is established
        
    Example:
        >>> mongo_conn = MongoDBConnector("mongodb")
        >>> collection = mongo_conn.create_collection("mydb", "mycollection")
        >>> result = MongoDBConnector.insert_one(collection, {"key": "value"})
    """
    def __init__(self, db_name=None):
        """
        Initialize MongoDBConnector with configuration from specified database section.
        
        Args:
            db_name (str, optional): Section name in database.ini file containing MongoDB configuration.
                                   If None, uses default section name.
        """
        self.mongo_config = load_config(
            filename="database.ini", section=db_name)
        self.client = None

    @logger.log_errors(logger)
    def create_collection(self, db_name, collection_name, index=None):
        """
        Create a MongoDB collection if it doesn't exist, with optional index creation.
        
        Args:
            db_name (str, optional): Database name. Uses default from config if None.
            collection_name (str, optional): Collection name. Uses default from config if None.
            index (str or list, optional): Index specification to create on the collection.
                                         Can be a field name or index specification.
        
        Returns:
            Collection: MongoDB collection object ready for operations.
            
        Note:
            This method is idempotent - it will not fail if the collection already exists.
            If an index is specified, it will be created as a unique index.
        """
        if self.client is None:
            self.conn_mongo()
        if db_name is None:
            db_name = self.mongo_config['default_db_name']
        if collection_name is None:
            collection_name = self.mongo_config['default_collection_name']
        db = self.client[db_name]
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)
            if index:
                db[collection_name].create_index(
                    index, unique=True)
        return db[collection_name]

    @logger.log_errors(logger, error_type=ConnectionFailure)
    def conn_mongo(self):
        """
        Establish connection to MongoDB server and perform connection health check.
        
        Raises:
            ConnectionFailure: If unable to connect to MongoDB server or ping fails.
            
        Note:
            Uses serverSelectionTimeoutMS=2000 for quick connection timeout.
            Performs a ping command to verify the connection is working.
        """
        client = MongoClient(
            host = self.mongo_config['host'],
            port = int(self.mongo_config['port']),
            serverSelectionTimeoutMS=2000,
        )
        client.admin.command("ping")  # Connection check
        self.client = client
    
    @staticmethod
    @logger.log_errors(logger, error_type=BulkWriteError)
    def insert_one(collection, document):
        """
        Insert a single document into MongoDB collection.
        
        Args:
            collection (Collection): MongoDB collection object.
            document (dict): Document to insert. Must not be empty.
            
        Returns:
            InsertOneResult or None: Result of the insert operation, or None if document is empty.
            
        Raises:
            BulkWriteError: If the insert operation fails.
        """
        """Insert a single document to MongoDB collection"""
        if not document:
            return None
        result = collection.insert_one(document)
        return result
    
    @staticmethod
    @logger.log_errors(logger, error_type=BulkWriteError)
    def insert_many(collection, documents):
        """
        Insert multiple documents into MongoDB collection in a single operation.
        
        Args:
            collection (Collection): MongoDB collection object.
            documents (list): List of documents to insert. Must not be empty.
            
        Returns:
            InsertManyResult or None: Result of the insert operation, or None if documents list is empty.
            
        Raises:
            BulkWriteError: If the bulk insert operation fails.
            
        Note:
            This method performs a bulk insert for better performance when inserting multiple documents.
        """
        """Insert multiple documents to MongoDB collection"""
        if not documents:
            return None
        result = collection.insert_many(documents)
        return result

class StreamingPipeline():
    """
    Base class for streaming data pipelines that process messages between Kafka and storage systems.
    
    This abstract class provides common functionality for extracting messages from Kafka,
    transforming data, and loading to various destinations. It serves as the foundation
    for specific pipeline implementations like Kafka2Kafka and Kafka2Mongo.
    
    Attributes:
        remote_kafka (KafkaConnector): Connector for remote/source Kafka cluster
        local_kafka (KafkaConnector): Connector for local/destination Kafka cluster  
        local_mongo (MongoDBConnector): Connector for local MongoDB instance
        
    Example:
        >>> pipeline = StreamingPipeline(remote_kafka, local_kafka, local_mongo)
        >>> messages = pipeline.extract_from_kafka(consumer, batch_size=100)
    """
    def __init__(self, remote_kafka, local_kafka, local_mongo):
        """
        Initialize StreamingPipeline with connector instances.
        
        Args:
            remote_kafka (KafkaConnector): Connector for remote/source Kafka cluster
            local_kafka (KafkaConnector): Connector for local/destination Kafka cluster
            local_mongo (MongoDBConnector): Connector for local MongoDB instance
        """
        self.remote_kafka = remote_kafka
        self.local_kafka = local_kafka
        self.local_mongo = local_mongo
    
    @logger.log_errors(logger)
    def extract_from_kafka(self, consumer, batch_size=1, timeout=1.0):
        """
        Extract messages from Kafka consumer
        
        Args:
            consumer: Kafka consumer instance
            batch_size: Number of messages to consume (1 for single, >1 for batch)
            timeout: Timeout in seconds for polling
            
        Returns:
            For batch_size=1: (msg, key, value) tuple or (None, None, None)
            For batch_size>1: list of (msg, key, value) tuples
        """
        if batch_size == 1:
            # Single message consumption (backward compatibility)
            msg = consumer.poll(timeout)
            key = None
            value = None
            
            if msg is None:
                logger.info("Waiting...")
                return None, None, None
            elif msg.error():
                logger.error(f"ERROR: {msg.error()}")
                return None, None, None
            else:
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value().decode('utf-8') if msg.value() else None
                logger.info(f"Consumed event from topic {msg.topic()}: key = {key or ''} value = {value or ''}")
                return msg, key, value
        else:
            # Batch message consumption
            message_batch = consumer.consume(num_messages=batch_size, timeout=timeout)
            
            if not message_batch:
                return []
            
            logger.info(f"Consumed batch of {len(message_batch)} messages")
            
            # Process batch and return list of (msg, key, value) tuples
            processed_messages = []
            for msg in message_batch:
                if msg.error():
                    logger.error(f"Kafka message error: {msg.error()}")
                    continue
                    
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value().decode('utf-8') if msg.value() else None
                
                processed_messages.append((msg, key, value))
            
            return processed_messages
    
    @logger.log_errors(logger)
    def load_to_kafka(self, producer, topic, key, value, flush_immediately=False):
        """
        Load a message to Kafka topic using the provided producer.
        
        Args:
            producer (Producer): Kafka producer instance
            topic (str): Target topic name
            key (str): Message key
            value (str): Message value/payload
            flush_immediately (bool): If True, flush the producer immediately after producing.
                                    Defaults to False for better performance.
                                    
        Note:
            When flush_immediately=False, messages are buffered for better throughput.
            Call producer.flush() manually or set flush_immediately=True for immediate delivery.
        """
        producer.produce(topic=topic, key=key, value=value)
        if flush_immediately:
            producer.flush()
    
    @logger.log_errors(logger)
    def load_to_mongo(self, db_name, collection_name, document):
        """
        Load a document to MongoDB collection.
        
        Args:
            db_name (str, optional): Database name. Uses default from config if None.
            collection_name (str, optional): Collection name. Uses default from config if None.
            document (dict): Document to insert into MongoDB.
            
        Note:
            This method uses insert_one for single document insertion.
            Database and collection names default to values from mongo_config if not provided.
        """
        if db_name is None:
            db_name = self.local_mongo.mongo_config['default_db_name']
        if collection_name is None:
            collection_name = self.local_mongo.mongo_config['default_collection_name']
        self.local_mongo[db_name][collection_name].insert_one(document)
    
    @logger.log_errors(logger)
    def transform(self, dict_string):
        """
        Transform a JSON string into a Python dictionary.
        
        Args:
            dict_string (str): JSON string to parse
            
        Returns:
            dict: Parsed JSON as Python dictionary
            
        Raises:
            json.JSONDecodeError: If the string is not valid JSON
        """
        return json.loads(dict_string)
    
    @logger.log_errors(logger)
    def run(self):
        """
        Abstract method to run the pipeline. Must be implemented by subclasses.
        
        Raises:
            NotImplementedError: This is an abstract method that must be overridden.
        """
        pass

class Kafka2Kafka(StreamingPipeline):
    """
    A streaming pipeline that transfers messages from a remote Kafka cluster to a local Kafka cluster.
    
    This pipeline consumes messages from a remote Kafka topic and produces them to a local Kafka topic,
    providing data replication and migration capabilities. It uses batch consumption for efficiency
    while maintaining individual message processing for fault tolerance.
    
    Attributes:
        batch_size (int): Number of messages to consume in each batch
        message_count (int): Total number of messages processed
        log_interval (int): Interval for progress logging (every N messages)
        
    Example:
        >>> pipeline = Kafka2Kafka(remote_kafka, local_kafka, batch_size=100, log_interval=500)
        >>> pipeline.run(topic="source-topic", consume_from_beginning=True)
    """
    def __init__(self, remote_kafka, local_kafka, batch_size=100, log_interval=100):
        """
        Initialize Kafka2Kafka pipeline.
        
        Args:
            remote_kafka (KafkaConnector): Connector for source Kafka cluster
            local_kafka (KafkaConnector): Connector for destination Kafka cluster
            batch_size (int): Number of messages to consume per batch. Defaults to 100.
            log_interval (int): Log progress every N messages. Defaults to 100.
        """
        super().__init__(remote_kafka, local_kafka, None)
        self.batch_size = batch_size
        self.message_count = 0
        self.log_interval = log_interval  # Log progress every N messages
    
    @logger.log_errors(logger)
    def run(self,topic = None, group_id=None, consume_from_beginning=False):
        """
        Run the Kafka-to-Kafka streaming pipeline.
        
        This method starts the pipeline that continuously consumes messages from the remote Kafka
        cluster and produces them to the local Kafka cluster. Each message is processed individually
        to ensure data consistency and fault tolerance.
        
        Args:
            topic (str, optional): Source topic to consume from. Uses default from config if None.
            group_id (str, optional): Consumer group ID. Uses default from config if None.
            consume_from_beginning (bool): If True, creates a fresh consumer group to start from
                                         the beginning of the topic. Defaults to False.
                                         
        Raises:
            KeyboardInterrupt: When user interrupts the pipeline execution
            Exception: For any other pipeline errors
            
        Note:
            - Uses batch consumption for better performance
            - Commits each message individually after successful local Kafka production
            - Flushes producer immediately for each message to ensure delivery
            - Creates local topic automatically if it doesn't exist
        """
        remote_consumer = self.remote_kafka.conn_kafka_consumer(
            topic=topic, 
            group_id=group_id, 
            reset_to_beginning=consume_from_beginning
        )
        local_admin = self.local_kafka.conn_kafka_admin()
        local_producer = self.local_kafka.conn_kafka_producer()
        local_topic = self.local_kafka.kafka_config['default_producer_topic']
        
        self.local_kafka.create_topic(admin_client = local_admin, topic = local_topic, num_partitions = 3, replication_factor = 3)
        
        try:
            # Use consistent batch consumption approach with reusable method
            # while True:
            for i in range(100): # For testing, replace with while True for production
                # Extract messages in batch using the reusable method
                message_batch = self.extract_from_kafka(remote_consumer, batch_size=self.batch_size, timeout=1.0)
                
                if not message_batch:
                    continue
                
                # Process each message individually for better fault tolerance
                for remote_msg, remote_key, remote_value in message_batch:
                    if remote_value is None:
                        continue
                    
                    # Send to local Kafka immediately for simplicity
                    self.load_to_kafka(local_producer, local_topic, remote_key, remote_value, flush_immediately=True)
                    
                    # Commit each message after successful local Kafka insert
                    remote_consumer.commit(remote_msg)
                    self.message_count += 1
                    
                    # Log only periodically for performance (configurable interval)
                    if self.message_count % self.log_interval == 0:
                        logger.info(f"Transferred {self.message_count} messages. Latest offset: {remote_msg.offset()}")
                    
                    # Debug level for individual messages (only when debug logging enabled)
                    logger.debug(f"Message {self.message_count}: offset {remote_msg.offset()}, key: {remote_key}")
                
                # Log batch completion with summary info
                logger.info(f"Completed batch of {len(message_batch)} messages. Total transferred: {self.message_count}")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise
        finally:
            # Final cleanup
            try:
                logger.info(f"Total messages processed: {self.message_count}")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
            finally:
                remote_consumer.close()
                local_producer.flush()  # Flush remaining messages

class Kafka2Mongo(StreamingPipeline):
    """
    A streaming pipeline that consumes messages from Kafka and stores them in MongoDB.
    
    This pipeline consumes messages from a Kafka topic, transforms them from JSON to documents,
    adds metadata (topic, partition, offset, timestamp), and stores them in MongoDB. It uses
    batch consumption for efficiency while maintaining individual document processing for
    fault tolerance and data consistency.
    
    Attributes:
        batch_size (int): Number of messages to consume in each batch
        message_count (int): Total number of messages processed and stored
        log_interval (int): Interval for progress logging (every N messages)
        
    Example:
        >>> pipeline = Kafka2Mongo(local_kafka, local_mongo, batch_size=100, log_interval=500)
        >>> pipeline.run(topic="data-topic", consume_from_beginning=True)
    """
    def __init__(self, local_kafka, local_mongo, batch_size=100, log_interval=100):
        """
        Initialize Kafka2Mongo pipeline.
        
        Args:
            local_kafka (KafkaConnector): Connector for Kafka cluster to consume from
            local_mongo (MongoDBConnector): Connector for MongoDB to store documents
            batch_size (int): Number of messages to consume per batch. Defaults to 100.
            log_interval (int): Log progress every N messages. Defaults to 100.
        """
        # For Kafka2Mongo, we don't need remote_kafka, so pass local_kafka as both remote and local
        super().__init__(None, local_kafka, local_mongo)
        self.batch_size = batch_size  # Keep for logging frequency
        self.message_count = 0
        self.log_interval = log_interval  # Log progress every N messages
    
    @logger.log_errors(logger)
    def run(self, topic=None, group_id=None, consume_from_beginning=False):
        """
        Run the Kafka-to-MongoDB streaming pipeline.
        
        This method starts the pipeline that continuously consumes messages from Kafka,
        transforms them to MongoDB documents with added metadata, and stores them individually
        for maximum data consistency and fault tolerance.
        
        Args:
            topic (str, optional): Kafka topic to consume from. Uses default from config if None.
            group_id (str, optional): Consumer group ID. Uses default from config if None.
            consume_from_beginning (bool): If True, creates a fresh consumer group to start from
                                         the beginning of the topic. Defaults to False.
                                         
        Raises:
            KeyboardInterrupt: When user interrupts the pipeline execution
            Exception: For any other pipeline errors
            
        Note:
            - Uses batch consumption from Kafka for better performance
            - Processes and commits each message individually for fault tolerance
            - Adds metadata to each document: _kafka_key, _kafka_topic, _kafka_partition, 
              _kafka_offset, _processed_at
            - Skips messages that fail JSON parsing but continues processing
            - Only commits Kafka offsets after successful MongoDB insertion
        """
        # Connect to local Kafka as consumer
        local_consumer = self.local_kafka.conn_kafka_consumer(
            topic=topic, 
            group_id=group_id, 
            reset_to_beginning=consume_from_beginning
        )
        
        # Connect to MongoDB and ensure collection exists
        self.local_mongo.conn_mongo()
        db_name = self.local_mongo.mongo_config.get('default_database', 'glamira')
        collection_name = self.local_mongo.mongo_config.get('default_collection', 'raw')
        collection = self.local_mongo.create_collection(db_name, collection_name)
        
        logger.info(f"Starting Kafka2Mongo pipeline - consuming from local Kafka to MongoDB collection: {db_name}.{collection_name}")
        
        try:
            while True:
                # Extract messages in batch using the reusable method
                message_batch = self.extract_from_kafka(local_consumer, batch_size=self.batch_size, timeout=1.0)
                
                if not message_batch:
                    continue
                
                # Process each message individually for better fault tolerance
                for kafka_msg, kafka_key, kafka_value in message_batch:
                    if kafka_value is None:
                        continue
                    
                    # Transform JSON string to document
                    try:
                        document = self.transform(kafka_value)
                        
                        # Add metadata to document
                        document['_kafka_key'] = kafka_key
                        document['_kafka_topic'] = kafka_msg.topic()
                        document['_kafka_partition'] = kafka_msg.partition()
                        document['_kafka_offset'] = kafka_msg.offset()
                        document['_processed_at'] = datetime.now().isoformat()
                        
                        # Insert each document individually for immediate consistency
                        result = self.local_mongo.insert_one(collection, document)
                        
                        if result and result.inserted_id:
                            # Commit this specific message offset after successful MongoDB insert
                            local_consumer.commit(kafka_msg)
                            self.message_count += 1
                            
                            # Log only periodically for performance (configurable interval)
                            if self.message_count % self.log_interval == 0:
                                logger.info(f"Processed {self.message_count} messages. Latest offset: {kafka_msg.offset()}")
                            
                            # Debug level for individual messages (only when debug logging enabled)
                            logger.debug(f"Message {self.message_count}: offset {kafka_msg.offset()}, key: {kafka_key}")
                        else:
                            logger.error(f"Failed to insert document to MongoDB for offset {kafka_msg.offset()}, not committing offset")
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON at offset {kafka_msg.offset()}: {e}, skipping message")
                        # Optionally commit the offset for unparseable messages to avoid reprocessing
                        # local_consumer.commit(kafka_msg)
                        continue
                    except Exception as e:
                        logger.error(f"Error processing message at offset {kafka_msg.offset()}: {e}")
                        continue
                
                # Log batch completion with summary info
                logger.info(f"Completed batch of {len(message_batch)} messages. Total processed: {self.message_count}")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down Kafka2Mongo gracefully...")
        except Exception as e:
            logger.error(f"Kafka2Mongo pipeline error: {e}")
            raise
        finally:
            # Final cleanup
            try:
                logger.info(f"Total messages processed and saved to MongoDB: {self.message_count}")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
            finally:
                local_consumer.close()