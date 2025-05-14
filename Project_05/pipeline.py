from pymongo import MongoClient
from config import load_config
from utils import Logger, batch_iterator
from pymongo.errors import ConnectionFailure, BulkWriteError
import IP2Location
import pandas as pd
import os
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/{timestamp}_ETL.log"
logger = Logger(log_filename)
class ETL():
    def __init__(self):
        self.config = load_config(filename='database.ini', section='mongodb')
        self.location_query_db = None
        self.remote_db = None
        self.base_collection = None
        
    def create_collection(self, collection_name="ip_location", index = None):
        if self.remote_db is None:
            self.conn_db()
        if collection_name not in self.remote_db.list_collection_names():
            self.remote_db.create_collection(collection_name)
            if index:
                self.remote_db[collection_name].create_index(index, unique=True)
        return self.remote_db[collection_name]
    
    @logger.log_errors(logger, error_type = ConnectionFailure)
    def conn_db(self):
        client = MongoClient(f"mongodb://{self.config['user']}:{self.config['password']}@{self.config['host']}/", serverSelectionTimeoutMS=2000)  # 5s timeout
        # Trigger a connection attempt
        client.admin.command("ping")
        remote_db = client[self.config['database']]
        base_collection = remote_db[self.config['base_collection']]
        self.remote_db = remote_db
        self.base_collection = base_collection
    
    @logger.log_errors(logger)
    def extract(self, batch_size=500_000, output_prefix="ips_batch"):
        if self.base_collection is None:
            self.conn_db()  # Ensure DB connection

        pipeline = [{"$group": {"_id": "$ip"}}]
        cursor = self.base_collection.aggregate(pipeline, allowDiskUse=True)

        for i, batch in enumerate(batch_iterator(cursor, batch_size)):
            ips = [doc["_id"] for doc in batch]
            df = pd.DataFrame(ips, columns=["ip"])

            filename = f"data/{output_prefix}_{i+1}.csv"
            df.to_csv(filename, index=False)

            logger.info(f"Wrote {len(df)} IPs to {filename}")
         
    @logger.log_errors(logger)
    def transform(self, input_dir="data", input_prefix="ips_batch_", output_prefix="location_batch_", skip_exist = True):
        if self.location_query_db is None:
            self.load_location_query_db()

        for filename in sorted(os.listdir(input_dir)):
            if filename.startswith(input_prefix) and filename.endswith(".csv"):
                input_path = os.path.join(input_dir, filename)
                output_filename = filename.replace(input_prefix, output_prefix)
                output_path = os.path.join(input_dir, output_filename)

                if skip_exist:
                    if os.path.exists(output_path):
                        logger.info(f"Skipping already processed file: {output_filename}")
                        continue

                self.transform_batch(input_path, output_path)

    @logger.log_errors(logger)
    def transform_batch(self, input_path, output_path):
        logger.info(f"Processing: {input_path}")
        df = pd.read_csv(input_path)

        records = []
        for ip in df["ip"]:
            location_obj = self.location_query_db.get_all(ip)
            location_data = {'ip': ip, 'country_short': location_obj.country_short, 'country_long': location_obj.country_long, 'region': location_obj.region, 'city': location_obj.city}
            records.append(location_data)

        output_df = pd.DataFrame(records)
        output_df.to_csv(output_path, index=False)
        logger.info(f"Saved transformed data to {output_path}")
    

    @logger.log_errors(logger)
    def load(self, input_dir="data", input_prefix="location_batch_", load_collection = None):
        load_collection_ = self.create_collection(collection_name=self.config[load_collection], index = 'ip')
        
        for filename in os.listdir(input_dir):
            if filename.startswith(input_prefix) and filename.endswith(".csv"):
                input_path = os.path.join(input_dir, filename)
                self.load_batch(input_path, load_collection_)

    @logger.log_errors(logger)
    def load_batch(self, input_path, load_collection):
        logger.info(f"Loading file into MongoDB: {input_path}")
        df = pd.read_csv(input_path)

        if df.empty:
            logger.warning(f"Empty CSV file: {input_path}")
            return

        # Convert rows to dictionaries
        records = df.to_dict(orient="records")

        # Insert in bulk using insert_many
        # load_collection.insert_many(records, ordered = False)
        batch_size = 10_000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                load_collection.insert_many(batch, ordered=False)
                logger.info(f"Inserted a small batch of {batch_size} records from {input_path}")
            except BulkWriteError as bwe:
                logger.warning(f"Some records failed to insert")
    
    def load_location_query_db(self):
        self.location_query_db = IP2Location.IP2Location(self.config['location_db_path'])
    
    def run(self):
        self.conn_db()
        self.extract(batch_size=500_000, output_prefix="ips_batch")
        self.transform(input_dir="data", input_prefix="ips_batch_", output_prefix="location_batch_", skip_exist = True)
        self.load(input_dir="data", input_prefix="location_batch_", load_collection = 'ip_location_collection')

if __name__ == '__main__':
    etl = ETL()
    # etl.transform(input_dir="data", input_prefix="ips_batch_", output_prefix="location_batch_", skip_exist = False)
    etl.load(input_dir="data", input_prefix="location_batch_", load_collection = 'ip_location_collection')
    # etl.run()
        