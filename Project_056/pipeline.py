from pymongo import MongoClient
from config import load_config
from utils import Logger, batch_iterator
from pymongo.errors import ConnectionFailure, BulkWriteError
import IP2Location
import pandas as pd
import os
from datetime import datetime
from crawl_tool import crawl_prod_name

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/{timestamp}_ETL.log"
logger = Logger(log_filename)

class MongoDB_ETL_Base:
    def __init__(self):
        self.config = load_config(filename="database.ini", section="mongodb")
        self.remote_db = None
        self.base_collection = None

    def create_collection(self, collection_name="default_collection", index=None):
        if self.remote_db is None:
            self.conn_db()
        if collection_name not in self.remote_db.list_collection_names():
            self.remote_db.create_collection(collection_name)
            if index:
                self.remote_db[collection_name].create_index(index, unique=True)
        return self.remote_db[collection_name]

    @logger.log_errors(logger, error_type=ConnectionFailure)
    def conn_db(self):
        client = MongoClient(
            f"mongodb://{self.config['user']}:{self.config['password']}@{self.config['host']}/",
            serverSelectionTimeoutMS=2000,
        )
        client.admin.command("ping")  # Connection check
        self.remote_db = client[self.config["database"]]
        self.base_collection = self.remote_db[self.config["base_collection"]]


class IP_Location_ETL(MongoDB_ETL_Base):
    def __init__(self):
        super().__init__()
        self.location_query_db = None

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
    def transform(
        self,
        input_dir="data",
        input_prefix="ips_batch_",
        output_prefix="location_batch_",
        skip_exist=True,
    ):
        if self.location_query_db is None:
            self.load_location_query_db()

        for filename in sorted(os.listdir(input_dir)):
            if filename.startswith(input_prefix) and filename.endswith(".csv"):
                input_path = os.path.join(input_dir, filename)
                output_filename = filename.replace(input_prefix, output_prefix)
                output_path = os.path.join(input_dir, output_filename)

                if skip_exist:
                    if os.path.exists(output_path):
                        logger.info(
                            f"Skipping already processed file: {output_filename}"
                        )
                        continue

                self.transform_batch(input_path, output_path)

    @logger.log_errors(logger)
    def transform_batch(self, input_path, output_path):
        logger.info(f"Processing: {input_path}")
        df = pd.read_csv(input_path)

        records = []
        for ip in df["ip"]:
            location_obj = self.location_query_db.get_all(ip)
            location_data = {
                "ip": ip,
                "country_short": location_obj.country_short,
                "country_long": location_obj.country_long,
                "region": location_obj.region,
                "city": location_obj.city,
            }
            records.append(location_data)

        output_df = pd.DataFrame(records)
        output_df.to_csv(output_path, index=False)
        logger.info(f"Saved transformed data to {output_path}")

    @logger.log_errors(logger)
    def load(
        self, input_dir="data", input_prefix="location_batch_", load_collection=None
    ):
        load_collection_ = self.create_collection(
            collection_name=self.config[load_collection], index="ip"
        )

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
            batch = records[i : i + batch_size]
            try:
                load_collection.insert_many(batch, ordered=False)
                logger.info(
                    f"Inserted a small batch of {batch_size} records from {input_path}"
                )
            except BulkWriteError as bwe:
                logger.warning(f"Some records failed to insert")

    def load_location_query_db(self):
        self.location_query_db = IP2Location.IP2Location(
            self.config["location_db_path"]
        )

    def run(self):
        self.conn_db()
        self.extract(batch_size=500_000, output_prefix="ips_batch")
        self.transform(
            input_dir="data",
            input_prefix="ips_batch_",
            output_prefix="location_batch_",
            skip_exist=True,
        )
        self.load(
            input_dir="data",
            input_prefix="location_batch_",
            load_collection="ip_location_collection",
        )


class Product_ETL(MongoDB_ETL_Base):
    def __init__(self):
        super().__init__()

    @logger.log_errors(logger)
    def extract(self, batch_size=500_000, output_prefix="prod_batch"):
        if self.base_collection is None:
            self.conn_db()  # Ensure DB connection

        pipeline = [
            {
                "$match": {
                    "collection": {
                        "$in": [
                            "view_product_detail",
                            "select_product_option",
                            "select_product_option_quality",
                        ]
                    }
                }
            },
            {"$project": {"product_id": 1, "current_url": 1}},
            {
                "$group": {
                    "_id": {"product_id": "$product_id", "current_url": "$current_url"}
                }
            },
        ]
        cursor = self.base_collection.aggregate(pipeline, allowDiskUse=True)

        for i, batch in enumerate(batch_iterator(cursor, batch_size)):
            prod_data = [doc["_id"] for doc in batch]
            df = pd.DataFrame(prod_data)

            filename = f"data/{output_prefix}_{i+1}.csv"
            df.to_csv(filename, index=False)

            logger.info(f"Wrote {len(df)} Products to {filename}")

    def load(self):
        pass

    @logger.log_errors(logger)
    def transform(
        self,
        input_dir="data",
        input_prefix="prod_batch_",
        output_prefix="prod_url_batch_",
        skip_exist=True,
    ):

        for filename in sorted(os.listdir(input_dir)):
            if filename.startswith(input_prefix) and filename.endswith(".csv"):
                input_path = os.path.join(input_dir, filename)
                output_filename = filename.replace(input_prefix, output_prefix)
                output_path = os.path.join(input_dir, output_filename)

                if skip_exist:
                    if os.path.exists(output_path):
                        logger.info(
                            f"Skipping already processed file: {output_filename}"
                        )
                        continue

                self.transform_batch(input_path, output_path)

    @logger.log_errors(logger)
    def transform_batch(self, input_path, output_path):
        def update_url(text):
            text_split = text.split('.html')
            return text_split[0] + '.html'
            # for i in text_split:
            #     if '.html' in i:
            #         return 'https://www.glamira.com/' + i.split('.html')[0] + '.html'

        def wrangle(df):
            df = df.dropna()
            fmt_condition = df['current_url'].str.startswith(r'https://www.glamira.')
            product_info_condition = df['current_url'].str.contains(r'.html')
            df = df[fmt_condition & product_info_condition]
            df['current_url'] = df['current_url'].apply(lambda x: x.split('?')[0])
            df['current_url'] = df['current_url'].apply(update_url)
            df = df.drop_duplicates()
            return df
        logger.info(f"Processing: {input_path}")
        df = pd.read_csv(input_path)
        output_df = wrangle(df)
        # Crawl product name
        output_df = crawl_prod_name(output_df)
        output_df.to_csv(output_path, index=False)
        logger.info(f"Saved transformed data to {output_path}")

    def run(self):
        self.conn_db()
        self.extract(batch_size=500_000, output_prefix="prod_batch")
        self.transform(input_dir="data",
            input_prefix="prod_batch_",
            output_prefix="prod_url_batch_",
            skip_exist=True,)
        self.load()

if __name__ == "__main__":
    # ip_etl = IP_Location_ETL()
    # ip_etl.run()
    prod_etl = Product_ETL()
    prod_etl.transform(skip_exist = False, input_dir="data")
    # prod_etl.run()

# Code to temporarily solve the incorrect format csv
# import pandas as pd
# import ast
# df_parsed = df['_id'].apply(ast.literal_eval)
# df_cleaned = pd.DataFrame(df_parsed.tolist())