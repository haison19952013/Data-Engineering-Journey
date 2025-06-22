from pymongo import MongoClient
import fastavro
from config import load_config
from utils import Logger, batch_iterator
from pymongo.errors import ConnectionFailure, BulkWriteError
from google.cloud.exceptions import GoogleCloudError
import IP2Location
import pandas as pd
import os
from datetime import datetime
from crawl_tool import crawl_prod_name
from google.cloud import storage, bigquery
import math
import json

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/{timestamp}_ETL.log"
logger = Logger(log_filename)


class MongoDB_Connector:
    def __init__(self):
        self.mongo_config = load_config(
            filename="database.ini", section="mongodb")
        self.remote_mongo = None
        self.mongo_base_collection = None

    def create_collection(self, collection_name="default_collection", index=None):
        if self.remote_mongo is None:
            self.conn_mongo()
        if collection_name not in self.remote_mongo.list_collection_names():
            self.remote_mongo.create_collection(collection_name)
            if index:
                self.remote_mongo[collection_name].create_index(
                    index, unique=True)
        return self.remote_mongo[collection_name]

    @logger.log_errors(logger, error_type=ConnectionFailure)
    def conn_mongo(self):
        client = MongoClient(
            f"mongodb://{self.mongo_config['user']}:{self.mongo_config['password']}@{self.mongo_config['host']}/",
            serverSelectionTimeoutMS=2000,
        )
        client.admin.command("ping")  # Connection check
        self.remote_mongo = client[self.mongo_config["database"]]
        self.mongo_base_collection = self.remote_mongo[self.mongo_config["base_collection"]]


class IP_Location_ETL_Mongo(MongoDB_Connector):
    def __init__(self):
        super().__init__()
        self.location_query_db = None

    @logger.log_errors(logger)
    def extract(self, batch_size=500_000, output_prefix="ips_batch"):
        if self.mongo_base_collection is None:
            self.conn_mongo()  # Ensure DB connection

        pipeline = [{"$group": {"_id": "$ip"}}]
        cursor = self.mongo_base_collection.aggregate(
            pipeline, allowDiskUse=True)

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
            collection_name=self.mongo_config[load_collection], index="ip"
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
            batch = records[i: i + batch_size]
            try:
                load_collection.insert_many(batch, ordered=False)
                logger.info(
                    f"Inserted a small batch of {batch_size} records from {input_path}"
                )
            except BulkWriteError as bwe:
                logger.warning(f"Some records failed to insert")

    def load_location_query_db(self):
        self.location_query_db = IP2Location.IP2Location(
            self.mongo_config["location_db_path"]
        )

    def run(self):
        self.conn_mongo()
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


class Product_ETL_Local(MongoDB_Connector):
    def __init__(self):
        super().__init__()

    @logger.log_errors(logger)
    def extract(self, batch_size=500_000, input_dir="data", output_prefix="prod_batch"):
        if self.mongo_base_collection is None:
            self.conn_mongo()  # Ensure DB connection

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
        cursor = self.mongo_base_collection.aggregate(
            pipeline, allowDiskUse=True)

        for i, batch in enumerate(batch_iterator(cursor, batch_size)):
            prod_data = [doc["_id"] for doc in batch]
            df = pd.DataFrame(prod_data)

            filename = f"{input_dir}/{output_prefix}_{i+1}.csv"
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

        def wrangle(df):
            df = df.dropna()
            fmt_condition = df['current_url'].str.startswith(
                r'https://www.glamira.')
            product_info_condition = df['current_url'].str.contains(r'.html')
            df = df[fmt_condition & product_info_condition]
            df['current_url'] = df['current_url'].apply(
                lambda x: x.split('?')[0])
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
        self.conn_mongo()
        self.extract(batch_size=500_000, output_prefix="prod_batch")
        self.transform(input_dir="data",
                       input_prefix="prod_batch_",
                       output_prefix="prod_url_batch_",
                       skip_exist=True,)
        self.load()


class Glamira_All_EL_GCS(MongoDB_Connector):
    def __init__(self, avro_schema_path=None):
        super().__init__()
        self.gcs_config = load_config(filename="database.ini", section="gcs")
        self.avro_schema_path = avro_schema_path

    @logger.log_errors(logger)
    def extract_load(self, batch_size=500_000, input_dir='data', output_prefix="glamira_batch"):
        if self.mongo_base_collection is None:
            self.conn_mongo()  # Ensure DB connection

        avro_schema = json.load(open(self.avro_schema_path))
        parsed_schema = fastavro.parse_schema(avro_schema)

        cursor = self.mongo_base_collection.find()
        for i, batch in enumerate(batch_iterator(cursor, batch_size)):
            batch_data = [self.normalize_record(doc, avro_schema) for doc in batch]

            filename = f"{output_prefix}_{i+1}.avro"
            local_filepath = os.path.join(input_dir, filename)
            bucket_name = self.gcs_config["bucket"]
            remote_filepath = os.path.join(
                self.gcs_config["post_process_all_path"], filename)

            with open(local_filepath, "wb") as f:
                fastavro.writer(f, parsed_schema, batch_data)
            self.load(local_filepath, bucket_name, remote_filepath)

    @logger.log_errors(logger)
    def load(self, local_filepath, bucket_name, remote_filepath, delete_local=True):
        """Upload local file to Google Cloud Storage."""
        storage_client = storage.Client(project=self.gcs_config["project_id"])
        bucket = storage_client.bucket(
            bucket_name, user_project=self.gcs_config["project_id"])
        blob = bucket.blob(remote_filepath)
        blob.upload_from_filename(local_filepath)
        logger.info(f"Uploaded {local_filepath} to {remote_filepath}")
        if delete_local:
            os.remove(local_filepath)
            logger.info(f"Deleted {local_filepath}")

    @logger.log_errors(logger)
    def transform(self):
        pass

    def run(self):
        self.conn_mongo()
        self.extract_load(batch_size=500_000, output_prefix="glamira_batch")
        # self.transform()

    @staticmethod
    def normalize_record(record, schema):
        """Convert MongoDB record to Avro-ready format, dropping unwanted fields."""
        record = dict(record)
        record["_id"] = str(record.get("_id", ""))
        # record.pop("option", None)  # Drop 'option' field

        # Ensure all schema fields exist
        schema_fields = {f["name"] for f in schema["fields"]}
        for field in schema_fields:
            if field not in record:
                record[field] = None
        return record


class IP_Location_EL_GCS(MongoDB_Connector):
    def __init__(self):
        super().__init__()
        self.gcs_config = load_config(filename="database.ini", section="gcs")

    @logger.log_errors(logger)
    def extract_load(self, batch_size=500_000, input_dir='data', output_prefix="locations_batch"):
        if self.mongo_base_collection is None:
            self.conn_mongo()  # Ensure DB connection

        schema = {
            "doc": "IP geolocation data from MongoDB",
            "name": "UserGeoLocation",
            "namespace": "com.example",
            "type": "record",
            "fields": [
                {"name": "_id", "type": "string"},
                {"name": "ip", "type": ["null", "string"], "default": None},
                {"name": "country_short", "type": [
                    "null", "string"], "default": None},
                {"name": "country_long", "type": [
                    "null", "string"], "default": None},
                {"name": "region", "type": [
                    "null", "string"], "default": None},
                {"name": "city", "type": ["null", "string"], "default": None}
            ]
        }
        parsed_schema = fastavro.parse_schema(schema)

        cursor = self.remote_mongo[self.mongo_config["ip_location_collection"]].find(
        )
        for i, batch in enumerate(batch_iterator(cursor, batch_size)):
            batch_data = [self.normalize_record(doc, schema) for doc in batch]

            filename = f"{output_prefix}_{i+1}.avro"
            local_filepath = os.path.join(input_dir, filename)
            bucket_name = self.gcs_config["bucket"]
            remote_filepath = os.path.join(
                self.gcs_config["post_process_ip_location_path"], filename)

            with open(local_filepath, "wb") as f:
                fastavro.writer(f, parsed_schema, batch_data)
            self.load(local_filepath, bucket_name, remote_filepath)

    @logger.log_errors(logger)
    def load(self, local_filepath, bucket_name, remote_filepath, delete_local=True):
        """Upload local file to Google Cloud Storage."""
        storage_client = storage.Client(project=self.gcs_config["project_id"])
        bucket = storage_client.bucket(
            bucket_name, user_project=self.gcs_config["project_id"])
        blob = bucket.blob(remote_filepath)
        blob.upload_from_filename(local_filepath)
        logger.info(f"Uploaded {local_filepath} to {remote_filepath}")
        if delete_local:
            os.remove(local_filepath)
            logger.info(f"Deleted {local_filepath}")

    def run(self):
        self.conn_mongo()
        self.extract_load(batch_size=500_000, output_prefix="locations_batch")

    @staticmethod
    def normalize_record(record, schema):
        """Convert MongoDB record to Avro-ready format, dropping unwanted fields."""
        record = dict(record)
        record["_id"] = str(record.get("_id", ""))

        # Ensure all schema fields exist
        schema_fields = {f["name"] for f in schema["fields"]}
        for field in schema_fields:
            value = record.get(field, None)
            if isinstance(value, float) and math.isnan(value):
                record[field] = None
            elif field not in record:
                record[field] = None
        return record


class Product_EL_GCS():
    def __init__(self):
        self.gcs_config = load_config(filename="database.ini", section="gcs")

    @logger.log_errors(logger)
    def extract_load(self, input_dir='data', input_prefix="prod_url_batch"):
        schema = {
            "doc": "Product URL metadata",
            "name": "ProductURLInfo",
            "namespace": "com.example",
            "type": "record",
            "fields": [
                {"name": "name", "type": ["null", "string"], "default": None},
                {"name": "language_code", "type": [
                    "null", "string"], "default": None},
                {"name": "status_code", "type": [
                    "null", "string"], "default": None},
                {"name": "method", "type": [
                    "null", "string"], "default": None},
                {"name": "product_id", "type": [
                    "null", "string"], "default": None},
                {"name": "url", "type": ["null", "string"], "default": None}
            ]
        }
        parsed_schema = fastavro.parse_schema(schema)

        for inp_filename in sorted(os.listdir(input_dir)):
            if inp_filename.startswith(input_prefix) and inp_filename.endswith(".csv"):
                local_csv_filepath = os.path.join(input_dir, inp_filename)
                df = pd.read_csv(local_csv_filepath, encoding="utf-8")
                batch_data = [self.normalize_record(
                    doc, schema) for doc in df.to_dict(orient='records')]
                bucket_name = self.gcs_config["bucket"]
                oup_filename = inp_filename.replace('.csv', '.avro')
                local_avro_filepath = os.path.join(input_dir, oup_filename)
                remote_filepath = os.path.join(
                    self.gcs_config["post_process_product_path"], oup_filename)

                with open(local_avro_filepath, "wb") as f:
                    fastavro.writer(f, parsed_schema, batch_data)
                self.load(local_avro_filepath, bucket_name, remote_filepath)

    @logger.log_errors(logger)
    def load(self, local_filepath, bucket_name, remote_filepath, delete_local=True):
        """Upload local file to Google Cloud Storage."""
        storage_client = storage.Client(project=self.gcs_config["project_id"])
        bucket = storage_client.bucket(
            bucket_name, user_project=self.gcs_config["project_id"])
        blob = bucket.blob(remote_filepath)
        blob.upload_from_filename(local_filepath)
        logger.info(f"Uploaded {local_filepath} to {remote_filepath}")
        if delete_local:
            os.remove(local_filepath)
            logger.info(f"Deleted {local_filepath}")

    @logger.log_errors(logger)
    def transform(self):
        pass

    def run(self):
        self.extract_load(input_prefix="prod_url_batch")
        # self.transform()

    @staticmethod
    def normalize_record(record, schema):
        """Convert MongoDB record to Avro-ready format, dropping unwanted fields."""
        record = dict(record)
        record["_id"] = str(record.get("_id", ""))
        record["product_id"] = str(record.get("product_id", ""))

        # Ensure all schema fields exist
        schema_fields = {f["name"] for f in schema["fields"]}
        for field in schema_fields:
            value = record.get(field, None)
            if isinstance(value, float) and math.isnan(value):
                record[field] = None
            elif field not in record:
                record[field] = None
        return record


class EL_BigQuery():
    def __init__(self):
        super().__init__()
        self.gcs_config = load_config(filename="database.ini", section="gcs")
        self.bigquery_config = load_config(
            filename="database.ini", section="bigquery")

    @logger.log_errors(logger, error_type=GoogleCloudError)
    def extract_load(self, data):
        """Extract data from GCS and load into Google BigQuery."""
        if data == 'all':
            table_id = self.bigquery_config['table_id_all']
            gcs_uri = f"gs://{self.gcs_config['bucket']}/{self.gcs_config['post_process_all_path']}/{self.gcs_config['all_file']}"
        elif data == 'ip_location':
            table_id = self.bigquery_config['table_id_ip_location']
            gcs_uri = f"gs://{self.gcs_config['bucket']}/{self.gcs_config['post_process_ip_location_path']}/{self.gcs_config['ip_location_file']}"
        elif data == 'product':
            table_id = self.bigquery_config['table_id_product']
            gcs_uri = f"gs://{self.gcs_config['bucket']}/{self.gcs_config['post_process_product_path']}/{self.gcs_config['product_file']}"
        else:
            logger.error("Error: Invalid table_id option")
        bq_client = bigquery.Client(project=self.bigquery_config["project_id"])
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.AVRO,
            autodetect=True,  # Automatically detect schema
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite the table
        )
        table_ref = f"{self.bigquery_config['project_id']}.{self.bigquery_config['dataset_id']}.{table_id}"
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        load_job.result()
        logger.info(f"Loaded {gcs_uri} into {table_ref}")

    def run(self):
        self.extract_load(data="all")
        self.extract_load(data="ip_location")
        self.extract_load(data="product")


if __name__ == "__main__":
    pass
    # ## Load all records to BigQuery
    # el_biquery = EL_BigQuery()
    # el_biquery.run()

    # ## Load all Glamira records to GCS
    #glamira_el = Glamira_All_EL_GCS(avro_schema_path = 'data_dict/avro_schema.json')
    # glamira_el.run()

    # ## Load IP locaton recors to GCS
    # ip_location_el = IP_Location_EL_GCS()
    # ip_location_el.run()

    # ## Load Product records to GCS
    # product_el = Product_EL_GCS()
    # product_el.run()

    # ## Extract IP, get location, and load to MongoDB
    # ip_etl = IP_Location_ETL_Mongo()
    # ip_etl.run()

    # ## Extract Product id, product url, get product name and save to local
    # prod_etl = Product_ETL_Local()
    # prod_etl.transform(skip_exist = False, input_dir="data")
    # prod_etl.run()

# Code to temporarily solve the incorrect format csv
# import pandas as pd
# import ast
# df_parsed = df['_id'].apply(ast.literal_eval)
# df_cleaned = pd.DataFrame(df_parsed.tolist())
