
# 🔄☁️💾 End-to-End GCP MongoDB Data Pipeline

# 1. GCP CLI Installation and Project Setup

## 1.1 Install GCP CLI

* Follow the official guide: [Install Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)

## 1.2 Authenticate with GCP

```bash
gcloud auth login
```

## 1.3 Create a GCP Project

> 🔸 **Recommended:** Use the [GCP Console](https://console.cloud.google.com/projectcreate) to avoid project ID conflicts.

Or via CLI:

```bash
gcloud projects create your-project-id --name="My Project"
```

## 1.4 List Existing Projects

```bash
gcloud projects list
```

## 1.5 Set Default Project

```bash
gcloud config set project your-project-id
```



# 2. Google Cloud Storage (GCS) Setup

## 2.1 Create a GCS Bucket

```bash
# Variables
PROJECT_ID="your-gcp-project-id"
BUCKET_NAME="your-bucket-name"
REGION="us-central1"

# Create the bucket
gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME/
```

## 2.2 (Optional) Set Bucket Access

```bash
gcloud projects add-iam-policy-binding your-project-id \
  --member="user:someone@gmail.com" \
  --role="roles/storage.objectViewer"
```

## 2.3 Upload Raw Data to GCS

```bash
gsutil cp your-local-file.csv gs://$BUCKET_NAME/
```



# 3. VM Instance Setup

## 3.1 Create a VM Instance

> 🧭 Console is easier, but CLI command is available below.

```bash
# Variables
VM_NAME="mongo-vm"
ZONE="us-central1-a"
MACHINE_TYPE="e2-medium"
IMAGE_NAME="ubuntu-2404-noble-amd64-v20250502a"
IMAGE_PROJECT="ubuntu-os-cloud"

# Create VM
gcloud compute instances create $VM_NAME \
  --zone=$ZONE \
  --machine-type=$MACHINE_TYPE \
  --image=$IMAGE_NAME \
  --image-project=$IMAGE_PROJECT \
  --tags=mongodb-server \
  --boot-disk-size=20GB
```

## 3.2 Connect via SSH

```bash
gcloud compute ssh $VM_NAME --zone=$ZONE
```



# 4. MongoDB Installation and Configuration

## 4.1 Install MongoDB on Ubuntu

Follow the guide: [Install MongoDB on Ubuntu](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/)

## 4.2 Test MongoDB Connection

```bash
mongo --eval 'db.runCommand({ connectionStatus: 1 })'
```

## 4.3 Secure MongoDB

### 4.3.1 Create Admin User

```bash
mongo
```

Then in the shell:

```js
use admin
db.createUser({
  user: "admin",
  pwd: "strongpassword123",  // Use a strong password
  roles: [{ role: "root", db: "admin" }]
})
```

Exit with:

```bash
exit
```

### 4.3.2 Enable Authentication

Edit config:

```bash
sudo nano /etc/mongod.conf
```

Uncomment and update:

```yaml
security:
  authorization: "enabled"
```

### 4.3.3 Restart MongoDB

```bash
sudo systemctl restart mongod
```

### 4.3.4 Test Login

```bash
mongo -u admin -p strongpassword123 --authenticationDatabase admin
```

### 4.3.5 (Optional) Create App-Specific User

```js
use myapp
db.createUser({
  user: "myapp_user",
  pwd: "anothersecurepassword",
  roles: [{ role: "readWrite", db: "myapp" }]
})
```


# 5. External MongoDB Access

## 5.1 Enable External Access in MongoDB

### 5.1.1 Edit `mongod.conf`

Update the binding IP address:

```yaml
# Change:
bindIp: 127.0.0.1

# To:
bindIp: 0.0.0.0
```

### 5.1.2 Restart MongoDB

```bash
sudo systemctl restart mongod
```

## 5.2 Open Firewall Port

```bash
gcloud compute firewall-rules create allow-mongodb \
  --allow=tcp:27017 \
  --source-ranges=0.0.0.0/0 \
  --target-tags=mongodb-server
```

## 5.3 Tag VM with Firewall Rule (if needed)

```bash
gcloud compute instances add-tags $VM_NAME \
  --tags=mongodb-server \
  --zone=$ZONE
```

## 5.4 Connect from Local Machine

```bash
mongo --host <external-ip> --port 27017 \
  -u admin -p strongpassword123 \
  --authenticationDatabase admin
```



# 6. Initial Data Loading

* SSH into VM
* Import raw data into MongoDB:

```bash
mongorestore --db countly --collection summary ~/summary.bson
```



# 7. IP Location Processing

* Install the `ip2location` Python package
* Run the ETL script:

```python
from pipeline import IP_Location_ETL

ip_etl = IP_Location_ETL()
ip_etl.run()
```

This script will:

* Connect to MongoDB
* Read unique IPs from the main collection
* Use `ip2location` to get location data
* Store results in a new collection



# 8. Product Name Collection

* Run the ETL script:

```python
from pipeline import Product_ETL

prod_etl = Product_ETL()
prod_etl.run()
```

This script will:

* Filter data in collections: `view_product_detail`, `select_product_option`, `select_product_option_quality`
* Extract `product_id` and `current_url`
* Crawl product names (get one active `product_name` per `product_id`)
* Store results in `.csv` for later transformation



# 9. 🧾 Data Quality & Profiling Guide for `summary` Collection

## 9.1 ✅ Data Quality Checks

### 9.1.1 🔍 Total Record Count

```js
db.summary.countDocuments()
```

### 9.1.2 🧪 Type Consistency (Boolean Fields)

```js
db.summary.aggregate([
  { $project: { typeOfField: { $type: "$show_recommendation" } } },
  { $group: { _id: "$typeOfField", count: { $sum: 1 } } }
])
```

Repeat for:

* `recommendation`
* `utm_source`
* `utm_medium`

### 9.1.3 🚫 Missing Fields

```js
db.summary.find({ "option": { $exists: false } }).count()
```

### 9.1.4 ❓ Null or Empty Strings

```js
db.summary.find({ "email_address": "" }).count()
db.summary.find({ "product_id": null }).count()
```

### 9.1.5 🔁 Duplicate Records (by device & product)

```js
db.summary.aggregate([
  {
    $group: {
      _id: { device_id: "$device_id", product_id: "$product_id" },
      count: { $sum: 1 }
    }
  },
  { $match: { count: { $gt: 1 } } }
])
```



## 9.2 📊 Data Profiling

### 9.2.1 📦 Count by `collection`

```js
db.summary.aggregate([
  { $group: { _id: "$collection", count: { $sum: 1 } } }
])
```

### 9.2.2 🔧 `option` Field Summary

```js
db.summary.aggregate([
  { $unwind: "$option" },
  {
    $group: {
      _id: "$option.option_label",
      count: { $sum: 1 },
      empty_value_count: {
        $sum: { $cond: [{ $eq: ["$option.value_label", ""] }, 1, 0] }
      }
    }
  }
])
```

### 9.2.3 📈 Numeric Field Stats (e.g. `time_stamp`)

```js
db.summary.aggregate([
  {
    $group: {
      _id: null,
      min_ts: { $min: "$time_stamp" },
      max_ts: { $max: "$time_stamp" },
      avg_ts: { $avg: "$time_stamp" }
    }
  }
])
```

Here’s a clearer, more structured rewrite of your documentation:

---

# 10. Loading Data from GCS to BigQuery

> **Note:** Before running the Python scripts using the Google Cloud SDK, make sure you're authenticated. Use the following command if needed:
>
> ```bash
> gcloud auth application-default login
> ```

---

## 10.1 Exporting Data to GCS

Three Python classes are used to export different data sources to Google Cloud Storage (GCS):

* **`Glamira_All_EL_GCS`**: Exports all records from the MongoDB collection `summary`.
* **`IP_Location_EL_GCS`**: Exports records from the `ip_location` collection in MongoDB.
* **`Product_EL_GCS`**: Uploads local `.csv` product data.

### Usage Example

```python
glamira_el = Glamira_All_EL_GCS(avro_schema_path='data_dict/avro_schema.json')
glamira_el.run()
```

### Key Details

* The data is first converted to `.avro` format, then uploaded to separate GCS folders.
* **Why `.avro`?** Because BigQuery supports it natively and it allows you to define an explicit schema.
* The schema for `Glamira_All_EL_GCS` is complex, so it's provided in `avro_schema.json`.
* For other classes, the schema is defined directly in the Python code.

---

## 10.2 Loading Data from GCS to BigQuery (Manual Step)

The `EL_BigQuery` class is responsible for loading the exported `.avro` files from GCS into BigQuery tables.

### Usage Example

```python
el_bigquery = EL_BigQuery()
el_bigquery.run()
```

This is a manual approach for initial loading. In the next section, we automate this step.

---

## 10.3 Automating BigQuery Load Jobs

The `trigger_bigquery_load` folder contains the setup for an automatic load job triggered by file uploads to GCS.

### Folder Contents

* **`main.py`**: Contains the `trigger_bigquery_load` function, which defines how to load new files into BigQuery.
* **`requirements.txt`**: Lists Python dependencies needed to run `main.py`.

> The `trigger_bigquery_load` function is conceptually similar to the `extract_load` method in the `EL_BigQuery` class.

---

## 10.4 Setting Up Error Alerts for Cloud Function

To monitor errors during BigQuery load jobs, you can set up an alert in Google Cloud Monitoring:

### Steps

1. **Create a log-based metric** using the following query:

   ```sql
   resource.type="cloud_function"
   resource.labels.function_name="trigger_bigquery_load"
   resource.labels.region="us-central1"
   severity>=ERROR
   ```

2. **Create an alerting policy** using this metric. Configure the alert to notify via email, Slack, or another preferred channel.

---

Let me know if you’d like this turned into a Markdown or README format.

