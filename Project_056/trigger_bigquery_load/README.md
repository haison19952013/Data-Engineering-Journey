# How to deploy the Cloud Function
Run the below: 
```
gcloud functions deploy trigger_bigquery_load \
  --runtime python310 \
  --trigger-resource raw_glamira_dataset \
  --trigger-event google.storage.object.finalize \
  --entry-point trigger_bigquery_load \
  --source . \
  --region us-central1 \
  --no-gen2
```

# Test
Upload any .avro file to one of the folders in the bucket, e.g.:
```bash
gsutil cp ./locations_batch_1.avro gs://raw_glamira_dataset/post_process/ip_location/
```

Then check logs:
```bash
gcloud functions logs read trigger_bigquery_load --region us-central1
```