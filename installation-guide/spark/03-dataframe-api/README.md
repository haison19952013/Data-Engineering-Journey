## 1. Create directory and copy files into the Spark Docker container

From the `spark` directory, run the following commands:

**Create directory:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/dataframe-api
```

**Verify:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/
```

**Copy file from host into the container:**

```shell
docker cp 03-dataframe-api/data/survey.csv spark-spark-worker-1:/data/dataframe-api
```

## 2. Run the program

```shell
docker container stop dataframe-api || true &&
docker container rm dataframe-api || true &&
docker run -ti --name dataframe-api \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/03-dataframe-api/dataframe_api.py
```

## 3. Exercises

Redo the exercises from the [spark-sql](../02-spark-sql) section using the `DataFrame API`.
