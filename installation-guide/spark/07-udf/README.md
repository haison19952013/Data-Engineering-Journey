## 1. Create directory and copy files into the Spark Docker container

From the `spark` directory, run the following commands:

**Create directory:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/udf
```

**Verify:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/udf
```

**Copy file from host into the container:**

```shell
docker cp 07-udf/data/survey.csv spark-spark-worker-1:/data/udf/
```

## 2. Run the program

```shell
docker container stop udf || true &&
docker container rm udf || true &&
(cd 07-udf && rm my_util.zip || true && zip -r my_util.zip my_util/*) &&
docker run -ti --name udf \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit --py-files /spark/07-udf/my_util.zip /spark/07-udf/udf.py
```

## 3. Exercises

### 3.1 Exercise 1

Write a program to retrieve all records where the number of employees is greater than or equal to 500.

Hint: Write a UDF to process data on the `no_employees` column.

Expected result:

| Age | Gender | Country        | state | no_employees   |
|-----|--------|----------------|-------|----------------|
| 44  | Male   | United States  | IN    | More than 1000 |
| 36  | Male   | United States  | CT    | 500-1000       |
| 41  | Male   | United States  | IA    | More than 1000 |
| 35  | Male   | United States  | TN    | More than 1000 |
| 30  | Male   | United Kingdom | NA    | 500-1000       |
| 35  | Male   | United States  | TX    | More than 1000 |
| 35  | Male   | United States  | MI    | More than 1000 |
| 44  | Male   | United States  | IA    | More than 1000 |
