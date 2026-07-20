## 1. Create directory and copy files into the Spark Docker container

From the `spark` directory, run the following commands:

**Create directory:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/spark-sql
```

**Verify:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/
```

**Copy file from host into the container:**

```shell
docker cp 02-spark-sql/data/survey.csv spark-spark-worker-1:/data/spark-sql
```

## 2. Run the program

```shell
docker container stop spark-sql || true &&
docker container rm spark-sql || true &&
docker run -ti --name spark-sql \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/02-spark-sql/spark_sql.py
```

## 3. Exercises

### 3.1 Exercise 1

Write a program using `Spark SQL` to retrieve a list of countries and the number of male respondents under 40 years old.

A person is male if the `Gender` field has a value of `male` or `m` (case-insensitive).

Sort the data by count in ascending order. If the count is equal, sort by country name.

Expected result:

| Country | Count |
|---------|-------|
| France  | 11    |
| India   | 10    |    
| Italy   | 7     |
| Sweden  | 7     |

### 3.2 Exercise 2

Write a program using `Spark SQL` to retrieve a list of countries along with the number of male and female respondents per country.

A person is male if the `Gender` field has a value of `male`, `man`, or `m` (case-insensitive).

A person is female if the `Gender` field has a value of `female`, `woman`, or `w` (case-insensitive).

Sort the data by country name.

Expected result:

| Country | num_male | num_female |
|---------|----------|------------|
| France  | 8        | 3          |
| India   | 10       | 2          |    
| Italy   | 7        | 6          |
| Sweden  | 7        | 9          |
