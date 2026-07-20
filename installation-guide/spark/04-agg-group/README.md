## 1. Create directory and copy files into the Spark Docker container

From the `spark` directory, run the following commands:

**Create directory:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/agg-group
```

**Verify:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/
```

**Copy file from host into the container:**

```shell
docker cp 04-agg-group/data/invoices.csv spark-spark-worker-1:/data/agg-group
```

## 2. Run the program

```shell
docker container stop agg-group || true &&
docker container rm agg-group || true &&
docker run -ti --name agg-group \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/04-agg-group/agg_group.py
```

## 3. Exercises

### 3.1 Exercise 1

Write a program to retrieve a list of countries, year, number of invoices, total quantity, and total invoice value per country and year.

Sort the data by country name and then by year.

Expected result:

| Country   | Year | num_invoices | total_quantity | invoice_value      |
|-----------|------|--------------|----------------|--------------------|
| Australia | 2010 | 4            | 454            | 1005.1000000000001 |
| Australia | 2011 | 65           | 83199          | 136072.16999999998 |
| Austria   | 2010 | 2            | 3              | 257.03999999999996 |
| Austria   | 2011 | 17           | 4824           | 9897.28            |

### 3.2 Exercise 2

Write a program to retrieve the top 10 customers with the highest total purchase amount in 2010.

Sort the data by total amount in descending order. If the amounts are equal, sort by customer ID in ascending order.

Expected result:

| CustomerID | invoice_value |
|------------|---------------|
| 18102      | 27834.61      |
| 15061      | 19950.66      |
| 16029      | 13112.52      |
