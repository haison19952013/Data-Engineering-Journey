## 1. Create directory and copy files into the Spark Docker container

From the `spark` directory, run the following commands:

**Create directory:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/window-function
```

**Verify:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/window-function
```

**Copy file from host into the container:**

```shell
docker cp 06-window-function/data/summary.parquet spark-spark-worker-1:/data/window-function/
```

## 2. Run the program

```shell
docker container stop window-function || true &&
docker container rm window-function || true &&
docker run -ti --name window-function \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/06-window-function/window_function.py
```

## 3. Exercises

### 3.1 Exercise 1

Write a program to retrieve a list of countries, week number, number of invoices, total quantity, total invoice value, and rank by highest total invoice value per country.

Sort the data by country name and rank in ascending order.

Expected result:

| Country   | WeekNumber | NumInvoices | TotalQuantity | InvoiceValue | rank |
|-----------|------------|-------------|---------------|--------------|------|
| Australia | 50         | 2           | 133           | 387.95       | 1    |
| Australia | 48         | 1           | 107           | 358.25       | 2    |
| Australia | 49         | 1           | 214           | 258.9        | 3    |
| Austria   | 50         | 2           | 3             | 257.04       | 1    |
| Bahrain   | 51         | 1           | 54            | 205.74       | 1    |
| Belgium   | 51         | 2           | 942           | 838.65       | 1    |
| Belgium   | 50         | 2           | 285           | 625.16       | 2    |
| Belgium   | 48         | 1           | 528           | 346.1        | 3    |

### 3.2 Exercise 2

Write a program to retrieve a list of countries, week number, number of invoices, total quantity, invoice value, cumulative invoice value up to the current week, and the percentage growth in invoice value compared to the previous week.

Sort the data by country name and week number.

Expected result:

| Country   | WeekNumber | NumInvoices | TotalQuantity | InvoiceValue | PercentGrowth | AccumulateValue |
|-----------|------------|-------------|---------------|--------------|---------------|-----------------|
| Australia | 48         | 1           | 107           | 358.25       | 0.0           | 358.25          |
| Australia | 49         | 1           | 214           | 258.9        | -27.73        | 617.15          |
| Australia | 50         | 2           | 133           | 387.95       | 49.85         | 1005.1          |
| Austria   | 50         | 2           | 3             | 257.04       | 0.0           | 257.04          |
| Bahrain   | 51         | 1           | 54            | 205.74       | 0.0           | 205.74          |
| Belgium   | 48         | 1           | 528           | 346.1        | 0.0           | 346.1           |
| Belgium   | 50         | 2           | 285           | 625.16       | 80.63         | 971.26          |
| Belgium   | 51         | 2           | 942           | 838.65       | 34.15         | 1809.91         |
