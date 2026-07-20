## 1. Create directory and copy files into the Spark Docker container

From the `spark` directory, run the following commands:

**Create directories:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/dataframe-join/d1
docker exec -ti spark-spark-worker-1 mkdir -p /data/dataframe-join/d2
```

**Verify:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/dataframe-join
```

**Copy files from host into the container:**

```shell
for f in 05-dataframe-join/data/d1/*.json; do docker cp $f spark-spark-worker-1:/data/dataframe-join/d1/; done
```

```shell
for f in 05-dataframe-join/data/d2/*.json; do docker cp $f spark-spark-worker-1:/data/dataframe-join/d2/; done
```

## 2. Run the program

```shell
docker container stop dataframe-join || true &&
docker container rm dataframe-join || true &&
docker run -ti --name dataframe-join \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/05-dataframe-join/dataframe_join.py
```

## 3. Exercises

### 3.1 Exercise 1

Write a program to retrieve a list of cancelled flights to Atlanta, GA in the year 2000.

Sort the data by flight date in descending order.

Expected result:

| id         | DEST | DEST_CITY_NAME | FL_DATE    | ORIGIN | ORIGIN_CITY_NAME   | CANCELLED |
|------------|------|----------------|------------|--------|--------------------|-----------|
| 168686     | ATL  | Atlanta, GA    | 2000-12-01 | PHX    | Phoenix, AZ        | 1         |
| 165272     | ATL  | Atlanta, GA    | 2000-12-01 | BOS    | Boston, MA         | 1         |
| 8589938391 | ATL  | Atlanta, GA    | 2000-12-01 | LGA    | New York, NY       | 1         |
| 8589938541 | ATL  | Atlanta, GA    | 2000-12-01 | STL    | St. Louis, MO      | 1         |
| 8589938399 | ATL  | Atlanta, GA    | 2000-12-01 | LGA    | New York, NY       | 1         |
| 8589938520 | ATL  | Atlanta, GA    | 2000-12-01 | SLC    | Salt Lake City, UT | 1         |
| 8589938558 | ATL  | Atlanta, GA    | 2000-12-01 | TLH    | Tallahassee, FL    | 1         |
| 8589938397 | ATL  | Atlanta, GA    | 2000-12-01 | LGA    | New York, NY       | 1         |
| 168522     | ATL  | Atlanta, GA    | 2000-12-01 | BOS    | Boston, MA         | 1         |
| 165432     | ATL  | Atlanta, GA    | 2000-12-01 | DTW    | Detroit, MI        | 1         |
| 8589938393 | ATL  | Atlanta, GA    | 2000-12-01 | LGA    | New York, NY       | 1         |
| 8589938370 | ATL  | Atlanta, GA    | 2000-12-01 | LAS    | Las Vegas, NV      | 1         |

### 3.2 Exercise 2

Write a program to retrieve a list of destinations, year, and the total number of cancelled flights for that year.

Sort the data by destination code and then by year.

Expected result:

| DEST | FL_YEAR | NUM_CANCELLED_FLIGHT |
|------|---------|----------------------|
| ABE  | 2000    | 5                    |
| ABQ  | 2000    | 15                   |
| AGS  | 2000    | 1                    |
| ALB  | 2000    | 12                   |
| AMA  | 2000    | 5                    |
| ANC  | 2000    | 36                   |
