## 1. Create directory and copy files into the Spark Docker container

From the `spark` directory, run the following commands:

**Create directory:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/source-and-sink
```

**Verify:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/source-and-sink
```

**Copy file from host into the container:**

```shell
docker cp 08-source-and-sink/data/flight-time.parquet spark-spark-worker-1:/data/source-and-sink/
```

## 2. Run the program

```shell
docker container stop source-and-sink || true &&
docker container rm source-and-sink || true &&
docker run -ti --name source-and-sink \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
-v spark_lib:/opt/bitnami/spark/.ivy2 \
unigap/spark:3.5 spark-submit \
--packages org.apache.spark:spark-avro_2.12:3.5.1 \
/spark/08-source-and-sink/source_and_sink.py
```

## 3. Exercises

### 3.1 Exercise 1

Write a program to read data from the `json` directory created in the example above and retrieve a list of cancelled flights to Atlanta, GA in the year 2000.

Sort the data by flight date in descending order.

Expected result:

| DEST | DEST_CITY_NAME | FL_DATE    | ORIGIN | ORIGIN_CITY_NAME     | CANCELLED |
|------|----------------|------------|--------|----------------------|-----------|
| ATL  | Atlanta, GA    | 2000-01-01 | MCO    | Orlando, FL          | 1         |
| ATL  | Atlanta, GA    | 2000-01-01 | CAE    | Columbia, SC         | 1         |
| ATL  | Atlanta, GA    | 2000-01-01 | LEX    | Lexington, KY        | 1         |
| ATL  | Atlanta, GA    | 2000-01-01 | PNS    | Pensacola, FL        | 1         |
| ATL  | Atlanta, GA    | 2000-01-01 | GSO    | Greensboro/High P... | 1         |
| ATL  | Atlanta, GA    | 2000-01-01 | STL    | St. Louis, MO        | 1         |
| ATL  | Atlanta, GA    | 2000-01-01 | BHM    | Birmingham, AL       | 1         |
| ATL  | Atlanta, GA    | 2000-01-01 | PIT    | Pittsburgh, PA       | 1         |

### 3.2 Exercise 2

Write a program to read data from the `avro` directory created in the example above and retrieve a list of airlines `OP_CARRIER`, `ORIGIN`, and the number of cancelled flights.

Sort the data by `OP_CARRIER` and then by `ORIGIN`.

Expected result:

| OP_CARRIER | ORIGIN | NUM_CANCELLED_FLIGHT |
|------------|--------|----------------------|
| AA         | ABQ    | 4                    |
| AA         | ALB    | 6                    |
| AA         | AMA    | 2                    |
| AA         | ATL    | 30                   |
| AA         | AUS    | 25                   |
| AA         | BDL    | 33                   |
| AA         | BHM    | 4                    |
