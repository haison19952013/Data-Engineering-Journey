# Project Requirements

## Overview

This project combines the use of `kafka` and `spark`. It uses `spark` to read data from `kafka`, then process, compute, and store the results in a `postgres` database.

## Problem Statement

**Input:**

- Kafka: A `Kafka` cluster set up locally with a `topic` containing user behavior data on a website, as built in the `Kafka` module project.
- Spark: A `Spark` cluster installed locally during the course.
- The data schema.

**Output:**

- Database design.
- Code to process the project requirements.
- Results of the required reports.
- Data stored in the `Postgres` database.

## Description

**Input data schema:**

| Name         | Data Type     | Description                                          | Example                                                                                                                                                             |
|--------------|---------------|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id           | String        | Log ID                                               | aea4b823-c5c6-485e-8b3b-6182a7c4ecce                                                                                                                                |
| api_version  | String        | API version                                          | 1.0                                                                                                                                                                 | 
| collection   | String        | Log type                                             | view_product_detail                                                                                                                                                 | 
| current_url  | String        | URL of the web page the user is visiting             | https://www.glamira.cl/glamira-anillo-saphira-skug100335.html?alloy=white-375&diamond=sapphire&stone2=diamond-Brillant&itm_source=recommendation&itm_medium=sorting |
| device_id    | String        | Device ID                                            | 874db849-68a6-4e99-bcac-fb6334d0ec80                                                                                                                                |
| email        | String        | User email address                                   |                                                                                                                                                                     |
| ip           | String        | IP address                                           | 190.163.166.122                                                                                                                                                     |
| local_time   | String        | Log creation time. Format: yyyy-MM-dd HH:mm:ss       | 2024-05-28 08:31:22                                                                                                                                                 |
| option       | Array<Object> | List of product options                              | `[{"option_id": "328026", "option_label": "diamond"}]`                                                                                                              |
| product_id   | String        | Product ID                                           | 96672                                                                                                                                                               |
| referrer_url | String        | Web URL that referred to the `current_url`           | https://www.google.com/                                                                                                                                             |
| store_id     | String        | Store ID                                             | 85                                                                                                                                                                  |
| time_stamp   | Long          | Timestamp when the log record was created            |                                                                                                                                                                     |
| user_agent   | String        | Browser and device information                       | Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1                           |

**Requirements:**

Design the database and write a program to produce the following reports:

- Top 10 `product_id`s with the highest number of views for the current day.
- Top 10 countries with the highest number of views for the current day (country is determined from the `domain`).
- Top 5 `referrer_url`s with the highest number of views for the current day.
- For any given country, retrieve the list of `store_id`s and their corresponding view counts, sorted by views in descending order.
- View data distributed by hour for any given `product_id` on a given day.
- Hourly view data for each `browser` and `os`.

## Appendix

**How to run the program with external libraries using a virtual environment**

```
(cd 99-project && zip -r browser.zip browser/*) &&
docker container stop test-spark || true &&
docker container rm test-spark || true &&
docker run -ti --name test-spark \
--network=streaming-network \
-p 4040:4040 \
-v ./:/spark \
-v spark_lib:/opt/bitnami/spark/.ivy2 \
-v spark_data:/data \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "python -m venv pyspark_venv &&
source pyspark_venv/bin/activate &&
pip install -r /spark/requirements.txt &&
venv-pack -o pyspark_venv.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--archives pyspark_venv.tar.gz#environment \
--py-files /spark/99-project/browser.zip \
/spark/99-project/test.py"
```

## References

[Python Package Management](https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html)

[JDBC To Other Databases](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
