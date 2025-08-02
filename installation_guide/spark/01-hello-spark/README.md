Tại thư mục `spark`, chạy lệnh sau:

```shell
docker container stop hello-spark || true && 
docker container rm hello-spark || true && 
docker run -ti --name hello-spark --network=streaming-network -v ./:/spark unigap/spark:3.5 spark-submit /spark/01-hello-spark/hello_spark.py
```