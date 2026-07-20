Tại thư mục `spark`, chạy lệnh sau:

## Remove any existing container named hello-spark
```shell
docker container stop hello-spark || true &&
```

## Remove the existing hello-spark container
```shell 
docker container rm hello-spark || true && 
```

## Run the hello-spark container in interactive mode, in the streaming-network, mounting the current directory to /spark in the container, and executing the hello_spark.py script
```shell
docker run -ti --name hello-spark --network=streaming-network -v ./:/spark unigap/spark:3.5 spark-submit /spark/01-hello-spark/hello_spark.py
```