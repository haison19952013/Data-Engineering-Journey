## 1. Tạo thư mục và copy file vào trong spark docker

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/dataframe-api
```

**Kiểm tra:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/
```

**Copy file từ host vào trong container:**

```shell
docker cp 03-dataframe-api/data/survey.csv spark-spark-worker-1:/data/dataframe-api
```

## 2. Chạy chương trình

```shell
docker container stop dataframe-api || true &&
docker container rm dataframe-api || true &&
docker run -ti --name dataframe-api \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/03-dataframe-api/dataframe_api.py
```

## 3. Yêu cầu

Làm lại các yêu cầu của phần [spark-sql](../02-spark-sql) nhưng sử dụng `DataFrame API`