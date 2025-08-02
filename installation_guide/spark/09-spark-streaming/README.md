## 1. Run netcat service

```shell
docker container stop netcat || true &&
docker container rm netcat || true &&
docker run -ti --name netcat \
--network=streaming-network \
alpine:3.14 \
/bin/sh -c "apk add --no-cache netcat-openbsd && nc -lk 9999"
```

## 2. Chạy chương trình

```shell
docker container stop spark-streaming || true &&
docker container rm spark-streaming || true &&
docker run -ti --name spark-streaming \
--network=streaming-network \
-p 4040:4040 \
-v ./:/spark \
unigap/spark:3.5 spark-submit \
/spark/09-spark-streaming/spark_streaming.py
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình đếm từ và in ra danh sách các từ có số lần xuất hiện là chẵn.

Ví dụ kết quả:

```
('x', 2)
('z', 4)
```

### 3.2 Yêu cầu 2

Viết chương trình đếm từ và in ra danh sách các từ có độ dài lớn hơn 1 và có số lần xuất hiện là lẻ.

Ví dụ kết quả:

```
('yy', 1)
('zz', 3)
('ttt', 1)
```
