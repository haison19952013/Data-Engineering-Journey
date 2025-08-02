## 1. Tạo thư mục và copy file vào trong spark docker

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/udf
```

**Kiểm tra:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/udf
```

**Copy file từ host vào trong container:**

```shell
docker cp 07-udf/data/survey.csv spark-spark-worker-1:/data/udf/
```

## 2. Chạy chương trình

```shell
docker container stop udf || true &&
docker container rm udf || true &&
(cd 07-udf && rm gender_util.zip || true && zip -r gender_util.zip gender_util/*) &&
docker run -ti --name udf \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit --py-files /spark/07-udf/gender_util.zip /spark/07-udf/udf.py
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình lấy ra danh sách các bản ghi có số lượng employees lớn hơn hoặc bằng 500

Gợi ý: viết 1 hàm udf để xử lý dữ liệu trên cột `no_employees`

Ví dụ kết quả:

| Age | Gender | Country        | state | no_employees   |
|-----|--------|----------------|-------|----------------|
| 44  | Male   | United States  | IN    | More than 1000 |
| 36  | Male   | United States  | CT    | 500-1000       |
| 41  | Male   | United States  | IA    | More than 1000 |
| 35  | Male   | United States  | TN    | More than 1000 |
| 30  | Male   | United Kingdom | NA    | 500-1000       |
| 35  | Male   | United States  | TX    | More than 1000 |
| 35  | Male   | United States  | MI    | More than 1000 |
| 44  | Male   | United States  | IA    | More than 1000 |
