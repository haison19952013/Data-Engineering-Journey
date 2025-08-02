## 1. Tạo thư mục và copy file vào trong spark docker

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/source-and-sink
```

**Kiểm tra:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/source-and-sink
```

**Copy file từ host vào trong container:**

```shell
docker cp 08-source-and-sink/data/flight-time.parquet spark-spark-worker-1:/data/source-and-sink/
```

## 2. Chạy chương trình

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

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình đọc dữ liệu từ thư mục `json` tạo được trong ví dụ trên và lấy ra danh sách các chuyến bay bị hủy tới
thành phố Atlanta, GA trong năm 2000

Dữ liệu sắp theo theo ngày bay giảm dần.

Ví dụ kết quả:

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

### 3.2 Yêu cầu 2

Viết chương trình đọc dữ liệu từ thư mục `avro` tạo được trong ví dụ trên và lấy ra danh sách các hãng
bay `OP_CARRIER`, `ORIGIN` và số chuyến bay bị hủy

Dữ liệu sắp theo theo `OP_CARRIER` và `ORIGIN`.

Ví dụ kết quả:

| OP_CARRIER | ORIGIN | NUM_CANCELLED_FLIGHT |
|------------|--------|----------------------|
| AA         | ABQ    | 4                    |
| AA         | ALB    | 6                    |
| AA         | AMA    | 2                    |
| AA         | ATL    | 30                   |
| AA         | AUS    | 25                   |
| AA         | BDL    | 33                   |
| AA         | BHM    | 4                    |
