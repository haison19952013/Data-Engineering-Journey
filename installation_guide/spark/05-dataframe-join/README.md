## 1. Tạo thư mục và copy file vào trong spark docker

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/dataframe-join/d1
docker exec -ti spark-spark-worker-1 mkdir -p /data/dataframe-join/d2
```

**Kiểm tra:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/dataframe-join
```

**Copy file từ host vào trong container:**

```shell
for f in 05-dataframe-join/data/d1/*.json; do docker cp $f spark-spark-worker-1:/data/dataframe-join/d1/; done
```

```shell
for f in 05-dataframe-join/data/d2/*.json; do docker cp $f spark-spark-worker-1:/data/dataframe-join/d2/; done
```

## 2. Chạy chương trình

```shell
docker container stop dataframe-join || true &&
docker container rm dataframe-join || true &&
docker run -ti --name dataframe-join \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/05-dataframe-join/dataframe_join.py
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình lấy ra danh sách các chuyến bay bị hủy tới thành phố Atlanta, GA trong năm 2000

Dữ liệu sắp theo theo ngày bay giảm dần.

Ví dụ kết quả:

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

### 3.2 Yêu cầu 2

Viết chương trình lấy ra danh sách các destination, năm và tổng số chuyến bay bị hủy của năm đó.

Dữ liệu sắp xếp theo mã destination và theo năm.

Ví dụ kết quả:

| DEST | FL_YEAR | NUM_CANCELLED_FLIGHT |
|------|---------|----------------------|
| ABE  | 2000    | 5                    |
| ABQ  | 2000    | 15                   |
| AGS  | 2000    | 1                    |
| ALB  | 2000    | 12                   |
| AMA  | 2000    | 5                    |
| ANC  | 2000    | 36                   |
