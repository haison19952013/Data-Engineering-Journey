## 1. Tạo thư mục và copy file vào trong spark docker

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/spark-sql
```

**Kiểm tra:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/
```

**Copy file từ host vào trong container:**

```shell
docker cp 02-spark-sql/data/survey.csv spark-spark-worker-1:/data/spark-sql
```

## 2. Chạy chương trình

```shell
docker container stop spark-sql || true &&
docker container rm spark-sql || true &&
docker run -ti --name spark-sql \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/02-spark-sql/spark_sql.py
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình sử dụng `Spark SQL` lấy ra danh sách các quốc gia và số người là nam có độ tuổi < 40.

Một người là nam thì trường `Gender` sẽ có giá trị là `male` hoặc `m` (lưu ý không phân biệt viết hoa/thường).

Dữ liệu sắp xếp theo số người tăng dần. Nếu số người bằng nhau thì sắp xếp theo tên quốc gia.

Ví dụ kết quả:

| Country | Count |
|---------|-------|
| France  | 11    |
| India   | 10    |    
| Italy   | 7     |
| Sweden  | 7     |

### 3.2 Yêu cầu 2

Viết chương trình sử dụng `Spark SQL` lấy ra danh sách quốc gia và số nam, nữ của từng quốc gia.

Một người là nam thì trường `Gender` sẽ có giá trị là `male` hoặc `man` hoặc `m` (lưu ý không phân biệt viết
hoa/thường).

Một người là nữ thì trường `Gender` sẽ có giá trị là `female` hoặc `woman` hoặc `w` (lưu ý không phân biệt viết
hoa/thường).

Dữ liệu sắp xếp theo tên quốc gia.

Ví dụ kết quả:

| Country | num_male | num_female |
|---------|----------|------------|
| France  | 8        | 3          |
| India   | 10       | 2          |    
| Italy   | 7        | 6          |
| Sweden  | 7        | 9          |