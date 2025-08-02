## 1. Tạo thư mục và copy file vào trong spark docker

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/agg-group
```

**Kiểm tra:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/
```

**Copy file từ host vào trong container:**

```shell
docker cp 04-agg-group/data/invoices.csv spark-spark-worker-1:/data/agg-group
```

## 2. Chạy chương trình

```shell
docker container stop agg-group || true &&
docker container rm agg-group || true &&
docker run -ti --name agg-group \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/04-agg-group/agg_group.py
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình lấy ra danh sách các quốc gia, năm, số hóa đơn, số lượng sản phẩm, tổng sô tiền của từng quốc gia và
năm đó

Dữ liệu sắp xếp theo tên quốc gia và theo năm.

Ví dụ kết quả:

| Country   | Year | num_invoices | total_quantity | invoice_value      |
|-----------|------|--------------|----------------|--------------------|
| Australia | 2010 | 4            | 454            | 1005.1000000000001 |
| Australia | 2011 | 65           | 83199          | 136072.16999999998 |
| Austria   | 2010 | 2            | 3              | 257.03999999999996 |
| Austria   | 2011 | 17           | 4824           | 9897.28            |

### 3.2 Yêu cầu 2

Viết chương trình lấy ra top 10 khách hàng có số tiền mua hàng nhiều nhất trong năm 2010

Dữ liệu sắp xếp theo số tiền giảm dần, nếu số tiền bằng nhau thì sắp xếp theo mã khách hàng tăng dần

Ví dụ kết quả:

| CustomerID | invoice_value |
|------------|---------------|
| 18102      | 27834.61      |
| 15061      | 19950.66      |
| 16029      | 13112.52      |
