## 1. Tạo thư mục và copy file vào trong spark docker

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/window-function
```

**Kiểm tra:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/window-function
```

**Copy file từ host vào trong container:**

```shell
docker cp 06-window-function/data/summary.parquet spark-spark-worker-1:/data/window-function/
```

## 2. Chạy chương trình

```shell
docker container stop window-function || true &&
docker container rm window-function || true &&
docker run -ti --name window-function \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/06-window-function/window_function.py
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình lấy ra danh sách các quốc gia, tuần, số hóa đơn, tổng số sản phẩm, tổng giá trị hóa đơn và xếp hạng
theo tiêu chí tổng số tiền nhiều nhất trên từng quốc gia

Dữ liệu sắp theo tên quốc gia và xếp hạng tăng dần.

Ví dụ kết quả:

| Country   | WeekNumber | NumInvoices | TotalQuantity | InvoiceValue | rank |
|-----------|------------|-------------|---------------|--------------|------|
| Australia | 50         | 2           | 133           | 387.95       | 1    |
| Australia | 48         | 1           | 107           | 358.25       | 2    |
| Australia | 49         | 1           | 214           | 258.9        | 3    |
| Austria   | 50         | 2           | 3             | 257.04       | 1    |
| Bahrain   | 51         | 1           | 54            | 205.74       | 1    |
| Belgium   | 51         | 2           | 942           | 838.65       | 1    |
| Belgium   | 50         | 2           | 285           | 625.16       | 2    |
| Belgium   | 48         | 1           | 528           | 346.1        | 3    |

### 3.2 Yêu cầu 2

Viết chương trình lấy ra danh sách các quốc gia, tuần, số hóa đơn, số sản phẩm, giá trị hóa đơn và tổng giá trị hóa đơn
tính tính đến tuần của bản ghi hiện tại, phần trăm tăng của giá trị hóa đơn so với tuần trước đó.

Dữ liệu sắp xếp theo tên quốc gia, tuần.

Ví dụ kết quả:

| Country   | WeekNumber | NumInvoices | TotalQuantity | InvoiceValue | PercentGrowth | AccumulateValue |
|-----------|------------|-------------|---------------|--------------|---------------|-----------------|
| Australia | 48         | 1           | 107           | 358.25       | 0.0           | 358.25          |
| Australia | 49         | 1           | 214           | 258.9        | -27.73        | 617.15          |
| Australia | 50         | 2           | 133           | 387.95       | 49.85         | 1005.1          |
| Austria   | 50         | 2           | 3             | 257.04       | 0.0           | 257.04          |
| Bahrain   | 51         | 1           | 54            | 205.74       | 0.0           | 205.74          |
| Belgium   | 48         | 1           | 528           | 346.1        | 0.0           | 346.1           |
| Belgium   | 50         | 2           | 285           | 625.16       | 80.63         | 971.26          |
| Belgium   | 51         | 2           | 942           | 838.65       | 34.15         | 1809.91         |
