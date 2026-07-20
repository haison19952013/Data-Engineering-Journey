## Overview

Giờ là lúc luyện tập thêm về `connection`, `operator`, `sensor`, `hook`.

Hãy xây dựng 1 data pipeline trên airflow thực hiện các yêu cầu sau:

1. Extract dữ liệu từ api [https://dummyjson.com/products](https://dummyjson.com/products). API trả về danh
   sách `products`.
2. Dữ liệu extract được lưu ra file `.csv`
3. Tạo bảng tên `products` trong db postgres
4. Load dữ liệu từ file `.csv` vào trong bảng `products`