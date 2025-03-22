# Warm up with Data Engineering
## Data cleanup and transformation
1. Chuẩn hóa cột salary về dạng số, xử lý các giá trị như "Thoả thuận", "Trên X triệu", "X - Y triệu", "Tới X triệu", etc. -> `def wrangle_salary()` in `data_pipeline.py`
2. Tạo thêm các cột phụ: min_salary, max_salary, salary_unit (VND/USD) -> `def wrangle_salary()` in `data_pipeline.py`
3. Xử lý cột address để tách thành city và district -> `def wrangle_address()` in `data_pipeline.py`
4. Chuẩn hóa job_title để gom nhóm các vị trí tương tự (ví dụ: "Software Engineer", "Developer", "Programmer" có thể gom vào một nhóm) -> `def wrangle_job_title()` in `data_pipeline.py`

## Data pipeline
1. Basic ETL design: .csv extraction, transformation, and loading into a database -> class `data_pipeline()` in `data_pipeline.py` w/ the flow: `data_wrangle()` -> `db_conn()` -> `save_to_db()`

2. Automate job schedule by cron job


Step 1: Open cron tab

```
crontab -e
```
Step 2: Add the cron job schedule, then save and exit
```
0 8 * * * /bin/bash -c 'source /home/sonhaile/miniconda3/etc/profile.d/conda.sh && conda activate data_engineering && cd /home/sonhaile/Data-Engineering-Journey/warm_up && python data_pipeline.py' >> /home/sonhaile/cron_log.txt 2>&1
````
Note: Install the a simple MTA like `postfix` to send email to the user's mailbox (in this case, for generating the `cronlog.txt`). 
```
sudo apt-get install postfix
```

Step 3: verify the cron job
```
crontab -l
```
Step 4: test the cron job
```
source /home/sonhaile/miniconda3/etc/profile.d/conda.sh && conda activate data_engineering && cd /home/sonhaile/Data-Engineering-Journey/warm_up && python data_pipeline.py
```
3. Error handling for common errors of `DataPipeline()` in `data_pipeline.py`: 
- `wrangle_data()` -> File not found
- `conn_db()` -> Connection error
- `read_from_db()` -> Table not found