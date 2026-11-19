# Lakehouse LaDe-D: phân tích giao hàng và dự đoán ETA

Đây là đồ án xây dựng hệ thống Lakehouse cho dữ liệu giao hàng LaDe-D tại Yantai. Hệ thống đi từ dữ liệu CSV, audit chất lượng, ETL bằng Spark/Delta Lake, xây dựng kho dữ liệu star schema, huấn luyện các bài toán ETA, ghi bảng serving vào MySQL và hiển thị trên dashboard Streamlit.

README này hướng dẫn từ lúc cài đặt đến lúc chạy pipeline, chạy từng notebook và kiểm tra kết quả.

## 1. Bài toán

Mỗi dòng trong file dữ liệu tương ứng với một order_id. Câu hỏi chính là:

> Tại một thời điểm quan sát được trạng thái giao hàng, đơn còn bao nhiêu phút nữa sẽ hoàn tất?

Công thức nhãn ETA:

~~~text
target_remaining_minutes = delivery_ts - query_ts
~~~

Cần phân biệt ba bài toán:

- Protocol P là bài toán chính theo quy trình LaDe. Query time là lúc shipper vừa giao xong một đơn; mô hình dự đoán thời gian còn lại của các đơn đang cầm.
- Protocol B/ETA-safe là bài toán phụ tại lúc nhận đơn, dùng để đo ảnh hưởng của leakage. Không so sánh trực tiếp MAE của Protocol B với Protocol P.
- Notebook 04 giữ baseline cũ dự đoán toàn bộ delivery_minutes ngay lúc nhận đơn.

## 2. Dữ liệu đầu vào

File mặc định:

~~~text
data/delivery_yt.csv
~~~

Thông tin dataset hiện tại:

| Thuộc tính | Giá trị |
|---|---:|
| Số dòng | 206.431 |
| Số cột | 17 |
| Thành phố | Yantai |
| Shipper | 205 |
| Khu vực | 17 |
| AOI | khoảng 8.163 |
| SHA-256 | aa5bca7fdaa8c89d4ab50a2ed2b94fb09f608acc49fc1108a1a612e9c0f5a7ad |

Các cột chính gồm order_id, courier_id, region_id, city, aoi_id, aoi_type, lat, lng, accept_time, delivery_time, các cột GPS và ds. Quy tắc thời điểm biết của từng cột nằm trong config/schema.yaml.

delivery_minutes là thời gian từ lúc shipper nhận đơn đến lúc giao xong, không phải thời gian lái xe.

## 3. Kiến trúc hệ thống

~~~text
delivery_yt.csv
    |
    v
Bronze Delta: dữ liệu thô + lineage
    |
    v
Silver Delta: ép kiểu + timestamp + quality flags
    |
    +--> delivery_rejected
    +--> dim_courier_silver (SCD2)
    +--> dim_region_gold / dim_aoi_gold / dim_date_gold / dim_hour_gold
    +--> fact_delivery_task_gold
    +--> eta_safe_gold       (Protocol B, bài toán phụ)
    +--> eta_paper_gold      (Protocol P, bài toán chính)
    |
    v
MySQL serving --> Streamlit dashboard
~~~

| Service | Vai trò | Địa chỉ |
|---|---|---|
| MinIO | Object storage S3 cho CSV và Delta | localhost:9000, console 9001 |
| Lakehouse | Hive Metastore và Spark Thrift Server | nội bộ |
| Jupyter | Chạy notebook và pipeline | http://localhost:8888 |
| Prefect | Điều phối flow | http://localhost:4200 |
| MySQL | Serving tables | localhost:3306 |
| Dashboard | Streamlit tiếng Việt | http://localhost:8501 |
| Metabase | Khám phá dữ liệu bổ sung | http://localhost:3000 |

Sơ đồ kho dữ liệu dạng bảng nằm tại docs/data_warehouse.txt. Chi tiết star schema nằm tại docs/star_schema.md.

## 4. Các tầng dữ liệu

Bronze giữ dữ liệu nguồn và metadata lineage, không âm thầm xóa dòng.

Silver ép kiểu số và timestamp, tính delivery_minutes, day_gap, khoảng cách Haversine và các cờ chất lượng. Dòng không hợp lệ được ghi vào delivery_rejected kèm reject_reason. Các dòng quá 24 giờ, qua ngày, thiếu GPS hoặc khoảng cách lớn vẫn được giữ để phân tích; từng cohort phải tự khai báo policy lọc.

Gold gồm fact_delivery_task_gold với grain một dòng cho một đơn và các dimension region, AOI, date, hour cùng lịch sử courier SCD2. SCD2 là thành phần kiến trúc để bảo toàn lịch sử, không được diễn giải là nguyên nhân làm ETA tốt hơn.

## 5. Protocol P

Protocol P dùng query time là lúc shipper vừa hoàn tất một đơn. Các đơn mục tiêu thỏa điều kiện:

~~~text
accept_ts < query_ts < delivery_ts
~~~

Nhãn:

~~~text
target_remaining_minutes = delivery_ts - query_ts
~~~

Quy trình hiện tại:

- giữ nhãn từ 1 đến 120 phút;
- cần ít nhất hai đơn đang còn cầm;
- cửa sổ tối đa 24 đơn;
- tối đa 12 query mỗi cửa sổ, với bước trượt 12 đơn;
- query được chọn bằng quy tắc xác định;
- chia theo ngày thành train/validation/test với tỷ lệ 60/20/20;
- chạy baseline median, SPEED và LightGBM với seed 42, 43, 44.

Kết quả ghi vào eta_paper_gold và eta_paper_metrics_gold. Bộ lọc 1-120 phút chỉ mô tả cohort đơn sắp giao; không được diễn giải là sai số của mọi đơn trong dataset. Chi tiết giới hạn xem docs/analysis_limits.md.

Protocol B ghi vào eta_safe_gold và eta_safe_metrics_gold. Feature phải tuân thủ thời điểm biết trong config/schema.yaml; không dùng dữ liệu tương lai.

## 6. Cài đặt

Yêu cầu:

- Docker Desktop và Docker Compose v2;
- Git;
- PowerShell trên Windows;
- ít nhất 6-8 GB RAM cấp cho Docker;
- khoảng 10 GB dung lượng trống;
- Internet ở lần build đầu.

Kiểm tra:

~~~powershell
docker --version
docker compose version
git --version
~~~

Clone project:

~~~powershell
git clone https://github.com/kOanhNe/Lakehouse-LaDe.git
cd Lakehouse-LaDe
~~~

Nếu project đã có trên máy:

~~~powershell
git switch main
git pull --ff-only origin main
~~~

Kiểm tra dữ liệu:

~~~powershell
Test-Path .\data\delivery_yt.csv
Get-FileHash .\data\delivery_yt.csv -Algorithm SHA256
~~~

Tạo cấu hình local:

~~~powershell
Copy-Item .env.example .env
~~~

Có thể giữ giá trị mặc định trong .env để chạy local. Không commit .env lên GitHub.

Dockerfile cần ba gói nén đặt ngay trong thư mục gốc của repo. Tải đúng phiên bản từ Apache Archive và giữ nguyên tên file.

- [Hadoop 3.3.6](https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz) (`hadoop-3.3.6.tar.gz`)
- [Hive 2.3.10](https://archive.apache.org/dist/hive/hive-2.3.10/apache-hive-2.3.10-bin.tar.gz) (`apache-hive-2.3.10-bin.tar.gz`)
- [Spark 3.5.3 cho Hadoop 3](https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz) (`spark-3.5.3-bin-hadoop3.tgz`)

Build và khởi động:

~~~powershell
docker compose up -d --build jupyter dashboard
docker compose ps
~~~

Chờ lakehouse, jupyter, minio, mysql và prefect ở trạng thái Up/healthy.
Nếu cần Metabase, chạy thêm `docker compose up -d metabase`.

## 7. Chạy toàn bộ pipeline

Cách khuyến nghị:

~~~powershell
docker compose exec jupyter python -u /home/jovyan/pipeline/flow.py
~~~

Flow chạy theo thứ tự:

~~~text
Bronze -> Silver -> courier SCD2 -> star dimensions
      -> ETA-safe 4b -> Protocol P 4c
      -> fact/baseline 4 -> serving MySQL
~~~

Theo dõi log trong terminal đang chạy lệnh hoặc mở Prefect tại http://localhost:4200. Flow hoàn tất khi log có `Finished in state Completed()`. Không chạy song song hai flow vì các bảng Delta được ghi ở chế độ overwrite.

## 8. Chạy notebook đúng thứ tự

Notebook chỉ là lớp trình bày gọi lại code trong pipeline/steps.py. Mở Jupyter:

~~~powershell
docker compose exec jupyter jupyter server list
~~~

Mở URL có dạng http://127.0.0.1:8888/lab?token=... rồi chạy từng cell bằng Shift + Enter.

Thứ tự bắt buộc:

| Thứ tự | Notebook | Kết quả |
|---:|---|---|
| 0 | 00_data_audit.ipynb | EDA và reports/data |
| 1 | 01_ingest_bronze.ipynb | delivery_bronze |
| 2 | 02_transform_silver.ipynb | delivery_silver, delivery_rejected |
| 3 | 03_dim_courier_silver.ipynb | courier SCD2 |
| 3b | 03b_star_dimensions.ipynb | region/AOI/date/hour dimensions |
| 4 | 04_fact_and_ml_gold.ipynb | fact Gold và baseline phụ |
| 4b | 04b_eta_safe.ipynb | eta_safe_gold và metrics |
| 4c | 04c_eta_paper.ipynb | Protocol P, SPEED, LightGBM |
| 5 | 05_serving_layer.ipynb | MySQL serving |

Chờ cell của notebook trước trả về kết quả rồi mới chạy notebook tiếp theo. Nếu báo PATH_NOT_FOUND, bảng upstream chưa được tạo hoặc notebook đang chạy sai thứ tự. Không chạy hai notebook ghi cùng bảng cùng lúc.

Chạy một notebook bằng lệnh:

~~~powershell
docker compose exec jupyter jupyter nbconvert --to notebook --execute --ExecutePreprocessor.timeout=-1 --output 04c_eta_paper.ipynb /home/jovyan/work/04c_eta_paper.ipynb
~~~

## 9. Dashboard và kết quả

Sau khi pipeline hoặc notebook 05 hoàn tất, mở http://localhost:8501.

Dashboard đọc dữ liệu từ MySQL, gồm tổng quan giao hàng, shipper/khu vực, chất lượng dữ liệu, lineage, ETA-safe và Protocol P. Dashboard không tự chạy pipeline.

Các Delta table chính:

~~~text
delivery_bronze
delivery_silver
delivery_rejected
dim_courier_silver
dim_region_gold
dim_aoi_gold
dim_date_gold
dim_hour_gold
fact_delivery_task_gold
eta_safe_gold
eta_safe_metrics_gold
eta_paper_gold
eta_paper_metrics_gold
~~~

Kiểm tra MySQL:

~~~powershell
docker compose exec mysql mysql -udelivery -pdelivery delivery_source -e "SHOW TABLES;"
~~~

## 10. Kiểm tra và test

Nếu chạy kiểm tra ngoài Docker trên Windows, tạo môi trường ảo bằng Python 3.12 và cài thư viện từ `requirements.txt`.

~~~powershell
py -3.12 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe scripts/validate_schema.py
.venv\Scripts\python.exe -m pytest -q
~~~

Chạy Protocol P ngoài Docker:

~~~powershell
.venv\Scripts\python.exe scripts/run_paper_protocol.py --source data/delivery_yt.csv
~~~

Phân tích độ nhạy nhãn:

~~~powershell
.venv\Scripts\python.exe scripts/run_label_sensitivity.py
~~~

## 11. Lỗi thường gặp

Jupyter yêu cầu token: lấy token mới bằng docker compose exec jupyter jupyter server list.

JAVA_GATEWAY_EXITED: Docker thiếu RAM hoặc Spark chưa khởi động xong. Chờ service healthy rồi chạy docker compose restart jupyter.

PATH_NOT_FOUND khi đọc Delta: chạy lại từ notebook tạo bảng upstream.

pandas dtypes must be int, float or bool: aoi: image cũ chưa chuẩn hóa aoi_type. Chạy docker compose up -d --build jupyter.

Spark WindowExec No Partition Defined: đây là cảnh báo hiệu năng; xem task đang chạy và chờ flow hoàn tất trước khi kết luận lỗi.

Dashboard chưa có dữ liệu: chạy flow hoặc notebook 05 trước, sau đó docker compose restart dashboard.

Dừng service nhưng giữ volume:

~~~powershell
docker compose down
~~~

Xóa toàn bộ volume local:

~~~powershell
docker compose down -v
~~~

## 12. Tài liệu tham khảo và giới hạn

Dataset hiện chỉ đại diện cho Yantai, không được suy rộng kết quả sang thành phố khác. Protocol P dùng toPandas cho khoảng 206 nghìn dòng, phù hợp phạm vi đồ án hiện tại nhưng chưa phải thiết kế huấn luyện phân tán cho dữ liệu rất lớn.

Nguồn tham khảo:

- LaDe repository: https://github.com/wenhaomin/LaDe
- LaDe dataset: https://huggingface.co/datasets/Cainiao-AI/LaDe
- Data dictionary: docs/data_dictionary.md
- Schema và allowlist: config/schema.yaml
- Phân tích giới hạn: docs/analysis_limits.md
