# Lakehouse LaDe-D Yantai

Đây là dự án xây dựng một Lakehouse phân tích dữ liệu giao hàng từ bộ LaDe-D Yantai. Pipeline đi từ kiểm tra dữ liệu nguồn, lưu Bronze, làm sạch Silver, tạo Gold, xây dựng ETA tại thời điểm truy vấn và phục vụ kết quả cho MySQL/Metabase.

## Mục tiêu dự án

- Hiểu rõ cấu trúc và chất lượng dữ liệu trước khi xử lý.
- Giữ nguyên dữ liệu nguồn trong Bronze để truy vết.
- Chuẩn hóa timestamp, tọa độ, thời lượng và quality flags ở Silver.
- Xây dựng lịch sử vùng hoạt động của tài xế theo thời gian bằng SCD2.
- Tạo fact delivery với phép nối point-in-time.
- Dự đoán ETA tại query time: đơn phải đang chờ giao và mục tiêu là số phút còn lại đến lúc hoàn tất.
- Phục vụ bảng tổng hợp và kết quả đánh giá qua MySQL/Metabase.

## Kiến trúc

```text
delivery_yt.csv
       |
       v
Bronze Delta -> Silver Delta -> Gold Delta -> MySQL / Metabase
                      |              |
                      |              +-- fact_delivery_task_gold
                      +-- eta_query_samples -> eta_eval_by_cohort
```

Các dịch vụ Docker:

| Dịch vụ | Vai trò | Địa chỉ |
|---|---|---|
| MinIO | Object storage tương thích S3 cho Delta Lake | `http://localhost:9001` |
| Spark/Hive | Xử lý Spark, Delta Lake và metastore | nội bộ Docker |
| Jupyter | Chạy notebook | `http://localhost:8888` |
| MySQL | Lưu bảng serving | `localhost:3306` |
| Metabase | Dashboard và truy vấn | `http://localhost:3000` |

## Dữ liệu

Bộ dữ liệu có 206.431 đơn hàng, 205 tài xế, 17 vùng, 8.163 AOI và 16 mã `aoi_type`. `delivery_minutes` là thời gian từ lúc nhận đơn đến lúc hoàn tất giao, không phải thời gian lái xe.

Có đơn giao khác ngày nhận và đơn kéo dài trên 24 giờ; các dòng này được giữ lại và gắn cờ. Timestamp chỉ chính xác đến phút nên pipeline dùng `order_id` làm tie-breaker. GPS zero-island được xem là không hợp lệ khi `abs(lat) < 0.01` và `abs(lng) < 0.01`.

Kết quả ETA query-time là bài toán khác với thí nghiệm delivery-time trong bài báo LaDe; không so sánh trực tiếp với Table 4. Xem thêm [báo cáo dữ liệu](reports/data/README.md), [data dictionary](docs/data_dictionary.md) và [summary](reports/data/summary.json).

## Cài đặt

Cần Docker Desktop/Docker Engine, Docker Compose, tối thiểu 8 GB RAM cho Docker và khoảng 15 GB dung lượng trống. Nếu chạy kiểm tra ngoài Docker, cần Python 3.10 trở lên.

```powershell
git clone https://github.com/kOanhNe/Lakehouse-LaDe.git
cd Lakehouse-LaDe
docker --version
docker compose version
docker compose up -d --build
docker compose ps
```

Build lần đầu có thể mất vài phút vì image cài Java, Hadoop, Hive, Spark và Delta Lake.

## Nạp dữ liệu

Đặt file nguồn tại `data/delivery_yt.csv`, sau đó mở MinIO tại `http://localhost:9001`:

```text
User: minioadmin
Password: minioadmin
```

Chọn bucket `lakehouse` và upload `delivery_yt.csv` vào thư mục gốc. Notebook đọc dữ liệu từ `s3a://lakehouse/delivery_yt.csv`.

## Chạy pipeline

Mở Jupyter tại `http://localhost:8888` và chạy theo thứ tự:

1. `00_data_audit.ipynb` — audit toàn bộ dữ liệu trước khi lọc.
2. `01_ingest_bronze.ipynb` — nạp dữ liệu vào Bronze Delta.
3. `02_transform_silver.ipynb` — ép kiểu, tạo timestamp, duration và quality flags.
4. `03_dim_courier_silver.ipynb` — tạo lịch sử vùng của tài xế.
5. `04_fact_and_ml_gold.ipynb` — tạo fact Gold và kiểm tra point-in-time join.
6. `05_serving_layer.ipynb` — ghi bảng tổng hợp sang MySQL.
7. `06_eta_query_dataset.ipynb` — tạo dataset ETA tại query time.
8. `07_eta_model.ipynb` — đánh giá Median, SPEED và Spark GBT.

Notebook ETA tạo các Delta table:

- `gold/ml/eta_query_samples`: các đơn đang pending tại từng query point.
- `gold/ml/eta_eval_by_cohort`: MAE, RMSE và ACC@30 theo `all`, `le24h`, `gt24h`.
- `gold/ml/eta_gbt_model`: model GBT đã lưu.

Mặc định train dùng query date trước `2022-09-01`, validation dùng tháng 9/2022 và test từ `2022-10-01`. Có thể cấu hình:

```powershell
$env:ETA_TRAIN_END = "2022-09-01"
$env:ETA_VALID_END = "2022-10-01"
$env:ADD_ACCEPT_QUERIES = "true"
```

## Demo nhanh

Kiểm tra dữ liệu và logic ETA local:

```powershell
.venv/Scripts/python.exe scripts/profile_delivery.py
.venv/Scripts/python.exe scripts/check_project.py
.venv/Scripts/python.exe -m pytest -q tests/test_eta_query.py
```

Sau khi chạy notebook 05, mở Metabase tại `http://localhost:3000` và kết nối MySQL bằng:

```text
Host: mysql
Port: 3306
Database: delivery_source
User: delivery
Password: delivery
```

Các bảng chính: `agg_courier_performance`, `agg_region_daily`, `agg_hourly` và `eta_eval_by_cohort`.

## Kiểm tra hệ thống

```powershell
.venv/Scripts/python.exe scripts/check_project.py
docker run --rm --mount type=bind,source=${PWD},target=/workspace,readonly --workdir /workspace --entrypoint /opt/spark/bin/spark-submit apache/spark:3.5.3-python3 /workspace/scripts/test_spark.py
```

## Dừng hệ thống

Giữ nguyên dữ liệu volume:

```powershell
docker compose down
```

Reset hoàn toàn cả MinIO, MySQL, Metabase và Hive:

```powershell
docker compose down -v
```

## Cấu trúc thư mục

- `data/`: dữ liệu nguồn.
- `notebooks/`: pipeline Spark và notebook ETA.
- `scripts/`: profiling, kiểm tra và test.
- `reports/data/`: báo cáo chất lượng dữ liệu.
- `docs/`: data dictionary.
- `docker/`: Dockerfile và cấu hình Hadoop/Hive/Spark.
- `docker-compose.yml`: các dịch vụ hệ thống.

## Tài liệu nguồn

Data card, README, bài báo và snapshot website của LaDe-D được lưu trong `reports/data/sources`. Split dữ liệu sử dụng là `delivery_yt` của LaDe-D Yantai.
