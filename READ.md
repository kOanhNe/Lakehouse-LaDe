# Delivery Lakehouse - LaDe-D Yantai

Pipeline end-to-end: nạp dữ liệu giao hàng Yantai (LaDe-D) vào Delta Lake, làm sạch, dựng lịch sử shipper (SCD2), dự đoán thời gian giao hàng (ETA), đưa bảng tổng hợp lên MySQL và hiển thị trên dashboard. Pipeline được điều phối bằng Prefect.

```
CSV -> Bronze -> Silver -> dim_courier (SCD2) -> Gold (fact + ML) -> MySQL -> Dashboard
        |__________________ Prefect flow (5 bước) __________________|
```

## Về dữ liệu

- Nguồn: `delivery_yt` trong LaDe-D, 206,431 đơn, 1 thành phố (Yantai), 205 shipper, 17 khu vực, 8,163 AOI.
- `region_id` là mã số khu vực do bộ dữ liệu ẩn danh, không có tên quận hoặc huyện.
- `delivery_minutes` là thời gian từ lúc nhận đơn đến lúc hoàn thành, không phải thời gian lái xe.
- Silver giữ các đơn có thời gian dương và gắn cờ chất lượng (đơn qua ngày, trên 24 giờ, GPS không hợp lệ, khoảng cách quá lớn). Chỉ phần ML mới loại các dòng bị gắn cờ.
- Khu vực 146 có khoảng 67% đơn mất hơn 24 giờ, vận hành khác hẳn các khu vực còn lại. Dashboard có cảnh báo cho các khu vực như vậy.

Đọc thêm: [báo cáo kiểm tra dữ liệu](reports/data/README.md), [từ điển dữ liệu](docs/data_dictionary.md), [thứ tự notebook](notebooks/README.md).

## Yêu cầu

- Docker Desktop đang chạy, Docker Compose v2.
- Khoảng 10 GB trống ổ đĩa và nên cấp cho Docker ít nhất 6 GB RAM. Spark hết RAM sẽ báo lỗi `JAVA_GATEWAY_EXITED`.
- Cổng trống: 3000, 3306, 4040, 4200, 8501, 8888, 9000, 9001.
- Có mạng ở lần chạy đầu (tải image, jar và thư viện).

## Cài đặt lần đầu

**1. Clone project**

```bash
git clone https://github.com/kOanhNe/Lakehouse-LaDe.git
cd Lakehouse-LaDe
```

**2. Tải các file lớn không nằm trong git**

```bash
curl -L -o docker/jars/hadoop-aws-3.3.4.jar \
  https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -L -o docker/jars/aws-java-sdk-bundle-1.12.262.jar \
  https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
```

**3. Dựng các dịch vụ**

```bash
docker compose up -d --build
docker compose ps
```

Lần đầu mất vài phút (build image và tải thư viện). Đợi đến khi `prefect`, `mysql`, `minio`, `lakehouse` ở trạng thái `healthy` và `jupyter` ở trạng thái `running`. `minio-init` ở trạng thái `Exited` là bình thường, nó chỉ tạo bucket rồi thoát.

## Chạy pipeline end-to-end (Prefect)

```bash
docker compose exec jupyter python /home/jovyan/pipeline/flow.py
```

Lần chạy đầu Spark tải thêm jar nên chậm hơn (vài phút), các lần sau nhanh hơn. Pipeline tự đọc `data/delivery_yt.csv` nếu MinIO chưa có file, nên không cần upload tay.

Pipeline gồm 5 bước, mỗi bước retry 1 lần nếu lỗi:

| Bước | Việc làm | Kết quả |
|---|---|---|
| 1. Bronze | Đọc CSV, kiểm tra trùng `order_id` | `delivery_bronze` |
| 2. Silver | Ép kiểu, tính thời gian xử lý, khoảng cách, cờ chất lượng | `delivery_silver`, `delivery_rejected` |
| 3. SCD2 | Lịch sử khu vực của từng shipper | `dim_courier_silver` |
| 4. Gold + ML | Bảng fact, huấn luyện Linear Regression và GBT, so với baseline | `fact_delivery_task_gold`, `eta_predictions_gold` |
| 5. Serving | Đẩy các bảng tổng hợp sang MySQL | các bảng MySQL bên dưới |

Theo dõi tiến trình từng bước tại http://localhost:4200. Xem dashboard tại http://localhost:8501 (nếu thấy cảnh báo chưa có dữ liệu thì đợi pipeline chạy xong rồi tải lại trang).

**Tuỳ chọn** (thêm vào cuối lệnh trên, không bắt buộc):

- `--train-end 2022-09-15`: ngày cắt giữa tập huấn luyện và tập kiểm tra (mặc định 2022-10-01).
- `--late-threshold 480`: ngưỡng đơn chậm, tính bằng phút (mặc định là p90 của tập huấn luyện).
- `--serve --cron "0 2 * * *"`: chạy nền, tự chạy lại theo lịch. Dùng `docker compose exec -d jupyter ...` để không giữ terminal.
- Đưa số benchmark của bài báo gốc vào dashboard: đặt `BENCHMARK_MAE`, `BENCHMARK_RMSE` (và `BENCHMARK_NAME`) trong mục `environment` của service `jupyter`.

## Dashboard

Dashboard Streamlit đọc từ MySQL, có 4 tab:

- **Tổng quan**: số đơn, thời gian xử lý trung vị, tỷ lệ đơn chậm, đơn mất hơn 24 giờ, xu hướng theo ngày, phân bố thời gian xử lý.
- **Shipper**: xếp hạng theo tỷ lệ chậm, thời gian xử lý hoặc số đơn, có lọc theo khu vực và số đơn tối thiểu.
- **Khu vực & giờ**: so sánh các khu vực (kèm cảnh báo khu vực bất thường) và thời gian xử lý theo giờ nhận đơn.
- **Mô hình ETA**: MAE, RMSE, R² của baseline, Linear Regression, GBT; mức quan trọng của đặc trưng; thực tế so với dự đoán.

Thư mục `dashboard` được mount vào container, nên khi sửa `dashboard/app.py` chỉ cần tải lại trang (hoặc bấm "Always rerun" ở góc trên bên phải trang) mà không cần build lại.

Metabase (http://localhost:3000) vẫn đọc được các bảng MySQL nếu muốn dựng thêm biểu đồ.

## Bảng MySQL do pipeline tạo

Database `delivery_source` (user `delivery`, mật khẩu `delivery`):

`agg_courier_performance`, `agg_region_daily`, `agg_hourly`, `agg_region_summary`, `agg_overview`, `agg_region_error`, `ml_metrics`, `ml_feature_importance`, `eta_prediction_sample`, `pipeline_run_info`.

## Chạy bằng notebook (cách thay thế)

Nếu muốn chạy từng bước thủ công thay vì Prefect:

1. Mở http://localhost:9001 (user và mật khẩu `minioadmin`), kiểm tra bucket `lakehouse` đã có, upload `data/delivery_yt.csv` vào bucket để có đường dẫn `s3a://lakehouse/delivery_yt.csv`.
2. Mở Jupyter tại http://localhost:8888 và chạy notebook `00` đến `05` theo thứ tự trong [notebooks/README.md](notebooks/README.md). Nếu Jupyter hỏi token, xem bằng `docker compose logs jupyter`.

## Các dịch vụ

| Dịch vụ | Cổng | Vai trò |
|---|---|---|
| MinIO | 9000 (API), 9001 (console) | Lưu file nguồn và dữ liệu Delta |
| Jupyter | 8888, 4040 (Spark UI) | Chạy notebook và pipeline Spark |
| Prefect | 4200 | Điều phối và theo dõi pipeline |
| MySQL | 3306 | Lưu bảng tổng hợp |
| Dashboard | 8501 | Web dashboard Streamlit |
| Metabase | 3000 | Dashboard BI (tuỳ chọn) |
| Lakehouse (Hive metastore, Spark Thrift) | 9083, 10002, 4041 | Hạ tầng Hive/Spark |

## Cấu trúc thư mục

```
data/            dữ liệu nguồn (delivery_yt.csv)
docker/          jupyter.Dockerfile, cấu hình Hadoop/Hive/Spark, jars/
pipeline/        flow.py (Prefect), steps.py (các bước Spark)
dashboard/       app.py (Streamlit), Dockerfile, requirements.txt
notebooks/       notebook 00-05 và lakehouse_common.py
reports/         báo cáo kiểm tra dữ liệu
scripts/         download_deps.sh, profile_delivery.py, kiểm thử
```

## Kiểm tra dữ liệu cục bộ và kiểm thử (không bắt buộc)

Cần môi trường `.venv` có Python và thư viện đã cài.

```bash
.venv/bin/python scripts/profile_delivery.py
.venv/bin/python scripts/check_project.py
docker run --rm --mount type=bind,source="$PWD",target=/workspace,readonly --workdir /workspace --entrypoint /opt/spark/bin/spark-submit apache/spark:3.5.3-python3 /workspace/scripts/test_spark.py
```

Trên Windows (PowerShell) dùng `.venv/Scripts/python.exe` và `${PWD}` thay cho `"$PWD"`.

## Tắt và reset

```bash
docker compose down        # tắt, giữ dữ liệu
docker compose down -v     # tắt và xoá toàn bộ volume (làm lại từ đầu)
```

## Xử lý lỗi

**`prefect` báo unhealthy.** Xem `docker logs prefect --tail 40`. Nếu có `permission denied` hoặc `unable to open database file`, bỏ volume `prefect_data` khỏi service `prefect` (và khỏi mục `volumes:` cuối file), rồi `docker volume rm lakehouse-lade_prefect_data` và `docker compose up -d`.

**`No module named 'pyspark'` khi chạy pipeline.** Service `jupyter` trong `docker-compose.yml` cần dòng `PYTHONPATH: /usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip` trong `environment`. Thêm xong chạy `docker compose up -d jupyter`.

**`can't open file '/home/jovyan/pipeline/flow.py'`.** Thư mục `pipeline/` đang rỗng hoặc nằm sai chỗ. Nó phải nằm ở thư mục gốc project, cạnh `notebooks/`. Kiểm tra bằng `ls pipeline` và `docker compose exec jupyter ls /home/jovyan/pipeline`.

**Dashboard vẫn hiện bản cũ.** Chạy `docker compose up -d --build dashboard` rồi tải lại trang bằng Cmd/Ctrl + Shift + R.

**Spark báo `JAVA_GATEWAY_EXITED`.** Thường do thiếu RAM. Tăng RAM cho Docker Desktop, hoặc kiểm tra các file jar trong `docker/jars` đã tải đủ.

**Build `lakehouse` báo không thấy file `.tar.gz` hoặc `.tgz`.** Chưa chạy `sh scripts/download_deps.sh`.

**Image MinIO báo sai nền tảng trên Mac chip Apple Silicon.** Hai image MinIO bản sao chỉ có amd64. Docker chạy qua giả lập và có thể in cảnh báo. Nếu bị lỗi, thêm dòng `platform: linux/amd64` dưới dòng `image:` của `minio` và `minio-init`.

**Cổng bị chiếm.** Tắt chương trình đang dùng cổng đó, hoặc đổi cổng bên trái trong phần `ports:` của `docker-compose.yml`.

## Nguồn

Data card, README, bài báo và bản chụp website chính thức của LaDe-D nằm trong `reports/data/sources`.