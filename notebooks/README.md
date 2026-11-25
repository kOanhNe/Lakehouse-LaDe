# Notebook từ dữ liệu gốc đến ETA

**Để giải thích một dự đoán**, mở notebook 06. Notebook đọc file Yantai, kiểm tra checksum, cho thấy số đơn sau từng bước và lần theo các lá cây LightGBM để đối chiếu với `predict()`. Khi chạy ngoài Docker, dùng kernel từ môi trường đã cài repo với `pip install -e .`. [Đối chiếu tính tay](06_eta_accept.ipynb)

## File đầu vào và thứ tự xem

| Notebook theo thứ tự trình bày | Đọc từ | Xem được |
| --- | --- | --- |
| [00 · Kiểm kê dữ liệu](00_data_audit.ipynb) | `data/delivery_yt.csv` | Header, mẫu dòng gốc và báo cáo chất lượng. Bước này phục vụ audit. |
| [01 · Bronze](01_ingest_bronze.ipynb) | CSV ở MinIO hoặc bản file trong Jupyter | Đường dẫn thực tế được chọn, kiểm tra khóa và các cột gốc trong Delta. |
| [02 · Silver và Rejected](02_transform_silver.ipynb) | `delivery_bronze` | Quy tắc tách hai bảng, phép tính thời lượng và lý do từ chối. |
| [03 · Năm dimension](03_dim_courier_silver.ipynb) | `delivery_silver` | Lịch sử shipper SCD2 rồi đến vùng, AOI, ngày và giờ. |
| [04 · Fact và Spark LR/GBT](04_fact.ipynb) | Silver và năm dimension | Ghép fact, lọc cohort GPS/24 giờ/50 km và điểm riêng của baseline Spark. |
| [05 · Serving](05_serving.ipynb) | Fact, dự đoán Spark, ngưỡng đơn trễ và dimension | Bảng MySQL và số dòng đã đối soát. |
| [06 · ETA lúc nhận](06_eta_accept.ipynb) | CSV để trình bày. Bronze khi chạy B đăng ký qua Prefect. | Mẫu theo thời điểm nhận, chia tập, dự đoán tính tay và điểm validation B + AOI + GPS. |
| [07 · ETA còn lại](07_eta_paper.ipynb) | Bảy cột của Bronze | Làm sạch quỹ đạo, cohort 1–120 phút và kết quả tái lập Protocol P. |

Prefect chạy Bronze → Silver → dimension → Protocol B → Protocol P → fact và baseline Spark → serving. [Flow](../pipeline/flow.py)

File CSV của repo nằm ở `data/delivery_yt.csv`, được mount tại `/home/jovyan/work/data/delivery_yt.csv` trong Docker. Bronze ưu tiên `s3a://lakehouse/delivery_yt.csv`. Khi file đó thiếu, nó dùng bản đã mount. Notebook 01 in đường dẫn thực tế đã đọc.

## Mở và chạy

Dùng [hướng dẫn bật Docker](../README.md#6-cài-đặt) cho các notebook ghi Delta hoặc MySQL. Lấy URL Jupyter rồi chạy các cell từ trên xuống.

~~~powershell
docker compose exec jupyter jupyter server list
~~~

Mỗi notebook có đầu vào, thân hàm xử lý và cell gọi hàm để xem kết quả. Mở notebook tương ứng để đọc cách lọc Silver, tạo đặc trưng, huấn luyện hoặc ghi MySQL. [Thứ tự task](../pipeline/flow.py)

## Sửa và chạy các bước notebook

Sửa code trong cell có tag `pipeline-step` rồi lưu notebook. Khi khởi động flow, Prefect nạp các cell này vào bộ nhớ và gọi hàm theo thứ tự. Cell xem dữ liệu, tính tay và bấm chạy thử dành cho người mở notebook. `python -m scripts.checks.check_project` kiểm tra cú pháp của các cell định nghĩa.

Chạy các cell ghi bảng khi Prefect đã dừng. Để công bố một lượt kết quả đầy đủ lên dashboard, chạy flow.

~~~powershell
docker compose exec jupyter python -u -m pipeline.flow
~~~
