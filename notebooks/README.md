# Hướng dẫn chạy notebook

Các notebook gọi lại đúng các hàm trong pipeline/steps.py. Chạy notebook và chạy Prefect sẽ dùng cùng logic.

## Chuẩn bị

Từ thư mục gốc dự án:

~~~powershell
docker compose up -d --build
docker compose ps
docker compose exec jupyter jupyter server list
~~~

Mở URL có dạng http://127.0.0.1:8888/lab?token=... trên trình duyệt. Trong mỗi notebook, chạy cell từ trên xuống bằng Shift + Enter.

## Thứ tự chạy

1. 00_data_audit.ipynb: audit và EDA toàn bộ CSV.
2. 01_ingest_bronze.ipynb: ghi delivery_bronze.
3. 02_transform_silver.ipynb: ghi delivery_silver và delivery_rejected.
4. 03_dim_courier_silver.ipynb: tạo courier SCD2.
5. 03b_star_dimensions.ipynb: tạo dimension region, AOI, date, hour.
6. 04_fact_and_ml_gold.ipynb: tạo fact Gold và baseline cũ.
7. 04b_eta_safe.ipynb: chạy bài toán phụ ETA-safe.
8. 04c_eta_paper.ipynb: chạy Protocol P, bài toán ETA chính.
9. 05_serving_layer.ipynb: ghi bảng serving sang MySQL.

Chờ cell của notebook trước hoàn tất trước khi chạy notebook sau. Không chạy hai notebook ghi cùng một bảng cùng lúc.

## Điều kiện và lỗi

- 00 cần file data/delivery_yt.csv.
- 01 cần MinIO và Lakehouse ở trạng thái healthy.
- 02 trở đi cần bảng upstream đã tạo.
- 04c có thể mất vài phút vì tạo mẫu và huấn luyện LightGBM.
- PATH_NOT_FOUND nghĩa là bảng upstream chưa có hoặc notebook chạy sai thứ tự.
- Sau notebook 05, mở dashboard tại http://localhost:8501.

Nếu muốn chạy toàn bộ một lần:

~~~powershell
docker compose exec jupyter python -u /home/jovyan/pipeline/flow.py
~~~
