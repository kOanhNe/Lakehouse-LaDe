# Pipeline Prefect + Dashboard

## Cấu trúc thêm vào project
- `pipeline/steps.py`: các bước Spark (Bronze, Silver, SCD2, Gold + ML, serving MySQL)
- `pipeline/flow.py`: flow Prefect nối 5 bước
- `dashboard/`: web dashboard Streamlit đọc từ MySQL
- `docker-compose.yml`, `docker/jupyter.Dockerfile`: bản đã sửa (thêm Prefect server, dashboard)

## Chạy
1. Chép các file vào thư mục gốc project (ghi đè `docker-compose.yml` và `docker/jupyter.Dockerfile`, nên commit git trước).
2. `docker compose up -d --build`
3. `docker compose exec jupyter python /home/jovyan/pipeline/flow.py`
4. Xem tiến trình: http://localhost:4200. Xem dashboard: http://localhost:8501

## Tuỳ chọn
- Chạy theo lịch: `docker compose exec -d jupyter python /home/jovyan/pipeline/flow.py --serve --cron "0 2 * * *"`
- Đổi ngày cắt train/test: `--train-end 2022-09-15`
- Đổi ngưỡng đơn chậm (phút): `--late-threshold 480` (mặc định p90 của tập train)
- Thêm số benchmark của bài báo vào dashboard: đặt biến môi trường `BENCHMARK_MAE`, `BENCHMARK_RMSE`
  (và `BENCHMARK_NAME`) trong mục `environment` của service `jupyter`.
