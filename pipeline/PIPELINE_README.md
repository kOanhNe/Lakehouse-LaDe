# Pipeline Prefect + Dashboard

`flow.py` điều phối Bronze, Silver, courier SCD2, star dimensions, ETA-safe,
Protocol P, fact/baseline và serving MySQL. Mỗi task gọi một bước trong
`steps.py`, nơi notebook cũng lấy hàm để trình bày cùng quy tắc xử lý.
[Đầu vào, bước lọc và kết quả theo notebook](../notebooks/README.md)

## Chạy một lần

Chuẩn bị các gói nén theo [README của dự án](../readme.md#6-cài-đặt), rồi chạy từ thư mục gốc.

```powershell
docker compose up -d --build jupyter dashboard
docker compose exec jupyter python -u /home/jovyan/pipeline/flow.py
```

Xem trạng thái tại http://localhost:4200. Khi flow báo `Completed`, mở dashboard tại http://localhost:8501.

## Tuỳ chọn
- Chạy theo lịch bằng `docker compose exec -d jupyter python /home/jovyan/pipeline/flow.py --serve --cron "0 2 * * *"`.
- Đặt ngày cắt train/test cho baseline Spark bằng `--train-end 2022-09-15`.
- Đặt ngưỡng đơn chậm (phút) bằng `--late-threshold 480`. Mặc định dùng p90 của tập train.
