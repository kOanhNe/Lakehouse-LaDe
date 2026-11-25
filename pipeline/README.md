# Pipeline Prefect + Dashboard

`flow.py` điều phối Bronze, Silver, courier SCD2, star dimensions, ETA-safe,
Protocol P, fact/baseline và serving MySQL. Khi khởi động, flow nạp các cell
`pipeline-step` từ notebook vào bộ nhớ. Mỗi task gọi hàm tương ứng.

Bước ETA tại lúc nhận công bố mô hình B + AOI + GPS với hàng chờ và lịch sử
gần đây trên cửa sổ validation cuối. Dashboard đọc mã mô hình, seed và mốc
chấm trong bảng metric. [Cách dựng mẫu và chọn nhãn](../docs/eta/training_data_lineage.md#35-so-sánh-rolling-origin-trên-cùng-mã-đơn)
Lưu notebook trước khi khởi động flow.
[Notebook chứa thân hàm](../notebooks/README.md#sửa-và-chạy-các-bước-notebook)

## Chạy một lần

Chuẩn bị các gói nén theo [README của dự án](../README.md#6-cài-đặt), rồi chạy từ thư mục gốc.

```powershell
docker compose up -d --build jupyter dashboard
docker compose exec jupyter python -u -m pipeline.flow
```

Xem trạng thái tại http://localhost:4200. Khi flow báo `Completed`, mở dashboard tại http://localhost:8501.

## Tuỳ chọn
- Chạy theo lịch bằng `docker compose exec -d jupyter python -m pipeline.flow --serve --cron "0 2 * * *"`.
- Đặt ngày cắt train/test cho baseline Spark bằng `--train-end 2022-09-15`.
- Đặt ngưỡng đơn chậm (phút) bằng `--late-threshold 480`. Mặc định dùng p90 của tập train.
