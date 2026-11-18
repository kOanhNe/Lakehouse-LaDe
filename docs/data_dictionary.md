# Data dictionary và thời điểm biết của cột

Nguồn chuẩn là [`config/schema.yaml`](../config/schema.yaml). File này dùng để
kiểm tra cột chưa khai báo, xác định nhãn và tạo allowlist cho feature builder.

| Cột | Tầng | Biết tại thời điểm | Vai trò |
|---|---|---|---|
| `order_id`, `courier_id`, `region_id`, `aoi_id`, `aoi_type` | Bronze/Silver | Khi tạo đơn | Feature được phép |
| `lng`, `lat` | Bronze/Silver | Khi tạo đơn, theo giả định data contract | Tọa độ điểm giao candidate |
| `accept_ts`, `accept_gps_*` | Silver | Khi nhận đơn | Feature được phép |
| `delivery_ts` | Silver | Khi giao xong | Mốc phân loại completed/pending |
| `delivery_gps_*` | Silver | Khi giao xong | Chỉ dùng với completed có `delivery_ts <= query_ts` |
| `delivery_minutes` | Silver | Sau khi giao xong | Label, không vào feature matrix |
| `target_remaining_minutes` | Gold | Sau khi giao xong | Label ETA, không vào feature builder |
| `ds`, `ds_date` | Bronze/Silver | Có trong source hoặc dựng khi ingest | Partition/audit |
| `run_id`, `source_file` | Bronze/Silver/Gold | Khi chạy pipeline | Metadata lineage |

Quy tắc theo vai trò:

- `completed`: được dùng `delivery_ts` và `delivery_gps_*` nếu giao trước hoặc tại query time.
- `pending`: giữ thuộc tính đã biết khi nhận đơn; che trường chỉ xuất hiện sau giao.
- `candidate`: chỉ dùng dữ liệu đã biết tại query time; không đọc `delivery_gps_*`.
- `label`: truyền qua kênh riêng và không được đưa vào hàm tạo feature.

Nếu thêm cột mới vào bất kỳ tầng nào, phải cập nhật `schema.yaml`, ghi status
(`allowed`, `forbidden`, `label` hoặc `ignored`) và chạy `scripts/validate_schema.py`.
