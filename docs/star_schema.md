# Star schema

Fact trung tâm `fact_delivery_task_gold` nối với **5 dimension** bằng khóa thay thế (surrogate key):

| Dimension | Khóa | Thuộc tính | Nối với fact bằng | Ghi chú |
|---|---|---|---|---|
| `dim_courier_silver` (SCD2) | `courier_sk` | `courier_id`, `region_id` (khu chính), `effective_date`, `end_date`, `is_active` | `courier_sk` (point-in-time join theo ngày nhận) | Lưu lịch sử đổi khu của shipper |
| `dim_region_gold` | `region_sk` | `region_id`, tâm (`centroid_lat/lng`), hộp bao (`min/max_lat/lng`), `distinct_aoi_observed` | `region_sk` | 17 dòng. Thuộc tính suy ra từ tọa độ đơn, dữ liệu không có tên khu |
| `dim_aoi_gold` | `aoi_sk` | `aoi_id`, `aoi_type`, `centroid_lat/lng` | `aoi_sk` | Mỗi `aoi_id` chỉ có 1 `aoi_type` (đã kiểm tra). 8.152 AOI xuất hiện trong Silver |
| `dim_date_gold` | `date_key` (yyyymmdd) | ngày, năm, tháng, tuần, thứ, tên thứ, cuối tuần | `accept_date_key`, `delivery_date_key` | Role-playing: một dimension, hai vai (ngày nhận, ngày giao). Phủ từ ngày đầu đến ngày cuối của dữ liệu |
| `dim_hour_gold` | `hour_key` (0-23) | buổi trong ngày (Sáng/Trưa/Chiều/Tối/Đêm) | `accept_hour_key` | 24 dòng |

`region_id`, `aoi_id`, `courier_id` vẫn được giữ trong fact làm khóa nghiệp vụ để truy vết về Bronze.

## Vì sao region không nằm trong dim_aoi
Dữ liệu cho thấy 350 AOI xuất hiện ở hơn một region (quan hệ không phân cấp). Nếu nhét `region_id` vào `dim_aoi` thì một AOI sẽ bị gán sai khu. Vì vậy `region_sk` là khóa ngoại riêng của fact.

## Fact trung tâm
`fact_delivery_task_gold` có grain một dòng cho một đơn giao, khóa nghiệp vụ `order_id`. Khi dựng, pipeline kiểm tra: số dòng không đổi sau khi gắn khóa, không khóa nào rỗng, mọi khóa ngoại đều tồn tại trong dimension tương ứng (`attach_dimension_keys`). `delivery_minutes` là nhãn của bài toán phụ.

## Các fact khác (fact constellation)
- `eta_safe_gold`: grain một đơn tại một thời điểm truy vấn, khóa `order_id + query_ts`.
- `eta_paper_gold` (Protocol P): mỗi dòng là một cặp truy vấn và đơn còn cầm trong một cửa sổ. `query_id` gom các dòng của cùng truy vấn và có thể lặp qua các cửa sổ chồng lấn.
- `eta_safe_metrics_gold`, `eta_paper_metrics_gold`: kết quả đo theo lần chạy, mô hình, seed.

## Bảng chất lượng
`delivery_rejected` giữ các dòng bị loại ở Silver cùng `reject_reason`. Phục vụ audit, không tham gia fact.

## Ghi chú trung thực
- Dữ liệu chỉ có mã (`region_id`, `aoi_type`), không có tên khu vực hay tên loại AOI. Vì vậy thuộc tính của `dim_region` và `dim_aoi` là thuộc tính tính từ tọa độ, không phải thuộc tính do mình đặt ra.
- `dim_aoi` có 8.152 dòng, ít hơn 8.163 AOI của file gốc: 11 AOI chỉ xuất hiện trong các dòng bị loại sang `delivery_rejected`.
