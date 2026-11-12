# Báo cáo hiểu dữ liệu và tiền xử lý

Báo cáo này được sinh từ `scripts/profile_delivery.py` trên toàn bộ file `data/delivery_yt.csv`. File nguồn có SHA-256 `aa5bca7fdaa8c89d4ab50a2ed2b94fb09f608acc49fc1108a1a612e9c0f5a7ad`, gồm 206.431 dòng và 17 cột. Metadata chính thức của LaDe-D cũng công bố split `delivery_yt` có 206.431 dòng; các giá trị của 17 cột đã được đối chiếu theo `order_id` với parquet chính thức, không phát hiện khác biệt.

## Nguồn và grain

Đây là tập LaDe-D, ngữ cảnh giao hàng, thành phố Yantai. Grain của file là một dòng cho một `order_id`; file có 206.431 `order_id` duy nhất và không có dòng trùng hoàn toàn. Dữ liệu quan sát từ 2022-05-01 đến 2022-10-31 ở thời điểm nhận đơn. `delivery_time` có thể kéo dài đến 2022-11-05 vì đơn được hoàn tất sau ngày nhận.

Theo data card LaDe-D:

| Nhóm | Cột | Ý nghĩa |
|---|---|---|
| Định danh | `order_id` | ID đơn/package |
| Địa điểm đích | `lng`, `lat` | Tọa độ điểm giao/điểm dừng |
| Phân vùng | `city`, `region_id`, `aoi_id`, `aoi_type` | Thành phố, vùng, khu vực AOI và mã loại AOI |
| Tài xế | `courier_id` | ID tài xế |
| Sự kiện | `accept_time`, `delivery_time` | Thời điểm tài xế nhận việc và hoàn tất giao |
| GPS sự kiện | `accept_gps_time`, `accept_gps_lng`, `accept_gps_lat` | Điểm GPS gần thời điểm nhận việc nhất |
| GPS sự kiện | `delivery_gps_time`, `delivery_gps_lng`, `delivery_gps_lat` | Điểm GPS gần thời điểm hoàn tất nhất |
| Ngày | `ds` | Ngày giao hàng trong schema LaDe-D; trong file này khớp ngày nhận đơn |

Các timestamp không có năm và không có giây khác 0. Pipeline tái tạo năm 2022 vì đây là phạm vi của split Yantai; đây là một assumption được ghi rõ, không phải thông tin có sẵn trong chuỗi gốc.

## Kết quả profiling

* Một thành phố: Yantai; 205 tài xế; 17 vùng; 8.163 AOI; 16 mã `aoi_type`.
* Không thiếu ID nghiệp vụ, timestamp, tọa độ điểm dừng hoặc GPS giao hàng.
* 2.422 dòng thiếu GPS nhận, 2 dòng GPS nhận bằng `(0,0)`, tổng cộng 2.424 dòng GPS nhận không dùng được.
* 369 dòng có thời lượng bằng 0; không có thời lượng âm.
* 2.987 dòng giao khác ngày; 2.650 dòng vượt 24 giờ. Đây là quan sát cần phân tích, không tự động chứng minh là lỗi.
* `ds` khớp `accept_time` ở 206.431/206.431 dòng và khớp `delivery_time` ở 203.444/206.431 dòng. Vì vậy không được dùng `ds` như ngày hoàn tất cho mọi đơn.
* `accept_gps_time` và `delivery_gps_time` bằng timestamp sự kiện ở toàn bộ file. Điều này cho biết record được ghép với điểm GPS gần nhất; không đủ để kết luận tọa độ GPS là tín hiệu có sẵn trước thời điểm dự báo. Pipeline gắn `accept_distance_available` theo quy tắc GPS không đến sau thời điểm nhận.
* 157 courier-day có hơn một `region_id`; 12.364 courier-day có hơn một AOI. Không được coi tài xế chỉ thuộc một vùng/AOI duy nhất trong cả ngày.
* 350 AOI xuất hiện ở hơn một region và 6.057 AOI có hơn một cặp tọa độ quan sát. `dim_aoi` vì thế là tâm quan sát phục vụ mô tả, không phải ground truth hình học.
* Có 160.153 dòng nằm trong các nhóm nhận cùng một timestamp; timestamp phút không biểu diễn thứ tự chính xác trong batch. Mọi thứ tự theo thời gian phải thêm `order_id` để deterministic và không được diễn giải là thứ tự tài xế thực tế.
* Tập train trước 2022-10-01 có 169.736 dòng và 190 tài xế; test có 36.695 dòng và 137 tài xế. Test có 1.851 tài xế và 465 AOI chưa từng xuất hiện ở train, nên kết quả mô hình phải báo rõ cold-start.

## Kết luận về biến dẫn xuất

`delivery_minutes` là khoảng thời gian từ nhận việc đến hoàn tất, không phải thời gian lái xe. Nó có thể bao gồm chờ, xử lý và các đơn khác trên tuyến. `distance_km` là khoảng cách đường chim bay giữa điểm dừng và GPS nhận; không gọi là quãng đường thực tế.

`accept_distance_available` chỉ đúng khi cả hai tọa độ hợp lệ và GPS nhận không có timestamp sau `accept_time`. `delivery_stop_distance_km` là khoảng cách kiểm tra giữa điểm dừng và GPS hoàn tất. Hai biến này phục vụ kiểm tra chất lượng và đặc trưng, không được gọi là route distance.

## Chính sách Silver và Gold

Silver mới giữ toàn bộ 206.431 dòng có thời lượng dương, đồng thời lưu các cờ `q_*` và bảng `delivery_rejected` cho 369 dòng thời lượng không dương. Các dòng >24 giờ, GPS nhận thiếu, khoảng cách lớn hoặc giao qua ngày vẫn ở Silver với cờ; việc chọn cohort là trách nhiệm của từng bài toán.

Gold/ML phải khai báo cohort ngay trong notebook. Cohort mặc định cho label là `delivery_minutes > 0`; cohort có GPS nhận hợp lệ dùng `eligible_ml_distance`. Không dùng ngưỡng 24 giờ hoặc 50 km để âm thầm xóa dữ liệu. Nếu báo cáo cần cohort <=24 giờ, phải ghi số dòng, lý do nghiệp vụ và so sánh độ nhạy với cohort đầy đủ.

Không suy ra tên cho 16 mã `aoi_type` vì data card chỉ mô tả đây là biến categorical, không cung cấp mapping. Dimension giữ mã gốc và tên hiển thị dạng `code_<n>`.

## Các file kết quả

* `summary.json`: hash, kích thước, phạm vi thời gian, cờ chất lượng và quan hệ khóa.
* `tables/column_profile.csv`: kiểu quan sát, missing, cardinality và ví dụ.
* `tables/quantiles.csv`: phân vị của duration và các khoảng cách.
* `tables/cohort_sensitivity.csv`: ảnh hưởng của từng cohort lên phân phối và tương quan.
* `tables/anomaly_examples.csv`: mẫu dòng cho từng loại cờ.
* `tables/courier_day.csv`, `aoi_relations.csv`, `region_relations.csv`: kiểm tra quan hệ thực thể.
* `row_quality.csv.gz`: audit theo từng dòng, có lý do cờ và trạng thái đủ điều kiện.
* `overview.png`: biểu đồ tổng quan.

Chạy lại bằng:

```powershell
.venv/Scripts/python.exe scripts/profile_delivery.py
```

Nguồn tham khảo đã lưu tại `reports/data/sources/`: data card LaDe-D, README repository, website và paper.
