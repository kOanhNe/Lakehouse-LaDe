# Trạng thái triển khai

Đợt ổn định này dựa trên commit `488db2e`. Prefect chạy tám bước
từ Bronze đến MySQL. Prefect nạp thân hàm từ các cell `pipeline-step` của notebook khi khởi động. [Notebook nguồn](../notebooks/README.md#sửa-và-chạy-các-bước-notebook)

## Hợp đồng dữ liệu

| Tầng | Kiểm tra |
|---|---|
| Bronze | Header LaDe-D đúng thứ tự, khóa `order_id` có giá trị và duy nhất. Sau khi ghi, đối soát khóa với CSV đã đọc. |
| Silver và Rejected | Đọc lại Delta để đối soát từng khóa với Bronze. Mỗi khóa phải xuất hiện đúng một lần trong hợp của hai bảng. Kiểm tra timestamp, thời lượng và lý do từ chối. |
| Courier SCD2 và star schema | Ghép lịch sử theo ngày nhận trong khoảng hiệu lực đầu đóng, cuối mở. Kiểm tra grain và khóa ngoại region, AOI, ngày nhận, ngày giao và giờ. |
| Fact và serving | Đối soát khóa giữa Silver và fact đã ghi. So sánh số dòng DataFrame với số dòng đọc lại từ MySQL và xác nhận mã lần chạy. |

Silver giữ các dòng có timestamp hợp lệ và thời lượng dương. Giá trị quá 24 giờ
hay khoảng cách lớn hơn 50 km được gắn cờ và dùng để xác định cohort mô hình Spark.
Với CSV Yantai có SHA-256
`aa5bca7fdaa8c89d4ab50a2ed2b94fb09f608acc49fc1108a1a612e9c0f5a7ad`,
các số tham chiếu là 206.431 Bronze, 206.062 Silver và 369 Rejected. Pipeline
tính số đối soát từ dữ liệu đã đọc và dùng khóa thực tế để kiểm tra.

## Công bố lần chạy và khôi phục

`lakehouse_pipeline_runs` lưu từng lần thử cùng ID Prefect, bước, trạng thái và
thời gian UTC. Khóa MySQL ở một kết nối riêng giữ quyền ghi trong toàn bộ flow.
Một flow thứ hai nhận lỗi nếu khóa đang được giữ. Flow báo `COMPLETED` sau khi
serving ghi xong và đọc lại thành công. Khi một bước hết lượt retry, nhật ký ghi
`FAILED`. Khi kết nối giữ khóa biến mất, dashboard nhận diện lần `RUNNING` là
`INTERRUPTED`. Lần chạy kế tiếp ghi trạng thái gián đoạn vào nhật ký.

Dashboard kiểm tra trạng thái trước và sau khi đọc các bảng. Biểu đồ chỉ xuất hiện
khi lần mới nhất đã hoàn tất và mọi dòng trong các bảng dashboard cùng mang mã
lần chạy đó. Nếu thất bại, đọc log theo ID Prefect và chạy lại toàn bộ flow.

Các bảng vẫn ghi đè riêng lẻ. Truy vấn trực tiếp Delta, MySQL hoặc Metabase có thể
đọc dữ liệu ghi dở. Các notebook chạy riêng cần tránh thời gian Prefect đang ghi
và phải chạy lại flow để công bố dashboard. Trang dashboard đã mở kiểm tra lại
trạng thái khi bấm làm mới.

## Kiểm thử

Lệnh và yêu cầu môi trường nằm trong [README](../README.md#10-kiểm-tra-và-test).
Chạy marker `not integration` trước. Bộ unit kiểm tra hợp đồng nguồn, phân tách
Silver/Rejected, SCD2, các khóa dimension, fact, tổng hợp serving, trạng thái
dashboard và các phụ thuộc tương lai của Protocol P.

Kiểm thử của benchmark Protocol B cắt CSV Yantai tại 15/08/2022 và ẩn các lần
giao sau mốc cắt. AOI, GPS, hàng chờ và lịch sử của các đơn nhận trước mốc
khớp với giá trị từ toàn bộ file. Phép tính trực tiếp trên 400 đơn cũng khớp
14 trường trạng thái và lịch sử đã kiểm tra.

Bộ integration cần Spark/Delta, MinIO, MySQL và API Prefect. Nó chạy flow với
CSV mẫu và namespace riêng, tạo lỗi sau khi một bảng serving đã được ghi, xác
nhận lần lỗi bị ẩn và chạy lại để đối chiếu grain, khóa và mã xuất dữ liệu.
Một kiểm thử khác dùng kết nối MySQL thật để kiểm tra tranh chấp khóa, nhận biết
mất kết nối và ghi nhận lần chạy bị gián đoạn.

Kết quả kiểm thử của bản ETA lúc nhận được ghi dưới đây.

| Kiểm tra | Kết quả |
|---|---|
| Unit trong container Jupyter, `pytest -m "not integration" -q` | 43 đạt, gồm 6 kiểm thử Spark. |
| Notebook 06 với CSV Yantai | Bảy giá trị tính tay khớp bảng mẫu. Tổng lá cây khớp `predict()`. Điểm của cửa sổ cuối khớp benchmark theo seed. |
| Bước ETA với Spark/Delta | Ghi và đọc được dự đoán, metric validation và mã lần chạy từ Delta cục bộ. Hàm ghi MySQL dùng bộ thay thế để kiểm tra bảng và dòng nhận được. |
| Integration toàn flow với Prefect, MinIO và MySQL | Chưa chạy lại trên nhánh ETA này. Lượt sau khi chuyển đường dẫn từng dừng do ổ đĩa máy kiểm thử hết chỗ. |
| Cấu hình và nguồn notebook | Compose, `config/schema.yaml` và các cell định nghĩa hợp lệ. |

Ở đợt ổn định trước khi chuyển đường dẫn, hai kiểm thử integration đã đạt. Đối
soát chỉ đọc khi đó xác nhận SHA-256 của CSV trên MinIO khớp bản đăng ký. Delta
có 206.431 Bronze, 206.062 Silver, 369 Rejected và 206.062 fact. Tổng đơn
serving MySQL khớp Silver.

## Kết quả ETA

Kết quả Protocol P đã lưu dùng thiết kế tái lập LaDe.
[Audit leakage](eta/leak_inventory.md#audit-protocol-p) ghi rõ các phụ thuộc của
`spd`, `ntodo`, `rank`, quỹ đạo đã làm sạch và cách chọn cửa sổ. Hai phép thử
phản thực tế kiểm tra sự phụ thuộc của đặc trưng. Metric hiện tại mô tả kết quả
tái lập ngoại tuyến trên cohort của Protocol P.

Prefect dùng `protocol_b_queue_history` cho ETA lúc nhận. Mô hình lấy các sự kiện
trước truy vấn, fit bằng nhãn có trước cửa sổ hiệu chỉnh và công bố validation
cuối. Kết quả ba cửa sổ và ba seed cho MAE 104,16 phút và ACC@30 31,88%.
Đối chứng Spark LR/GBT đã chạy trên cùng các đơn, mốc nhận và bộ đặc trưng.
Bảng Spark của notebook 04 vẫn mô tả cohort GPS/24 giờ/50 km. Holdout đóng
băng của cấu hình B + AOI + GPS lưu trong báo cáo lịch sử.
[Nguồn mẫu và kết quả](eta/training_data_lineage.md#35-so-sánh-rolling-origin-trên-cùng-mã-đơn)
