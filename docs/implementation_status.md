# Trạng thái triển khai

Đợt ổn định này dựa trên commit `488db2e`. Prefect chạy tám bước
từ Bronze đến MySQL. Notebook hiện tại gọi chung các hàm trong `pipeline/steps.py`.

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

Lệnh và yêu cầu môi trường nằm trong [README](../readme.md#10-kiểm-tra-và-test).
Chạy marker `not integration` trước. Bộ unit kiểm tra hợp đồng nguồn, phân tách
Silver/Rejected, SCD2, các khóa dimension, fact, tổng hợp serving, trạng thái
dashboard và các phụ thuộc tương lai của Protocol P.

Bộ integration cần Spark/Delta, MinIO, MySQL và API Prefect. Nó chạy flow với
CSV mẫu và namespace riêng, tạo lỗi sau khi một bảng serving đã được ghi, xác
nhận lần lỗi bị ẩn và chạy lại để đối chiếu grain, khóa và mã xuất dữ liệu.
Một kiểm thử khác dùng kết nối MySQL thật để kiểm tra tranh chấp khóa, nhận biết
mất kết nối và ghi nhận lần chạy bị gián đoạn.

| Kiểm tra | Kết quả |
|---|---|
| Unit trong container Jupyter, `pytest -m "not integration" -q` | 28 đạt, 2 integration được loại theo marker. |
| Integration trong Docker với các dịch vụ thật | 2 đạt. Đã chạy thành công, tiêm lỗi khi serving, chạy khôi phục và kiểm tra mất khóa MySQL. |
| Đối soát chỉ đọc với Yantai | SHA-256 của CSV trên MinIO khớp bản đăng ký. Delta hiện có 206.431 Bronze, 206.062 Silver, 369 Rejected và 206.062 fact. Tổng đơn serving MySQL khớp Silver. |
| Cấu hình Compose và `config/schema.yaml` | Hợp lệ. |

## Kết quả ETA

Giữ nguyên thuật toán, seed, cách chia và các kết quả nghiên cứu đã lưu.
[Audit leakage](leak_inventory.md#audit-protocol-p) ghi rõ các phụ thuộc của
`spd`, `ntodo`, `rank`, quỹ đạo đã làm sạch và cách chọn cửa sổ. Hai phép thử
phản thực tế kiểm tra sự phụ thuộc của đặc trưng. Metric hiện tại mô tả kết quả
tái lập ngoại tuyến trên cohort của Protocol P.

Protocol B đánh giá ETA tại lúc nhận đơn với kiểm soát leakage theo contract.
Spark LR/GBT là baseline thời lượng giao hàng trên cohort riêng. Dashboard có
tab cho từng bài toán. Xem [phạm vi so sánh MAE](analysis_limits.md#4-phạm-vi-so-sánh-kết-quả-eta).

Script thử nghiệm Protocol B so ba bộ đặc trưng trên cùng 73.107 đơn train
và 24.647 đơn validation. Hai unit test kiểm tra mốc quan sát của lịch sử
AOI, GPS và nhãn huấn luyện. Kết quả hiện có dùng validation với seed 42.
[Nguồn dữ liệu và cách chạy](training_data_lineage.md#34-thử-nghiệm-thêm-lịch-sử-aoi-và-gps-cho-protocol-b)
