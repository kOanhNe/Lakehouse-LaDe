# Bảng kiểm kê leakage

| Nguồn có thể gây leak | A-lite | B đã chặn | B đã định lượng | Bằng chứng |
| --- | --- | --- | --- | --- |
| Courier average tính từ toàn bộ dữ liệu | Có | Có | Có | `dataset.py`, so sánh A-lite/B |
| Courier history dùng delivery sau query | Có thể xảy ra | Có | Có | expanding history trong `dataset.py` |
| `delivery_gps` của pending/candidate | Có thể xảy ra nếu đọc raw trực tiếp | Có | Có | allowlist + truncation test |
| Label `delivery_minutes` đi vào feature | Có nguy cơ ở tầng notebook cũ | Có | Có | `schema.yaml`, feature contract test |
| Pending order bị xóa khi truncation | Không áp dụng | Có | Không | test giữ pending |
| Split ranh giới làm thay đổi mẫu | Có | Có | Có | common spine và ablation đã đăng ký |
| Lọc nhãn `ETA <= 120` | Không áp dụng ở A-lite | B báo native và common spine | Có | `prereg.yaml` |

"Đã chặn" chỉ có nghĩa là có kiểm thử hoặc contract bảo vệ. "Đã định lượng" chỉ
được ghi khi có kết quả thực nghiệm trên cùng evaluation spine; static code không
đủ để điền cột này.

## Audit Protocol P

Các kết luận dưới đây dựa trên `src/eta/paper_protocol.py` của nhánh hiện tại.
Kết quả tái lập, cách chia dữ liệu, seed, thuật toán tạo mẫu và file metric đã lưu
được giữ nguyên. Bảng chỉ ra thông tin có sẵn tại thời điểm truy vấn và các phụ thuộc
hình thành khi xem toàn bộ lịch sử.

| Đặc trưng hoặc bước | Phụ thuộc vào dữ liệu tương lai | Bằng chứng |
| --- | --- | --- |
| `spd` | Có. `clean` ước lượng theo shipper trên toàn file trước khi chia ngày, gồm cả lần giao sau query và các ngày validation/test. SPEED và LightGBM đều dùng tốc độ này. | Unit test thêm giao hàng ngày sau. Cặp và đặc trưng khác ngày trước giữ nguyên, `spd` thay đổi. |
| `ntodo` | Có. `len(ok)` đếm ứng viên còn lại sau lọc theo nhãn `0 < fin[j] - fin[i] <= 120` và theo vị trí hoàn tất trong cửa sổ. | Unit test dời thời điểm giao của một đơn khác vượt ngưỡng 120 phút. Nhãn của đơn đang xét giữ nguyên, `ntodo` đổi từ 3 xuống 2. |
| `rank` | Có. Thứ hạng khoảng cách dùng cùng tập `ok`, nên thứ hạng của đơn còn lại đổi khi nhãn tương lai loại một ứng viên gần hơn. | Cùng test phản thực tế của `ntodo` cho thấy `rank` đổi từ 1 xuống 0. |
| `ndone` | Có thể phụ thuộc tương lai gián tiếp. Chỉ số `i` là vị trí trên quỹ đạo sau làm sạch. Bước A quyết định loại một dòng dựa trên cả cờ nhảy tốc độ tại dòng đó và cờ của lần giao kế tiếp. | `clean` dùng `shift(-1)`; `build_samples` đưa `i` vào feature. |
| `tmin`, `elapsed` | Phút query và thời gian từ lúc nhận của đơn đã biết tại query. Việc cặp có mặt trong bảng đặc trưng vẫn phụ thuộc điều kiện chọn mẫu ngoại tuyến. | `fin[i]`, `fin[i] - acc[j]`. |
| `dis`, `aoi` | Giá trị có thể sẵn tại query theo giả định tọa độ và loại AOI đích đã biết khi nhận đơn. Việc chọn ứng viên và chọn query vẫn dùng cửa sổ ngoại tuyến. | `haversine_m` dùng tọa độ đích, `aoi` lấy từ đơn ứng viên. Không có `delivery_gps_*` trong đầu vào Protocol P. |
| Cửa sổ train/validation/test | Có. Tập pending được dựng từ thứ tự giao xong, chọn theo nhãn, giới hạn cửa sổ 24 đơn và yêu cầu ít nhất hai ứng viên. Một ứng viên ngoài cửa sổ có thể loại cả cửa sổ test gồm các query trước đó. | `build_samples` tính `todo`, `ok` và hủy `recs` nếu `bad`. |

Hai kiểm thử phản thực tế xác nhận đường phụ thuộc, chưa đo phần chênh lệch MAE
do từng nguồn leakage. Một đánh giá point-in-time cần protocol riêng với tập pending
dựng từ sự kiện đã quan sát, vận tốc chỉ từ lịch sử có trước query và cohort
được định nghĩa độc lập với feature. Các metric hiện tại mô tả tái lập ngoại tuyến.

Xem phép đo độ nhạy của MAE khi thay `spd`, `ntodo`, `rank` và khi đổi cohort. [Kết quả thực nghiệm](leak_measurement.md)
