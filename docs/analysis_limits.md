# Giới hạn đã đo và cách xử lý

Các số dưới đây sinh từ script trong `scripts/`, chạy trên `data/delivery_yt.csv`.

## 1. Bộ lọc nhãn 1-120 phút của bài báo (`scripts/run_label_sensitivity.py`)
Bài báo chỉ giữ cặp (truy vấn, đơn) có số phút còn lại trong (0, 120]. Bộ lọc này loại khoảng 48% cặp. Bảng dưới cho thấy kết quả thay đổi thế nào khi đổi ngưỡng (LightGBM seed 42; kết quả chính thức dùng 3 seed):

| Ngưỡng nhãn | Giữ lại % cặp | MAE trung vị | MAE SPEED | MAE LightGBM | ACC@20 LightGBM |
|---:|---:|---:|---:|---:|---:|
| 60 phút | 28,9 | 14,0 | 19,5 | 11,0 | 84,8% |
| **120 phút (bài báo)** | **51,8** | **28,5** | **33,6** | **20,7** | **59,2%** |
| 240 phút | 80,0 | 50,2 | 55,2 | 36,2 | 41,7% |
| 480 phút | 97,7 | 70,5 | 75,7 | 53,9 | 35,9% |

Đọc: sai số tuyệt đối phụ thuộc rất mạnh vào ngưỡng, nên con số 20,7 phút chỉ đúng cho đơn sắp giao. Điều **không đổi** là thứ tự mô hình: LightGBM luôn tốt hơn mốc trung vị khoảng 22-28% ở mọi ngưỡng, và SPEED luôn kém mốc trung vị. Vì vậy kết luận "học từ dữ liệu tốt hơn công thức khoảng cách / tốc độ" không phụ thuộc vào bộ lọc, còn con số MAE tuyệt đối thì có.

## 2. Tập truy vấn của A-lite / B (`prereg.yaml`)
`query_grid` nay được dùng: mỗi (shipper, ngày) giữ tối đa 12 truy vấn, chọn bằng băm xác định (seed 42), A-lite và B dùng chung tập này (123.117 dòng thay vì 206.062). Trên tập chung đó, LightGBM test MAE: A-lite 114,2, B 117,5 phút (mốc trung vị 182,3). Chênh lệch do leakage khoảng 3 phút, không lớn.

## 3. SCD2 shipper (`scripts/scd2_evidence.py`)
Chỉ 3/205 shipper (1,5%) đổi khu vực chính, với 21 lần đổi; nếu chỉ lưu khu vực hiện tại thì 1.977/206.431 đơn (1,0%) bị gán sai khu vực của shipper tại ngày nhận. Kết luận: SCD2 đúng về mặt dữ liệu nhưng tác động nhỏ trên bộ dữ liệu này. Đồ án **không** khẳng định SCD2 làm ETA chính xác hơn; nó là thành phần kiến trúc.

## 4. Phạm vi so sánh kết quả ETA

- Protocol P là bài toán tái lập ngoại tuyến để đối chiếu mô tả với bài báo. Tốc độ shipper, tập ứng viên và cửa sổ có phụ thuộc dữ liệu tương lai. Xem [audit Protocol P](leak_inventory.md#audit-protocol-p).
- Protocol B là bài toán phụ tại lúc nhận đơn, có kiểm soát leakage theo contract. Phép so với A-lite dùng cùng tập truy vấn.
- Spark LR/GBT là baseline thời lượng giao hàng từ lúc nhận đơn, đánh giá trên cohort riêng có GPS hợp lệ, khoảng cách tối đa 50 km và thời lượng tối đa 24 giờ.

MAE khoảng 20,7 phút mô tả LightGBM trên tập test của Protocol P. Mỗi phép xếp hạng dùng các mô hình có cùng thời điểm truy vấn, nhãn và tập đánh giá. Xem [bảng vai trò trong README](../readme.md#1-bài-toán).

## 5. Kiểm tra ETA lúc nhận đơn cho vận hành

Chọn B + AOI + GPS dựa trên validation của benchmark và khóa cách chấm tại tag
`eta-accept-holdout-v1`. Các phương án cùng dự đoán số phút từ nhận đến giao
trên các đơn test của Protocol B. CSV Yantai và quy tắc chọn truy vấn theo
ngày lấy từ `config/prereg.yaml`. So với trung vị train, trung vị theo vùng
và khung bốn giờ, và Protocol B gốc. Trung vị theo nhóm dùng tối thiểu 30 đơn
train, rồi lùi về trung vị toàn train với nhóm ít dữ liệu.

Chỉ dùng nhãn train có trước validation. Chỉ dùng nhãn validation có trước
ngày bắt đầu test để dừng sớm và lấy sai số hiệu chỉnh. Chạy seed 42–44.
Chấm MAE, P90 sai số tuyệt đối, ACC@30, ACC@60 và tỷ lệ dự đoán sớm hơn
thực tế trên 60 phút. Giữ toàn bộ đơn trong tập test đã đăng ký.

Với B + AOI + GPS, thử khoảng dự đoán từ phân vị 10% và 90% của sai số
có dấu trên validation. Đo tỷ lệ đơn test lọt khoảng cùng độ rộng trung vị.
Để thử ưu tiên kiểm tra đơn lâu, chọn 10% đơn có ETA dài nhất và đếm số đơn
thực tế mất hơn ba giờ. Ba giờ và 10% là mốc minh họa cho cách dùng nội bộ.

~~~bash
python scripts/run_eta_accept_benchmark.py --holdout
~~~

### Kết quả trên 25.345 đơn nhận từ 25/09 đến 31/10/2022

Mỗi phương án đọc cùng một tập test trên CSV Yantai. Có 73.107 đơn để fit và
24.629 đơn validation có nhãn trước khi bắt đầu test. Các hàng LightGBM là
trung bình qua seed 42–44. [Kết quả theo seed](../reports/eta_accept_benchmark_holdout.csv)

| Phương án | MAE (phút) | ACC@30 | ACC@60 | P90 sai số (phút) | Dự đoán sớm hơn thực tế trên 60 phút |
| --- | ---: | ---: | ---: | ---: | ---: |
| Trung vị train | 182,28 | 13,78% | 28,01% | 315,00 | 37,43% |
| Trung vị theo vùng và khung giờ | 142,57 | 17,84% | 34,89% | 280,00 | 34,75% |
| Protocol B | 118,66 | 24,75% | 45,84% | 233,92 | 27,64% |
| B + AOI + GPS | 114,63 | 28,21% | 49,35% | 229,81 | 23,21% |

B + AOI + GPS giảm MAE 4,03 phút so với B. Trên 100 đơn, khoảng 72 đơn
lệch quá 30 phút và 23 đơn giao muộn hơn ETA trên một giờ. Khoảng dự đoán
nhắm 80% phủ thực tế 76,56% với độ rộng trung vị 295,39 phút. Kết quả này
chưa phù hợp để báo một giờ giao cụ thể cho khách.

### Phạm vi thử nghiệm nội bộ

Trong mô phỏng xếp hạng cả tập test, chọn 2.535 đơn có ETA dài nhất.
Trong nhóm này, 96,90% đơn thực tế mất hơn ba giờ. Tỷ lệ của toàn tập là
53,55%. Đối chiếu bổ sung với cách xếp 2.535 đơn theo trung vị vùng và
khung giờ cho tỷ lệ 71,91%. Với hai cách, các ETA bằng nhau được xếp
theo thời điểm nhận rồi đến mã đơn. Nhóm B + AOI + GPS bao phủ 18,10%
tổng số đơn trên ba giờ, phù hợp để thử ưu tiên kiểm tra khi năng lực
theo dõi có hạn. Kết quả này đo xếp hạng ngoại tuyến. Kiểm chứng vận
hành cần ghi dự báo tại từng lần nhận đơn và so với kết quả giao sau đó.

Repo hiện huấn luyện và chấm theo batch. Dashboard đọc metric đã ghi.
Khi bỏ thời điểm giao và nhãn của đơn `2641862`, `build_eta_samples()`
bỏ đơn đó khỏi bảng mẫu. Đường dự đoán lúc nhận cần dựng đặc trưng cho
đơn còn mở và nạp lại mô hình cùng giá trị điền thiếu đã chốt. Phép đo
trên Yantai chỉ dùng đơn có nhãn giao hàng từ năm 2022. [Builder mẫu](../src/eta/dataset.py) · [Bước batch Protocol B](../notebooks/04b_eta_safe.ipynb)

Một hướng để cải thiện ETA vận hành là đo thời gian theo tuyến đường
và thêm trạng thái hàng chờ đã quan sát. DeepETA của Uber dùng dữ liệu
tuyến đường và giao thông thời gian thực rồi dùng ML hiệu chỉnh.
[Thiết kế DeepETA](https://www.uber.com/us/en/blog/deepeta-how-uber-predicts-arrival-times/)
