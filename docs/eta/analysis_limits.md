# Giới hạn đã đo và cách xử lý

Các số dưới đây sinh từ script trong `scripts/`, chạy trên `data/delivery_yt.csv`.

## 1. Bộ lọc nhãn 1-120 phút của bài báo (`scripts/experiments/run_label_sensitivity.py`)
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

## 3. SCD2 shipper (`scripts/checks/scd2_evidence.py`)
Chỉ 3/205 shipper (1,5%) đổi khu vực chính, với 21 lần đổi; nếu chỉ lưu khu vực hiện tại thì 1.977/206.431 đơn (1,0%) bị gán sai khu vực của shipper tại ngày nhận. Kết luận: SCD2 đúng về mặt dữ liệu nhưng tác động nhỏ trên bộ dữ liệu này. Đồ án **không** khẳng định SCD2 làm ETA chính xác hơn; nó là thành phần kiến trúc.

## 4. Phạm vi so sánh kết quả ETA

- Protocol P là bài toán tái lập ngoại tuyến để đối chiếu mô tả với bài báo. Tốc độ shipper, tập ứng viên và cửa sổ có phụ thuộc dữ liệu tương lai. Xem [audit Protocol P](leak_inventory.md#audit-protocol-p).
- Protocol B là bài toán dùng để đánh giá ETA vận hành tại lúc nhận đơn. Phép so với A-lite là nghiên cứu leakage trước đây. Benchmark mới so LightGBM và Spark trên cùng đơn lúc nhận.
- Spark LR/GBT trong Prefect là baseline thời lượng giao hàng tại lúc nhận. Tập chấm riêng gồm mọi đơn Fact có nhãn hợp lệ từ ngày cắt. Lát ≤24 giờ là báo cáo sau dự đoán. Kết quả Spark trên cùng mẫu Protocol B nằm trong benchmark riêng.

MAE khoảng 20,7 phút mô tả LightGBM trên tập test của Protocol P. Mỗi phép xếp hạng dùng các mô hình có cùng thời điểm truy vấn, nhãn và tập đánh giá. Xem [bảng vai trò trong README](../../README.md#1-bài-toán).

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
python -m scripts.experiments.run_eta_accept_benchmark --holdout
~~~

### Kết quả trên 25.345 đơn nhận từ 25/09 đến 31/10/2022

Mỗi phương án đọc cùng một tập test trên CSV Yantai. Có 73.107 đơn để fit và
24.629 đơn validation có nhãn trước khi bắt đầu test. Các hàng LightGBM là
trung bình qua seed 42–44. [Kết quả theo seed](../../reports/eta/eta_accept_benchmark_holdout.csv)

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
bỏ đơn đó khỏi bảng mẫu có nhãn. Builder sự kiện mới dựng được đặc trưng
cho đơn còn mở. Dự đoán thực tế khi nhận đơn cần lưu và nạp lại mô hình
cùng median đã chốt, rồi ghi dự báo trước khi biết kết quả giao. Phép đo
trên Yantai chỉ dùng đơn có nhãn giao hàng từ năm 2022. [Đặc trưng theo sự kiện](../../src/eta/features.py) · [Bước batch Protocol B](../../notebooks/06_eta_accept.ipynb)

Một hướng để cải thiện ETA vận hành là đo thời gian theo tuyến đường
và thêm trạng thái hàng chờ đã quan sát. DeepETA của Uber dùng dữ liệu
tuyến đường và giao thông thời gian thực rồi dùng ML hiệu chỉnh.
[Thiết kế DeepETA](https://www.uber.com/us/en/blog/deepeta-how-uber-predicts-arrival-times/)

## 6. Giới hạn của bản hàng chờ và lịch sử

1. Audit định lượng ảnh hưởng của `spd`, `ntodo` và `rank` trong Protocol P
   bằng một biến thể thử nghiệm. Điểm tái lập Protocol P đi kèm giới hạn
   cohort và các phụ thuộc tương lai đã công bố. Với baseline Spark cũ,
   phép xấp xỉ bằng LightGBM đo ảnh hưởng của trung bình shipper có nhãn
   của chính dòng. Notebook 04 hiện tính lịch sử từ các sự kiện đã quan sát
   và dùng nhãn fit đã có trước ngày cắt. [Phép đo leakage](leak_measurement.md)
2. Trên 24.647 đơn ở ba cửa sổ phát triển rolling-origin, thêm hàng chờ
   và lịch sử giảm MAE từ 109,82 xuống **104,16 phút** qua seed 42–44 khi
   giữ nguyên LightGBM và bộ đặc trưng nền theo sự kiện. Mức giảm là
   **5,66 phút** so với bộ đặc trưng nền và **5,36 phút** so với cấu hình
   lịch sử. Cả ba cửa sổ đã dùng để chọn đặc trưng. Holdout ở mục 5 thuộc
   cấu hình B + AOI + GPS lịch sử và từng được xem trong một lượt thăm dò
   với bộ đặc trưng gần giống. CSV Yantai có đơn nhận tới 31/10/2022, cũng
   là ngày cuối của holdout đã xem. Để
   đánh giá bản hàng chờ độc lập, cần chốt đặc trưng, cấu hình và cách chấm
   trước khi chấm một lần trên giai đoạn mới. [Kết quả rolling-origin](training_data_lineage.md#35-so-sánh-rolling-origin-trên-cùng-mã-đơn)
3. Spark LR/GBT trong Prefect là baseline tham chiếu. Notebook 04 giữ cấu
   hình 40 cây cố định để mỗi lần chạy chỉ cần một lượt fit GBT với số cây
   giới hạn. Trong benchmark cùng đơn và cùng bộ đặc trưng hàng chờ và lịch
   sử với seed 42, GBT đã hiệu chỉnh đạt **126,39 phút** và LightGBM đạt
   **103,71 phút**. Chênh lệch là 22,68 phút với khoảng tin cậy 95% từ 20,03
   đến 25,68 phút. Kết quả phản ánh các cấu hình đã thử và chưa tách được
   ảnh hưởng riêng của loss hay step size. [Đối chứng Spark và LightGBM](training_data_lineage.md#đối-chiếu-lightgbm-và-spark)

Bản mới đạt ACC@30 **31,88%**, tức khoảng **68 trên 100 đơn** lệch quá
30 phút. P90 sai số tuyệt đối khoảng **222 phút**. Mức này còn xa yêu cầu
hứa một ETA trong vòng ±30 phút cho khách. Dữ liệu Yantai thiếu sự kiện
hủy đơn, nên đơn đã nhận có thể còn trong hàng chờ khi thiếu lần giao hợp
lệ. Đo độ phủ phân vị và thử nghiệm dự báo có dấu thời gian lúc nhận là
các phép đánh giá riêng. [Chi tiết mẫu và kết quả](training_data_lineage.md#35-so-sánh-rolling-origin-trên-cùng-mã-đơn)

## 7. Khoảng ETA lúc nhận đơn

Thí nghiệm dùng `data/delivery_yt.csv` đã kiểm tra SHA-256 theo `prereg.yaml`,
cùng mã đơn và bộ đặc trưng `protocol_b_queue_history` ở mục 6. Nhãn là số
phút từ nhận đến giao. Tập chấm gồm 24.647 đơn trong ba cửa sổ phát triển
từ 19/08 đến 24/09/2022. [Nguồn mẫu và đặc trưng](training_data_lineage.md#35-so-sánh-rolling-origin-trên-cùng-mã-đơn)

1. Mỗi cửa sổ dùng phần fit của rolling-origin. Các ngày trong tập hiệu chỉnh
   được chia đôi theo thứ tự thời gian. Nửa đầu dùng để dừng sớm theo quantile
   loss, nửa sau dùng để tính hiệu chỉnh conformal. Nhãn phải có trước mốc
   bắt đầu giai đoạn kế tiếp.
2. LightGBM học P10, P50 và P90 bằng ba mô hình quantile. Giá trị thiếu được
   điền bằng median trên tập fit. Ba dự đoán được đưa về không âm và sắp thứ
   tự trước khi hiệu chỉnh. Mốc vùng–giờ lấy ba phân vị của nhãn fit theo
   vùng và khung giờ 4 tiếng. Nhóm có dưới 30 đơn dùng phân vị của toàn bộ fit.
3. Khoảng 80% dùng điểm `max(P10 - y, y - P90)`. Với `n` nhãn conformal,
   lấy điểm ở hạng `ceil(0,80 × (n + 1))` theo thứ tự tăng dần. Trừ giá trị
   này khỏi P10 và cộng vào P90. Cận dưới tối thiểu bằng 0 và khoảng luôn
   chứa P50. Hạn hứa P90 hiệu chỉnh riêng bằng điểm `y - P90` ở hạng
   `ceil(0,90 × (n + 1))`. Hạn hứa tối thiểu bằng P50. Hai phương án dùng
   cùng cách hiệu chỉnh trên cùng các đơn. [Romano, Patterson và Candès (2019)](https://proceedings.neurips.cc/paper_files/paper/2019/file/5103c3584b063c431bd1268e9b5e76fb-Paper.pdf)

`late_risk` được gắn khi hạn hứa P90 vượt thời hạn được nhập bằng
`--deadline-minutes`. Ví dụ dùng 360 phút tính từ lúc nhận. Báo cáo lưu
số đơn được gắn cờ, số đơn giao quá hạn thực tế và giao của hai tập này.
Quy tắc giúp chọn đơn cần theo dõi theo một mốc vận hành cụ thể.

### Kết quả trên tập phát triển

Kết quả dưới đây dùng seed 42 và LightGBM 4.7.0. Khoảng tin cậy 95% dùng
2.000 lượt bootstrap theo 3.579 cụm shipper–ngày.

| Mô hình | Độ phủ trước hiệu chỉnh | Độ phủ sau hiệu chỉnh (KTC 95%) | Rộng trung bình (phút) | Rộng trung vị (phút) | Trần khoảng trung vị (phút) |
| --- | ---: | ---: | ---: | ---: | ---: |
| LightGBM, khoảng 80% | 71,54% | 79,80% (79,14–80,48%) | 299,63 | 247,00 | 374,55 |
| Vùng–giờ, khoảng 80% | 78,63% | 79,98% (79,31–80,60%) | 429,79 | 378,00 | 465,80 |

| Mô hình | Đúng hạn P90 | Trễ hạn P90 | Hạn P90 trung bình (phút) | Hạn P90 trung vị (phút) |
| --- | ---: | ---: | ---: | ---: |
| LightGBM | 90,59% | 9,41% | 454,68 | 391,88 |
| Vùng–giờ | 90,60% | 9,40% | 518,04 | 485,80 |

LightGBM thu hẹp độ rộng trung vị **34,7%** ở mức phủ gộp gần nhau.
Với tỷ lệ đúng hạn P90 xấp xỉ bằng nhau, hạn hứa ngắn hơn **93,92 phút
ở trung vị (19,3%)** và **63,36 phút ở trung bình**. Khoảng điển hình vẫn
rộng hơn 4 giờ và hạn hứa trung vị khoảng 6,5 giờ.

| Lát của LightGBM, seed 42 | Số đơn | Độ phủ khoảng 80% | Đúng hạn P90 |
| --- | ---: | ---: | ---: |
| 19–31/08 | 8.642 | 80,50% | 90,72% |
| 01–12/09 | 8.395 | 76,49% | 88,15% |
| 13–24/09 | 7.610 | 82,65% | 93,14% |
| Lô một đơn | 6.639 | 76,29% | 88,30% |
| Lô 2–3 đơn | 8.628 | 80,35% | 90,99% |
| Lô từ 4 đơn | 9.380 | 81,77% | 91,84% |
| Nhận từ 04:00 đến trước 08:00 | 328 | 70,12% | 81,10% |
| Vùng 162 | 571 | 59,19% | 63,92% |
| Thời lượng thực tế >24 giờ | 594 | 57,07% | 60,27% |

Cỡ lô được tính theo các đơn cùng shipper và phút nhận để cắt lát báo cáo.
Lát >24 giờ được tính sau dự đoán. Với hạn minh họa 360 phút, mô hình gắn
cờ 14.419 đơn. Trong số đó, 5.550 đơn thực tế giao quá hạn, tương đương
38,49% số cờ và 92,86% trong 5.977 đơn thực tế giao quá hạn.

Qua seed 42–44, độ phủ gộp 79,80–80,22%, độ rộng trung vị 247–251 phút,
và đúng hạn P90 90,30–90,59%. Chạy lại seed 42 cho cùng báo cáo và cùng
dự đoán theo đơn. Chênh lệch độ phủ giữa các cửa sổ lớn hơn độ rộng của
khoảng tin cậy gộp. Bootstrap mô tả biến động lấy mẫu trong giai đoạn đã
chấm. Bảo đảm conformal dựa trên giả định các quan sát có thể hoán đổi,
trong khi dữ liệu chấm theo thời gian có thể đổi phân phối.
[Conformal khi phân phối thay đổi](https://papers.neurips.cc/paper/2021/hash/0d441de75945e5acbc865406fc9a2559-Abstract.html)

Kết quả phù hợp để demo độ bất định và minh họa cách chọn đơn theo dõi
trong điều phối. Đánh giá khả năng hứa hạn giao với khách cần một holdout
theo thời gian và dữ liệu vận hành độc lập. [Giới hạn của holdout Yantai](#6-giới-hạn-của-bản-hàng-chờ-và-lịch-sử)

~~~powershell
python -m scripts.experiments.run_eta_accept_benchmark --intervals --seeds 42 43 44 --deadline-minutes 360 --predictions-output reports/eta/eta_predictions_intervals.csv
~~~

CSV tổng hợp ghi số dòng và mốc thời gian mỗi giai đoạn, độ phủ trước và
sau hiệu chỉnh, độ rộng trung bình và trung vị, cùng hạn hứa P90. CSV theo
đơn sinh từ tùy chọn `--predictions-output` để so tay lại dự đoán và nhãn.
[Kết quả từng cửa sổ, lô, vùng, khung giờ và lát thời lượng](../../reports/eta/eta_accept_intervals_rolling.csv) · [Notebook tính tay](../../notebooks/06_eta_accept.ipynb)
