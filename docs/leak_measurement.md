# Đo mức độ leakage bằng thực nghiệm

Các phép đo dùng `data/delivery_yt.csv` với SHA-256 trong `config/prereg.yaml` và seed 42. Mục 1 và 2 dùng adapter LightGBM của Protocol P. Mục 3 dùng cấu hình LightGBM riêng trong script. Protocol P gốc tái lập MAE 20,67 phút đã lưu cho seed 42. [Kiểm kê leakage](leak_inventory.md) · [Metric Protocol P](../reports/eta_paper_metrics.csv)

## 1. Ba feature rò rỉ của Protocol P (đối chứng ghép cặp)

`src/eta/paper_protocol_pit.py` giữ nguyên tập cặp (truy vấn, đơn), nhãn, cửa sổ và cách chia ngày của Protocol P. Script kiểm tra mọi cột gốc trùng với `build_samples` trước khi đo ảnh hưởng của ba cách thay feature.

| Feature gốc | Bản point-in-time |
| --- | --- |
| Tốc độ shipper `spd` trên cả file | `spd_pit` tích lũy tới truy vấn trên quỹ đạo đã làm sạch |
| Số ứng viên `ntodo` sau khi lọc nhãn ≤ 120 và giới hạn cửa sổ | `ntodo_pit` đếm ứng viên trong quỹ đạo trước hai điều kiện này |
| Hạng khoảng cách `rank` trong tập đã lọc | `rank_pit` xếp hạng trong tập ứng viên trước lọc |

| Mô hình, test (n = 198.463) | MAE (phút) | RMSE gộp (phút) | ACC@20 (%) | ACC@30 (%) |
| --- | ---: | ---: | ---: | ---: |
| P gốc (có leak) | 20,67 | 27,11 | 59,19 | 76,20 |
| chỉ sửa `spd` | 20,85 | 27,36 | 58,85 | 75,86 |
| chỉ sửa `ntodo` + `rank` | 20,77 | 26,94 | 58,08 | 75,78 |
| sửa cả ba | **20,95** | 27,15 | 57,62 | 75,40 |
| median (train) | 28,51 | 33,18 | 36,57 | 54,31 |
| SPEED, `spd` cả file / `spd_pit` | 33,63 / 33,71 | 44,55 / 48,49 | 40,66 / 41,02 | 54,33 / 54,72 |

Trên cùng tập test, thay đồng thời ba feature làm MAE tăng 0,28 phút với seed 42. LightGBM vẫn có MAE thấp hơn median và SPEED trong phép đối chứng này. [CSV kết quả](../reports/eta_paper_pit_metrics.csv)

## 2. Cohort chọn theo nhãn (0 < nhãn ≤ 120 và ít nhất hai đơn)

Protocol P dùng nhãn đã quan sát sau khi giao để giữ các cặp có thời gian còn lại tối đa 120 phút. `scripts/run_paper_protocol_cohort.py` giữ cùng bộ feature đã thay ở mục 1 và so sánh các trường hợp có hoặc bỏ ngưỡng. Khi bỏ ngưỡng, điều kiện tối thiểu hai ứng viên và luật loại cửa sổ test có thể thay đổi cả tập truy vấn. B và C dùng chung tập test.

| Trường hợp | n test | MAE (phút) | MAE của median (phút) |
| --- | ---: | ---: | ---: |
| A. train và test trên cohort ≤ 120 (Protocol P, PIT) | 198.463 | 20,95 | 28,51 |
| B. mô hình của A, chấm trên tập bỏ ngưỡng 120 | 215.319 | 69,01 | 79,73 |
| C. train và test trên tập bỏ ngưỡng 120 | 215.319 | 60,17 | 76,22 |
| D. như C, chỉ chấm cặp test có nhãn ≤ 120 | 139.657 | 30,54 | — |

Trong tập test bỏ ngưỡng 120, 64,9% cặp có nhãn ≤ 120 phút và phân vị 90% của nhãn là 258 phút. MAE 20,67 phút ứng với cohort đã lọc của Protocol P. MAE 60,17 phút của C là kết quả ngoại tuyến trên tập bỏ ngưỡng trong các quỹ đạo và cửa sổ được lấy mẫu. LightGBM giảm MAE khoảng 21% so với median trên tập của C. Báo cáo sai số triển khai cần đánh giá riêng với tập ứng viên và feature được dựng hoàn toàn từ sự kiện đã quan sát. [CSV kết quả](../reports/eta_paper_cohort_metrics.csv)

## 3. Baseline Spark: `courier_train_avg_min`

`steps._train_baseline` tính trung bình thời gian giao theo shipper trên toàn tập train rồi gán lại cho cả các dòng train, nên nhãn của một dòng nằm trong feature của chính nó. `scripts/check_courier_avg_leak.py` dựng lại cohort Yantai (train 165.222, test 35.778, MAE median 127,59 phút, khớp Spark) và dùng LightGBM làm phép xấp xỉ.

| Cách tính feature với LightGBM | MAE (phút) | RMSE (phút) |
| --- | ---: | ---: |
| không có feature | 97,26 | 140,98 |
| trung bình trên toàn train, gồm chính dòng (code hiện tại) | 91,57 | 135,84 |
| lịch sử đã hoàn tất tới lúc nhận, dự phòng bằng trung bình train | 92,58 | 135,13 |

Trong phép xấp xỉ, biến thể dùng lịch sử giảm MAE từ 97,26 xuống 92,58 phút, cao hơn 1,01 phút so với cách tính toàn train. Chênh lệch này đo hai cách tạo feature trong pipeline xấp xỉ, gồm cả việc cập nhật lịch sử theo thời gian và giá trị dự phòng.

## Giới hạn

- Các bảng dùng một seed (42). Với Protocol P gốc, ba seed chênh nhau dưới 0,03 phút MAE.
- Bản PIT dùng lại nhóm đơn theo mã ngày `ds` từ CSV, thứ tự giao trên quỹ đạo đã làm sạch, `ndone` và bước `clean` dùng `shift(-1)`. Luật lấy mẫu theo cửa sổ 24 đơn vẫn dùng thứ tự giao và nhãn. Ảnh hưởng của các nguồn này chưa được định lượng.
- Mục 3 dùng pandas và LightGBM. Biến thể lịch sử chỉ xét các đơn thuộc cohort đã lọc theo thời lượng, dùng trung bình toàn train khi shipper thiếu lịch sử và cập nhật từ các lần giao quan sát được trong test. MAE của Spark LR/GBT với từng biến thể chưa được đo.
- Chưa đo lại Protocol B (A-lite so với B đã có trong [analysis_limits.md](analysis_limits.md#2-tập-truy-vấn-của-a-lite--b-prereg-yaml)).

## Chạy lại

~~~bash
python scripts/run_paper_protocol_pit.py --seeds 42 --output reports/eta_paper_pit_metrics.csv
python scripts/run_paper_protocol_cohort.py
python scripts/check_courier_avg_leak.py
pytest tests/test_paper_protocol_pit.py
~~~
