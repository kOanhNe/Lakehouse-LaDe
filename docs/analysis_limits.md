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

## 4. Vai của các protocol
- Protocol P: bài toán chính, so được với bài báo.
- Protocol A-lite / B: bài toán phụ "ETA lúc nhận đơn", dùng để định lượng leakage; không so với bài báo.
