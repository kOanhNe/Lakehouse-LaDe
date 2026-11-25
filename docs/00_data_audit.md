# 00 — Data audit trước Silver

Chạy từ thư mục gốc:

```powershell
python -m scripts.checks.profile_delivery --source data/delivery_yt.csv --output reports/data
```

Script đọc toàn bộ 206.431 dòng ở dạng chuỗi, giữ nguyên raw input và tạo các bảng
EDA trong `reports/data/tables/`. Không có bộ lọc nào được áp dụng ngầm. Các cột
`q_*` chỉ là cờ chất lượng để đo lường trước khi quyết định policy Silver.

Các quyết định phải dựa trên những bảng sau:

- `column_profile.csv`: kiểu và mức thiếu dữ liệu;
- `quantiles.csv`: phân phối thời lượng, khoảng cách và độ trễ GPS;
- `quality_flags.csv`: tỷ lệ từng lỗi;
- `cohort_sensitivity.csv`: tác động của từng bộ lọc;
- `split_profile.csv`: phân phối theo train/validation/test;
- `anomaly_examples.csv`: ví dụ dòng bị gắn cờ.

Ngưỡng `>24h` và `50 km` không phải quy tắc làm sạch mặc định. Chúng chỉ được
dùng để định nghĩa cohort mô hình sau khi đã báo cáo tỷ lệ bị loại và phân tích độ
nhạy. Dữ liệu Silver vẫn phải giữ bảng rejected cùng `reject_reason` để truy vết.

Audit hiện tại trên file local có 206.431 dòng: 369 dòng có thời lượng bằng 0,
2.650 dòng dài hơn 24 giờ, 2.424 dòng có accept GPS không hợp lệ và 122 dòng có
khoảng cách stop–accept GPS lớn hơn 50 km. Vì vậy các ngưỡng này phải được báo cáo
như policy tạo cohort, không được gọi là quy luật tự nhiên của dữ liệu.

## Nguồn LaDe

- [Data card LaDe-D cho dữ liệu giao hàng](https://huggingface.co/datasets/Cainiao-AI/LaDe-D)
- [Data card LaDe và các bài toán benchmark](https://huggingface.co/datasets/Cainiao-AI/LaDe)
- [Mã benchmark của tác giả](https://github.com/wenhaomin/LaDe)
- [Website LaDe](https://cainiaotechai.github.io/LaDe-website/)
- [Bài báo LaDe](https://arxiv.org/abs/2306.10675)
