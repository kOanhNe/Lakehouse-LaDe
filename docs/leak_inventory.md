# Bảng kiểm kê leakage

| Nguồn có thể gây leak | A-lite | B đã chặn | B đã định lượng | Bằng chứng |
|---|---|---|---|---|
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
