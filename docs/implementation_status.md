# Trạng thái triển khai theo kế hoạch

Đã triển khai trên nhánh `thesis-rebuild-v1`:

1. Audit toàn bộ file dữ liệu và notebook `00_data_audit` trước Silver.
2. `config/schema.yaml` làm nguồn máy đọc được cho data dictionary và allowlist.
3. `docs/star_schema.md` chốt grain của fact và ETA gold.
4. `src/eta/` tách feature, split, dataset, model và metrics khỏi notebook.
5. Truncation test giữ pending order nhưng che toàn bộ trường phát sinh sau giao.
6. Deterministic query grid, chronological split và paired courier-day bootstrap.
7. Protocol B dùng courier history expanding theo `delivery_ts <= query_ts`; A-lite
   giữ statistic toàn file để định lượng leakage.
8. Prefect có task `4b` ghi `eta_safe_gold` và `eta_safe_metrics_gold`.
9. `config/prereg.yaml`, `config/freeze-v1.yaml` và bảng kiểm kê leak được lưu cùng code.

Kiểm tra hiện tại:

```powershell
.venv/Scripts/python.exe scripts/validate_schema.py
.venv/Scripts/python.exe -m pytest -q
docker compose config --quiet
```

Kết quả local: schema hợp lệ, 5 test contract/leakage pass và Compose hợp lệ.
Full pipeline Docker vẫn cần chạy sau khi build image trong môi trường có Docker
daemon. File dự đoán local chỉ là pilot smoke run để kiểm tra đường chạy, không
được dùng làm kết quả test chính thức; test chính thức phải chạy sau freeze/tag.
