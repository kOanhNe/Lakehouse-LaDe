# Delivery Lakehouse — LaDe-D Yantai

Dự án nạp dữ liệu giao hàng Yantai (LaDe-D) vào Delta Lake, kiểm tra chất lượng dữ liệu, dựng lịch sử shipper (SCD2) và đưa các bảng tổng hợp lên MySQL/Metabase.

## Về dữ liệu

- Nguồn: `delivery_yt` trong LaDe-D, 206,431 đơn, 1 thành phố, 205 shipper, 17 khu vực, 8,163 AOI, 16 mã AOI.
- `delivery_minutes` là thời gian từ lúc nhận đơn đến lúc hoàn thành. Không phải thời gian lái xe.
- Các cột GPS là quan sát gần nhất với sự kiện, nên việc có hay không có GPS được ghi lại riêng.
- Tầng Silver giữ các đơn có thời gian dương và gắn cờ chất lượng cho: đơn qua ngày, thời gian trên 24 giờ, GPS không hợp lệ, khoảng cách đường chim bay quá lớn. Chỉ nhóm phân tích (cohort) khai báo rõ trong từng notebook Gold/ML mới được loại các dòng bị gắn cờ.

Đọc trước:

- [Báo cáo kiểm tra dữ liệu](reports/data/README.md)
- [Từ điển dữ liệu](docs/data_dictionary.md)
- [Tóm tắt tự sinh](reports/data/summary.json)
- [Thứ tự chạy notebook](notebooks/README.md)

## Yêu cầu

- Docker Desktop (đang chạy) và Docker Compose v2.
- File dữ liệu `delivery_yt.csv` (bản Yantai của LaDe-D, nguồn xem trong `reports/data/sources`).
- Cổng trống: 9000, 9001 (MinIO), 8888 (Jupyter), 3306 (MySQL), 3000 (Metabase).
- Khoảng vài GB trống ổ đĩa cho image và dữ liệu.

## Các bước chạy

**1. Dựng các dịch vụ**

```bash
docker compose up -d --build
docker compose ps
```

Chờ đến khi `docker compose ps` cho thấy các dịch vụ đang chạy (MinIO, Jupyter, MySQL, Metabase). Lần đầu có thể mất vài phút để tải image.

Nếu lệnh báo lỗi `pull access denied` hoặc `unauthorized` ở MinIO, xem mục "Xử lý lỗi" bên dưới.

**2. Đưa dữ liệu vào MinIO**

- Mở `http://localhost:9001`.
- Đăng nhập: user `minioadmin`, mật khẩu `minioadmin` (theo `docker-compose.yml`).
- Kiểm tra bucket `lakehouse` đã có chưa (service `minio-init` tạo bucket này). Nếu chưa có thì tạo bucket tên `lakehouse`.
- Upload `delivery_yt.csv` vào bucket để đường dẫn là `s3a://lakehouse/delivery_yt.csv`.

**3. Chạy notebook**

- Mở Jupyter tại `http://localhost:8888`.
- Chạy lần lượt notebook `00` đến `05` theo đúng thứ tự trong [notebooks/README.md](notebooks/README.md).

**4. Xem kết quả**

- Metabase: `http://localhost:3000`, đọc các bảng tổng hợp từ MySQL.
- Thông tin kết nối MySQL (host, user, mật khẩu, database): xem trong `docker-compose.yml`.

## Các dịch vụ

| Dịch vụ | Cổng | Vai trò |
|---|---|---|
| MinIO | 9000 (API), 9001 (console) | Lưu file nguồn và dữ liệu Delta |
| Jupyter | 8888 | Chạy notebook Spark |
| MySQL | 3306 | Lưu bảng tổng hợp |
| Metabase | 3000 | Dashboard |

MySQL, Metabase, MinIO và Hive metastore dùng named volume, nên dữ liệu còn sau khi tắt dịch vụ.

## Kiểm tra dữ liệu cục bộ (không cần Spark)

Cần có môi trường `.venv` với Python và các thư viện đã cài.

Windows (PowerShell):

```powershell
.venv/Scripts/python.exe scripts/profile_delivery.py
```

macOS/Linux:

```bash
.venv/bin/python scripts/profile_delivery.py
```

Lệnh này ghi bảng kiểm tra chất lượng theo từng dòng vào `reports/data`.

## Kiểm thử

Windows (PowerShell):

```powershell
.venv/Scripts/python.exe scripts/check_project.py
docker run --rm --mount type=bind,source=${PWD},target=/workspace,readonly --workdir /workspace --entrypoint /opt/spark/bin/spark-submit apache/spark:3.5.3-python3 /workspace/scripts/test_spark.py
```

macOS/Linux:

```bash
.venv/bin/python scripts/check_project.py
docker run --rm --mount type=bind,source="$PWD",target=/workspace,readonly --workdir /workspace --entrypoint /opt/spark/bin/spark-submit apache/spark:3.5.3-python3 /workspace/scripts/test_spark.py
```

Lệnh thứ hai kiểm tra: quy tắc xử lý trùng của SCD2, fact đúng grain một-một, lịch sử không bị hở hoặc chồng lấn, khoảng cách GPS hợp lệ.

## Tắt và reset

```bash
docker compose down        # tắt, giữ dữ liệu
docker compose down -v     # tắt và XÓA toàn bộ volume (reset hoàn toàn)
```

Chỉ dùng `down -v` khi muốn làm lại từ đầu. Sau đó phải upload lại `delivery_yt.csv` và chạy lại notebook.

## Xử lý lỗi

**`pull access denied` / `unauthorized` khi tải image MinIO**

MinIO đã ngừng phát hành image trên Docker Hub và quay.io, nên `minio/minio` và `quay.io/minio/*` không tải được. Cách sửa là đổi sang bản sao công khai trên Docker Hub, chạy các lệnh dưới đây TRƯỚC khi chạy `docker compose up -d --build`. Chạy trong thư mục gốc của project.

Hai image thay thế (chỉ có kiến trúc amd64):

- `scnd/minio-mirror:RELEASE.2025-09-07T16-13-09Z` (server)
- `scnd/mc-mirror:RELEASE.2025-02-04T04-57-50Z` (client tạo bucket)

*Mac chip Intel:*

```bash
sed -i '' 's|image: .*minio/minio.*|image: scnd/minio-mirror:RELEASE.2025-09-07T16-13-09Z|; s|image: .*minio/mc.*|image: scnd/mc-mirror:RELEASE.2025-02-04T04-57-50Z|' docker-compose.yml
docker compose up -d --build
```

*Mac chip Apple Silicon (M1/M2/M3/M4):* chạy hai lệnh đổi image như trên, rồi thêm dòng `platform: linux/amd64` dưới hai dòng `image:` mới để chạy qua giả lập:

```bash
sed -i '' 's|image: .*minio/minio.*|image: scnd/minio-mirror:RELEASE.2025-09-07T16-13-09Z|; s|image: .*minio/mc.*|image: scnd/mc-mirror:RELEASE.2025-02-04T04-57-50Z|' docker-compose.yml
sed -i '' '/image: scnd\/minio-mirror/a\
    platform: linux/amd64
' docker-compose.yml
sed -i '' '/image: scnd\/mc-mirror/a\
    platform: linux/amd64
' docker-compose.yml
docker compose up -d --build
```

*Linux:* giống Mac Intel nhưng bỏ `''` sau `-i` (`sed -i 's|...|...|' docker-compose.yml`). Nếu là chip ARM thì thêm dòng `platform` như bản Apple Silicon.

*Windows:* mở `docker-compose.yml`, sửa tay hai dòng `image:` của `minio` và `minio-init` thành hai image ở trên, rồi chạy `docker compose up -d --build`.

Kiểm tra lại các dòng image đã đổi:

```bash
grep -n "image:\|platform:" docker-compose.yml
```

Chạy lệnh `sed` hai lần liên tiếp không gây hại, nhưng với bản Apple Silicon, nếu chạy lệnh thêm `platform` hai lần thì dòng `platform` bị lặp và Docker sẽ báo lỗi file. Khi đó xóa bớt dòng thừa.

Nếu vẫn không tải được, dùng `cgr.dev/chainguard/minio:latest` cho server. Bản này không có `mc`, nên service `minio-init` không chạy và phải tạo bucket `lakehouse` bằng tay.

**MinIO báo `unhealthy`**

Kiểm tra bằng `docker compose ps` và `docker logs minio`. Nguyên nhân có thể là healthcheck dùng `curl` mà image mới không có curl.

**Cổng bị chiếm**

Tắt chương trình đang dùng cổng đó, hoặc đổi cổng bên trái trong phần `ports:` của `docker-compose.yml`.

## Nguồn

Data card, README, bài báo và bản chụp website chính thức của LaDe-D nằm trong `reports/data/sources`. Split nguồn là `delivery_yt`, metadata chính thức ghi 206,431 mẫu.