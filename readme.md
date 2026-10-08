
# 🚀 Local Data Lakehouse Project: End-to-End Delivery Analytics

Dự án này triển khai một hệ thống **Data Lakehouse** hoàn chỉnh trên môi trường local sử dụng Docker. Dự án mô phỏng lại một vòng đời dữ liệu (Data Pipeline) thực tế: từ việc thu thập dữ liệu thô, xử lý làm sạch, xây dựng mô hình Machine Learning, cho đến việc phục vụ dữ liệu trực quan hóa trên Dashboard.

## 🏗 Kiến trúc hệ thống & Công nghệ

Hệ thống tuân thủ chặt chẽ kiến trúc **Medallion (Bronze -> Silver -> Gold)** với các công nghệ Big Data hiện đại:

* **Storage Layer:** MinIO (S3-compatible object storage).
* **Table Format:** Delta Lake (version 3.1.0/3.3.2).
* **Processing Layer:** Apache Spark (version 3.5.3) với PySpark.
* **Serving Layer:** MySQL 8.4 (Lưu trữ dữ liệu tổng hợp để query tốc độ cao).
* **BI & Dashboarding:** Metabase (Trực quan hóa dữ liệu).
* **Workspace:** Jupyter Notebook.

## 📂 Cấu trúc thư mục chuẩn

```text
lakehouse-project/
├── docker-compose.yml          # Cấu hình mạng lưới các container (Lakehouse, MinIO, MySQL, Jupyter, Metabase)
├── Dockerfile                  # Script build image chứa Java, Hadoop, Hive, và Spark
├── docker/                     # Chứa các tài nguyên phục vụ việc build Docker image
│   ├── config/                 # File cấu hình phân tán của hệ thống (core-site, hadoop-env, spark-defaults...)
│   ├── jars/                   # Thư viện bổ sung (AWS S3, Delta Lake, MySQL Connector)
│   └── start.sh                # Script khởi động tự động tạo MinIO bucket
├── notebooks/                  # Thư mục làm việc của Jupyter (được mount vào container)
│   ├── 01_ingest_bronze.ipynb        # Ingest dữ liệu thô (CSV -> Delta Bronze)
│   ├── 02_transform_silver.ipynb     # Làm sạch, tính khoảng cách (Haversine) & thời gian giao hàng
│   ├── 03_dim_courier_silver.ipynb   # Trích xuất bảng Dimension với lịch sử thay đổi (SCD Type 2)
│   ├── 04_fact_and_ml_gold.ipynb     # Point-in-time Join tạo bảng Fact & Train ML dự báo ETA
│   └── 05_serving_layer.ipynb        # Tổng hợp Data (Aggregations) và đẩy sang MySQL
└── logs/                       # Log hệ thống mount từ container ra để dễ dàng debug

```

## ⚙️ Yêu cầu cài đặt (Prerequisites)

Để chạy dự án này, máy tính của bạn cần cài đặt sẵn:

1. **Docker Desktop** (hoặc Docker Engine).
2. **Docker Compose**.
*(Dự án tương thích với Windows (qua WSL2), macOS, và Linux).*

---

## 🚀 Hướng dẫn khởi chạy hệ thống

### Bước 1: Khởi động mạng lưới Container

Mở Terminal/Command Prompt tại thư mục gốc của dự án (`lakehouse-project/`) và chạy:

```bash
docker-compose up -d --build

```

*(Lưu ý: Lần chạy đầu tiên sẽ mất khoảng 10-15 phút để Docker tải image và build hệ thống Spark/Hadoop).*

### Bước 2: Kiểm tra trạng thái

Chạy lệnh `docker ps` để đảm bảo 5 services đang hoạt động: `lakehouse`, `minio`, `mysql`, `jupyter`, và `metabase`.

### Bước 3: Chuẩn bị dữ liệu thô

1. Truy cập **MinIO Console:** [http://localhost:9001](http://localhost:9001) (User/Pass: `minioadmin` / `minioadmin`).
2. Vào phần **Buckets**, bạn sẽ thấy bucket `lakehouse` đã được tạo tự động.
3. Upload file dữ liệu `delivery_yt.csv` của bạn trực tiếp vào thư mục gốc của bucket `lakehouse`.

---

## 🌐 Các cổng truy cập (Access Points)

Khi hệ thống đã sẵn sàng, bạn có thể truy cập các giao diện sau qua trình duyệt:

| Dịch vụ | URL Truy cập | Thông tin Đăng nhập (nếu có) |
| --- | --- | --- |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **Jupyter Notebook** | [http://localhost:8888](http://localhost:8888) | *Xem hướng dẫn lấy Token bên dưới* |
| **Metabase (BI)** | [http://localhost:3000](http://localhost:3000) | Tự tạo tài khoản ở lần truy cập đầu |
| **Spark UI** | [http://localhost:4040](http://localhost:4040) | *(Chỉ khả dụng khi đang chạy code Spark)* |

**🔑 Cách lấy Token cho Jupyter Notebook:**
Nếu Jupyter yêu cầu Password/Token, hãy mở Terminal và chạy lệnh:

```bash
docker logs jupyter

```

Tìm dòng có chứa URL dạng `[http://127.0.0.1:8888/lab?token=abc123xyz](http://127.0.0.1:8888/lab?token=abc123xyz)...`. Copy toàn bộ URL này dán vào trình duyệt để truy cập.

---

## 🛠 Thực thi Data Pipeline (Medallion Architecture)

Mở **Jupyter Notebook** và chạy tuần tự các file sau để luân chuyển dữ liệu từ thô đến tinh chế:

1. 🥉 **`01_ingest_bronze.ipynb`**: Đọc dữ liệu CSV từ bucket MinIO và ghi định dạng Delta Lake nguyên bản vào lớp Bronze.
2. 🥈 **`02_transform_silver.ipynb`**: Làm sạch dữ liệu, xử lý Null tọa độ, chuẩn hóa Timestamp, và tính toán *Khoảng cách địa lý (Haversine)* & *Thời gian giao hàng thực tế*.
3. 🥈 **`03_dim_courier_silver.ipynb`**: Trích xuất danh sách tài xế (Couriers) và tạo bảng Dimension theo dõi lịch sử thay đổi khu vực hoạt động bằng kỹ thuật **Slowly Changing Dimension (SCD) Type 2**.
4. 🥇 **`04_fact_and_ml_gold.ipynb`**:
* Sử dụng **Point-in-time Join** để nối dữ liệu giao hàng với đúng thông tin lịch sử của tài xế tại thời điểm phát sinh đơn.
* Huấn luyện mô hình **Machine Learning (Linear Regression)** để dự báo thời gian giao hàng (ETA) dựa trên khoảng cách.


5. 🚀 **`05_serving_layer.ipynb`**: Truy vấn bảng Fact Gold, tính toán các chỉ số tổng hợp (Metrics) như *Tổng đơn, Khoảng cách TB, Thời gian TB* theo từng khu vực/tài xế và xuất (push) thẳng vào **MySQL** để phục vụ Dashboard.

---

## 📊 Xây dựng Dashboard với Metabase

Sau khi chạy xong file `05_serving_layer.ipynb`, dữ liệu tổng hợp đã nằm sẵn trong MySQL. Thực hiện các bước sau để trực quan hóa:

### 1. Kết nối Database

1. Truy cập **[http://localhost:3000](http://localhost:3000)** và hoàn tất các bước thiết lập tài khoản ban đầu (có thể skip bước add email).
2. Khi được hỏi *"Add your data"*, chọn **MySQL**.
3. Nhập cấu hình kết nối như sau:
* **Host:** `mysql` *(Sử dụng tên container thay vì localhost)*
* **Port:** `3306`
* **Database Name:** `delivery_source`
* **Username:** `root`
* **Password:** `root`


4. Bấm **Save** để Metabase quét dữ liệu.

### 2. Tạo Dashboard

Vào mục **Browse data -> delivery_source**, bạn sẽ thấy bảng `agg_courier_performance`. Từ đây, bạn có thể tạo các biểu đồ (Questions) và gom vào một Dashboard hoàn chỉnh.

**Gợi ý các biểu đồ (Metrics):**

* 🏆 **Top 10 Couriers:** Biểu đồ cột (Bar chart) thể hiện `total_orders` theo `courier_id`.
* ⏱️ **Courier Performance:** Biểu đồ phân tán (Scatter plot) so sánh giữa `avg_distance_km` và `avg_delivery_minutes`.
* 📍 **Region Analytics:** Gom nhóm (Group by) theo `region_id` để xem khu vực nào có thời gian giao hàng cao nhất (bottleneck).

---

## 🛑 Dừng và Xóa hệ thống

* Để tắt hệ thống nhưng **GIỮ LẠI** toàn bộ dữ liệu (đã mount volumes):
```bash
docker-compose stop

```


* Để tắt và **XÓA SẠCH** toàn bộ dữ liệu (Reset hệ thống về trạng thái ban đầu):
```bash
docker-compose down -v

```
