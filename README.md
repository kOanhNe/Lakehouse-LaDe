# Lakehouse LaDe-D: phân tích giao hàng và dự đoán ETA

Đây là đồ án xây dựng hệ thống Lakehouse cho dữ liệu giao hàng LaDe-D tại Yantai. Hệ thống đi từ dữ liệu CSV, audit chất lượng, ETL bằng Spark/Delta Lake, xây dựng kho dữ liệu star schema, huấn luyện các bài toán ETA, ghi bảng serving vào MySQL và hiển thị trên dashboard Streamlit.

Mở hướng dẫn notebook để đi từ CSV qua từng bước lọc đến một dự đoán ETA có đối chiếu tính tay. [Bắt đầu từ notebook](notebooks/README.md)

## Cấu trúc dự án

~~~text
.
├── config/                     # schema và cấu hình protocol
├── data/                       # CSV Yantai
├── docker/                     # Dockerfile, hadoop-conf và JAR
├── docs/                       # tài liệu hệ thống và đánh giá ETA
├── notebooks/                  # code xử lý theo thứ tự trình bày
├── pipeline/                   # flow Prefect và trạng thái lần chạy
├── dashboard/                  # dashboard Streamlit
├── reports/{data,eta,scd2}/    # kết quả kiểm kê và thí nghiệm
├── scripts/{checks,experiments}/
├── src/{eta,lakehouse}/         # module Python dùng chung
└── tests/
~~~

## 1. Bài toán

Mỗi dòng trong file dữ liệu tương ứng với một order_id. Câu hỏi chính là:

> Tại một thời điểm quan sát được trạng thái giao hàng, đơn còn bao nhiêu phút nữa sẽ hoàn tất?

Công thức nhãn ETA:

~~~text
target_remaining_minutes = delivery_ts - query_ts
~~~

| Bài toán | Vai trò | Cách diễn giải |
|---|---|---|
| Protocol P | ETA chính, tái lập LaDe | Tại lúc shipper vừa giao xong một đơn, dự đoán thời gian còn lại của các đơn đang cầm. Đánh giá ngoại tuyến trên cohort nhãn 1–120 phút, với các giới hạn leakage đã công bố trong [audit Protocol P](docs/eta/leak_inventory.md#audit-protocol-p). |
| Protocol B | Bài toán chính để đánh giá ETA vận hành | Dự đoán thời gian giao ngay khi nhận. Benchmark rolling-origin chấm trên cùng mã đơn và cùng cửa sổ cho các mô hình tại thời điểm nhận. |
| Spark LR/GBT | Baseline thời lượng giao hàng từ Fact | Dự đoán `delivery_minutes` tại lúc nhận. Lịch sử dùng sự kiện đã thấy từ Bronze. Chấm mọi đơn có nhãn hợp lệ từ ngày cắt và tách lát ≤24 giờ sau dự đoán. |

MAE khoảng **20,7 phút** của LightGBM thuộc Protocol P trên Yantai, trung bình qua ba seed. Chỉ xếp hạng các mô hình trong cùng bài toán và cùng tập đánh giá. Protocol B và Spark LR/GBT truy vấn lúc nhận đơn, còn Protocol P truy vấn sau một lần giao xong và dùng tập đánh giá riêng. [Metric Protocol P đã lưu](reports/eta/eta_paper_metrics.csv)

## 2. Dữ liệu đầu vào

File mặc định:

~~~text
data/delivery_yt.csv
~~~

Thông tin dataset hiện tại:

| Thuộc tính | Giá trị |
|---|---:|
| Số dòng | 206.431 |
| Số cột | 17 |
| Thành phố | Yantai |
| Shipper | 205 |
| Khu vực | 17 |
| AOI | khoảng 8.163 |
| SHA-256 | aa5bca7fdaa8c89d4ab50a2ed2b94fb09f608acc49fc1108a1a612e9c0f5a7ad |

Các cột chính gồm order_id, courier_id, region_id, city, aoi_id, aoi_type, lat, lng, accept_time, delivery_time, các cột GPS và ds. Quy tắc thời điểm biết của từng cột nằm trong config/schema.yaml.

delivery_minutes là thời gian từ lúc shipper nhận đơn đến lúc giao xong, không phải thời gian lái xe.

## 3. Kiến trúc hệ thống

~~~text
delivery_yt.csv
    |
    v
Bronze Delta: dữ liệu thô
    |
    v
Silver Delta: ép kiểu + timestamp + quality flags
    |
    +--> delivery_rejected
    +--> dim_courier_silver (SCD2)
    +--> dim_region_gold / dim_aoi_gold / dim_date_gold / dim_hour_gold
    +--> fact_delivery_task_gold
    +--> eta_safe_gold       (Protocol B, ETA lúc nhận)
    +--> eta_paper_gold      (Protocol P, bài toán chính)
    |
    v
MySQL serving --> Streamlit dashboard
~~~

| Service | Vai trò | Địa chỉ |
|---|---|---|
| MinIO | Object storage S3 cho CSV và Delta | localhost:19000, console localhost:19001 |
| Lakehouse (tuỳ chọn) | Hive Metastore và Spark Thrift Server | nội bộ |
| Jupyter | Chạy notebook và pipeline | http://localhost:8888 |
| Prefect | Điều phối flow | http://localhost:4200 |
| MySQL | Serving tables | localhost:3306 |
| Dashboard | Streamlit tiếng Việt | http://localhost:8501 |
| Metabase (tuỳ chọn) | Khám phá dữ liệu bổ sung | http://localhost:3000 |

Pipeline đọc và ghi Delta trực tiếp theo đường dẫn `s3a://`, không đăng ký bảng vào Hive Metastore và không gọi Spark Thrift Server. Vì vậy `lakehouse` và `metabase` là tuỳ chọn: tắt chúng không ảnh hưởng đến pipeline và dashboard. Dashboard đọc từ MySQL.

Sơ đồ và chi tiết các bảng nằm trong tài liệu star schema. [Star schema](docs/star_schema.md)

## 4. Các tầng dữ liệu

Bronze giữ 17 cột nguồn ở dạng chuỗi. Pipeline kiểm tra header, khóa `order_id` và đối soát khóa giữa CSV đã đọc và Delta Bronze sau khi ghi.

Silver ép kiểu, tính thời lượng, khoảng cách và cờ chất lượng. Timestamp không phân tích được hoặc thời lượng không dương đi vào `delivery_rejected` kèm lý do. Ngưỡng 24 giờ và 50 km là cờ chất lượng. Baseline Spark dùng Fact có nhãn hợp lệ và chỉ dùng ngưỡng 24 giờ để tách lát sau dự đoán. Sau khi ghi, pipeline đọc lại Silver và Rejected để kiểm tra mỗi khóa Bronze xuất hiện đúng một lần trong hai bảng và kiểm tra điều kiện của từng bảng.

Gold gồm `fact_delivery_task_gold` với grain một dòng cho một đơn, các dimension region, AOI, date, hour và lịch sử courier SCD2. Pipeline kiểm tra khóa ngoại và đối soát khóa giữa Silver với fact đã ghi. Các đối soát dùng dữ liệu đọc thực tế.

## 5. Protocol P

Protocol P dùng query time là lúc shipper vừa hoàn tất một đơn. Các đơn mục tiêu thỏa điều kiện:

~~~text
accept_ts < query_ts < delivery_ts
~~~

Nhãn:

~~~text
target_remaining_minutes = delivery_ts - query_ts
~~~

Quy trình hiện tại:

- giữ nhãn từ 1 đến 120 phút;
- cần ít nhất hai đơn đang còn cầm;
- cửa sổ tối đa 24 đơn;
- tối đa 12 query mỗi cửa sổ, với bước trượt 12 đơn;
- query được chọn bằng quy tắc xác định;
- chia theo ngày theo mốc 60/20/20, bỏ hai ngày tại ranh giới theo cách lấy mẫu hiện tại;
- chạy baseline median, SPEED và LightGBM với seed 42, 43, 44.

Kết quả ghi vào `eta_paper_gold` và `eta_paper_metrics_gold`. Đây là kết quả tái lập ngoại tuyến trên cohort 1–120 phút. `spd` dùng cả file, còn `ntodo` và `rank` phụ thuộc nhãn tương lai của các đơn trong cửa sổ. Xem [audit Protocol P](docs/eta/leak_inventory.md#audit-protocol-p) để biết phạm vi diễn giải.

Protocol B ghi vào `eta_safe_gold` và `eta_safe_metrics_gold`. Prefect dùng B + AOI + GPS với hàng chờ và lịch sử gần đây. Bước này chọn nhãn fit và hiệu chỉnh theo thời điểm đã giao rồi công bố điểm trên cửa sổ validation cuối. Dashboard ghi mã biến thể, ngày chấm, MAE, WAPE và ACC@30/60. [Notebook tính tay](notebooks/06_eta_accept.ipynb)

Trên 24.647 đơn validation Yantai ở ba cửa sổ liên tiếp, thêm hàng chờ và lịch sử gần đây đưa MAE trung bình qua ba seed từ **109,82** xuống **104,16 phút** so với bộ B + AOI + GPS dựng từ sự kiện. Mức giảm so với cấu hình lịch sử 109,52 là **5,36 phút**. ACC@30 là **31,88%**. Cả ba cửa sổ thuộc tập phát triển đã dùng để chọn đặc trưng. Đánh giá độc lập cần giai đoạn chưa dùng trong quá trình chọn mô hình. [Thiết kế và kết quả](docs/eta/training_data_lineage.md#35-so-sánh-rolling-origin-trên-cùng-mã-đơn)

Trong benchmark seed 42, Spark GBT và LightGBM cùng dùng bộ đặc trưng hàng chờ và lịch sử. GBT dừng sớm giữ một cây và đạt MAE 127,41 phút. GBT hiệu chỉnh với 80 cây, step size 0,10 và độ sâu 6 đạt 126,39 phút. LightGBM đạt 103,71 phút. Baseline Spark trong notebook 04 dùng tám cột số cùng AOI và vùng, với 40 cây, step size 0,10 và độ sâu 5. MAE của baseline này lấy từ `ml_metrics` theo từng lần chạy Prefect. [Đối chiếu cấu hình và tập chấm](docs/eta/training_data_lineage.md#đối-chiếu-lightgbm-và-spark)

Cách biến CSV Yantai thành các bảng và tập huấn luyện của từng nhánh nằm trong tài liệu nguồn dữ liệu. [Nguồn dữ liệu huấn luyện](docs/eta/training_data_lineage.md)

## 6. Cài đặt

Yêu cầu:

- Docker Desktop và Docker Compose v2;
- Git;
- PowerShell trên Windows;
- 6-8 GB RAM cấp cho Docker (chế độ đầy đủ), hoặc khoảng 4 GB với chế độ tiết kiệm RAM ở mục 6.2;
- khoảng 10 GB dung lượng trống;
- Internet ở lần build đầu.

Kiểm tra:

~~~powershell
docker --version
docker compose version
git --version
~~~

Clone project:

~~~powershell
git clone https://github.com/kOanhNe/Lakehouse-LaDe.git
cd Lakehouse-LaDe
~~~

Nếu project đã có trên máy:

~~~powershell
git switch main
git pull --ff-only origin main
~~~

Kiểm tra dữ liệu:

~~~powershell
Test-Path .\data\delivery_yt.csv
Get-FileHash .\data\delivery_yt.csv -Algorithm SHA256
~~~

Tạo cấu hình local:

~~~powershell
Copy-Item .env.example .env
~~~

Có thể giữ giá trị mặc định trong .env để chạy local. Không commit .env lên GitHub.

Trên macOS hoặc Linux, các lệnh tương đương với ba bước kiểm tra và tạo cấu hình ở trên:

~~~bash
ls -l data/delivery_yt.csv
shasum -a 256 data/delivery_yt.csv
cp .env.example .env
~~~

Chỉ khi build service `lakehouse` (chế độ đầy đủ, mục 6.1) thì docker/lakehouse.Dockerfile mới cần ba gói nén đặt ngay trong thư mục gốc của repo. Chế độ tiết kiệm RAM (mục 6.2) không build `lakehouse` nên không cần ba file này. Tải đúng phiên bản từ Apache Archive và giữ nguyên tên file.

- [Hadoop 3.3.6](https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz) (`hadoop-3.3.6.tar.gz`)
- [Hive 2.3.10](https://archive.apache.org/dist/hive/hive-2.3.10/apache-hive-2.3.10-bin.tar.gz) (`apache-hive-2.3.10-bin.tar.gz`)
- [Spark 3.5.3 cho Hadoop 3](https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz) (`spark-3.5.3-bin-hadoop3.tgz`)

### 6.1 Chế độ đầy đủ (Docker từ 6-8 GB RAM)

~~~powershell
docker compose up -d --build jupyter dashboard
docker compose ps
~~~

Chờ lakehouse, jupyter, minio, mysql và prefect ở trạng thái Up/healthy.
Nếu cần Metabase, chạy thêm `docker compose up -d metabase`.

### 6.2 Chế độ tiết kiệm RAM (Docker khoảng 4 GB)

Chế độ này không bật `lakehouse` (Hive Metastore và Spark Thrift) và `metabase`, là hai service tuỳ chọn và tốn nhiều RAM. Pipeline vẫn chạy đủ các bước, cùng dữ liệu và cùng các bảng.

**Bước 1. Chỉnh `docker-compose.yml`** (bỏ qua nếu file đã có sẵn các dòng này).

Service `mysql`, thêm dòng `command` dưới `image`:

~~~yaml
  mysql:
    image: mysql:8.4
    command: --performance-schema=OFF --innodb-buffer-pool-size=64M --max-connections=50
~~~

Service `jupyter`, trong `PYSPARK_SUBMIT_ARGS` thêm ba dòng (giữ nguyên các dòng còn lại):

~~~yaml
      PYSPARK_SUBMIT_ARGS: >-
        --master local[2]
        --driver-memory 1500m
        --conf spark.sql.execution.arrow.pyspark.enabled=true
        --conf spark.sql.shuffle.partitions=8
        --conf spark.jars.ivy=/tmp/lakehouse-ivy
        --packages ...
        pyspark-shell
~~~

`--driver-memory 1500m` cấp bộ nhớ cho Spark driver. Tuỳ chọn Arrow giúp đổi bảng Spark sang pandas (bước 4b, 4c) tốn ít bộ nhớ hơn; nếu thiếu `pyarrow` Spark tự quay về cách cũ. `shuffle.partitions=8` giảm số phân vùng mặc định (200) cho phù hợp dữ liệu nhỏ.

**Bước 2. Tắt các service đang chạy mà không cần** (bỏ qua nếu chưa bật):

~~~bash
docker compose stop lakehouse metabase dashboard
~~~

**Bước 3. Khởi động đúng các service cần cho pipeline:**

~~~bash
docker compose up -d minio minio-init
docker compose up -d --build --no-deps mysql prefect jupyter
docker compose ps
~~~

- Lệnh đầu bật MinIO và tạo bucket `lakehouse` (chạy lại nhiều lần vẫn an toàn). Cần chạy ít nhất một lần trên máy mới hoặc sau `docker compose down -v`, vì bước này vốn chỉ chạy khi `lakehouse` được bật.
- `--no-deps` nghĩa là không tự bật các service mà service đó phụ thuộc (`depends_on`). Nếu thiếu cờ này, `jupyter` sẽ kéo `lakehouse` lên và làm Docker hết RAM.
- Chờ `mysql`, `minio` và `prefect` ở trạng thái healthy rồi mới chạy flow (mục 7).

Trong chế độ này không dùng `docker compose up -d` không kèm tên service, vì nó bật cả `lakehouse` và `metabase`.

## 7. Chạy toàn bộ pipeline

Cách khuyến nghị:

~~~powershell
docker compose exec jupyter python -u -m pipeline.flow
~~~

Flow chạy theo thứ tự:

~~~text
Bronze -> Silver -> courier SCD2 -> star dimensions
      -> ETA-safe 4b -> Protocol P 4c
      -> fact/baseline 4 -> serving MySQL
~~~

Theo dõi terminal hoặc mở Prefect tại http://localhost:4200. Flow ưu tiên CSV trên MinIO nếu có, rồi dùng file local được mount nếu object không tồn tại. Log Bronze ghi URI thực tế.

MySQL lưu từng lần thử trong `lakehouse_pipeline_runs` cùng mã run, Prefect flow ID, bước hiện tại và thời gian UTC. Khóa MySQL giới hạn một flow ghi dữ liệu tại một thời điểm. Trạng thái `COMPLETED` được ghi sau khi các lượt ghi và đối soát hoàn tất. `FAILED` ghi nhận lỗi trong flow. Lần chạy `RUNNING` mất phiên giữ khóa được nhận diện là `INTERRUPTED`, rồi được cập nhật vào nhật ký khi lần kế tiếp bắt đầu.

Với chế độ 6.2, chạy flow khi `dashboard` đang tắt (để dành RAM). Khi Prefect báo xong, bật dashboard:

~~~bash
docker compose up -d --build --no-deps dashboard
~~~

Khi lỗi, xem bước cuối trong Prefect và chạy lại flow từ đầu. Chạy lại dùng overwrite theo batch. Delta và MySQL có thể chứa dữ liệu ghi dở giữa hai lần chạy, vì mỗi bảng được cập nhật riêng.

## 8. Chạy notebook đúng thứ tự

Hướng dẫn notebook có một bảng đường dẫn đầu vào, điều kiện lọc và kết quả của từng bước theo luồng Prefect. [Thứ tự chạy notebook](notebooks/README.md)

Notebook 06 kiểm tra một đơn thật từ `data/delivery_yt.csv`. Chạy các cell để tính nhãn và đặc trưng, xem các nhánh cây LightGBM rồi đối chiếu phép cộng với dự đoán của máy. [Đối chiếu tính tay](notebooks/06_eta_accept.ipynb)

## 9. Dashboard và kết quả

Sau khi flow Prefect hoàn tất, mở http://localhost:8501 và bấm làm mới nếu trang đang mở.

Dashboard chỉ hiển thị biểu đồ khi lần mới nhất là `COMPLETED` và tất cả bảng dashboard có cùng `pipeline_run_id`. Lần đang chạy, thất bại, gián đoạn hoặc có bảng lẫn mã sẽ hiển thị trạng thái thay cho biểu đồ. Sau khi nâng cấp từ phiên bản chưa có nhật ký, chạy flow một lần để tạo dữ liệu có mã. Truy vấn MySQL trực tiếp và Metabase đọc bảng tại thời điểm truy vấn.

Các Delta table chính:

~~~text
delivery_bronze
delivery_silver
delivery_rejected
dim_courier_silver
dim_region_gold
dim_aoi_gold
dim_date_gold
dim_hour_gold
fact_delivery_task_gold
eta_safe_gold
eta_safe_metrics_gold
eta_paper_gold
eta_paper_metrics_gold
~~~

Kiểm tra MySQL:

~~~powershell
docker compose exec mysql mysql -udelivery -pdelivery delivery_source -e "SHOW TABLES;"
~~~

## 10. Kiểm tra và test

Để chạy ngoài Docker, cài repo dưới dạng editable trong môi trường ảo. Lệnh lấy danh sách thư viện từ `requirements.txt`.

~~~powershell
py -3.12 -m venv .venv
.venv\Scripts\python.exe -m pip install -e .
.venv\Scripts\python.exe -m scripts.checks.validate_schema
.venv\Scripts\python.exe -m pytest -m "not integration" -q
~~~

Sau khi khởi động các dịch vụ theo mục 6, chạy unit test Spark rồi chạy integration trong Jupyter.

~~~powershell
docker compose exec -T jupyter python -m pytest -m "not integration" -q -p no:cacheprovider
docker compose exec -T jupyter python -m pytest -m integration -q -p no:cacheprovider
~~~

Unit test Spark cần PySpark và JVM, có sẵn trong container Jupyter. Integration dùng Spark/Delta, MinIO, MySQL và API Prefect thật với CSV mẫu và các bảng có tiền tố riêng. Bộ test ghi dữ liệu, chạy lại sau một lỗi ghi MySQL được mô phỏng và kiểm tra cách phát hiện khóa bị mất. Xem [trạng thái triển khai](docs/implementation_status.md) để biết kết quả và giới hạn.

Chạy Protocol P ngoài Docker:

~~~powershell
.venv\Scripts\python.exe -m scripts.experiments.run_paper_protocol --source data/delivery_yt.csv
~~~

Phân tích độ nhạy nhãn:

~~~powershell
.venv\Scripts\python.exe -m scripts.experiments.run_label_sensitivity
~~~

## 11. Lỗi thường gặp

Jupyter yêu cầu token: lấy token mới bằng docker compose exec jupyter jupyter server list.

JAVA_GATEWAY_EXITED: Docker thiếu RAM hoặc Spark chưa khởi động xong. Chờ service healthy rồi chạy docker compose restart jupyter.

Py4JNetworkError, `Connection refused` hoặc `SparkSession$ does not exist in the JVM`: tiến trình Spark đã chết, thường do hết RAM ở bước 4b hoặc 4c (đổi Bronze sang pandas). Dùng chế độ tiết kiệm RAM (mục 6.2), tắt `lakehouse`, `metabase` và `dashboard` trong lúc chạy flow, rồi chạy lại flow từ đầu. Nếu vẫn lỗi, tăng RAM cho Docker.

`NoSuchBucket` hoặc không tìm thấy bucket `lakehouse`: chạy `docker compose up -d minio minio-init` để tạo bucket.

PATH_NOT_FOUND khi đọc Delta: chạy lại từ notebook tạo bảng upstream.

pandas dtypes must be int, float or bool: aoi: image cũ chưa chuẩn hóa aoi_type. Chạy docker compose up -d --build jupyter.

Spark WindowExec No Partition Defined: đây là cảnh báo hiệu năng; xem task đang chạy và chờ flow hoàn tất trước khi kết luận lỗi.

Khi dashboard báo thiếu hoặc lẫn lần chạy, kiểm tra nhật ký `lakehouse_pipeline_runs`, chạy lại flow và bấm làm mới sau khi có `COMPLETED`.

Dừng service nhưng giữ volume:

~~~powershell
docker compose down
~~~

Xóa toàn bộ volume local:

~~~powershell
docker compose down -v
~~~

## 12. Tài liệu tham khảo và giới hạn

Dataset hiện chỉ đại diện cho Yantai, không được suy rộng kết quả sang thành phố khác. Protocol P dùng toPandas cho khoảng 206 nghìn dòng, phù hợp phạm vi đồ án hiện tại nhưng chưa phải thiết kế huấn luyện phân tán cho dữ liệu rất lớn.

Nguồn tham khảo:

- LaDe repository: https://github.com/wenhaomin/LaDe
- LaDe dataset: https://huggingface.co/datasets/Cainiao-AI/LaDe
- Data dictionary: docs/data_dictionary.md
- Schema và allowlist: config/schema.yaml
- Phân tích giới hạn: docs/eta/analysis_limits.md
