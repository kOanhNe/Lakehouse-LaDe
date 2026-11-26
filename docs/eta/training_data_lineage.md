# Nguồn dữ liệu và cách tạo tập huấn luyện LaDe-D

Với bộ Yantai của repo, cả ba nhánh bắt đầu từ cùng file **`data/delivery_yt.csv`**.
Khi pipeline chạy, mỗi nhánh dựng bảng học riêng. **Đặc trưng** là các cột mô hình
đọc để dự đoán. **Nhãn** là số phút thực tế dùng để học hoặc chấm dự đoán.

| Nhánh huấn luyện | Đọc trực tiếp từ đâu | Một dòng trong tập học | Mốc truy vấn và nhãn |
| --- | --- | --- | --- |
| **Protocol P · LightGBM** | Delta `delivery_bronze` | Một cặp (lần truy vấn, đơn còn cầm) | Truy vấn khi shipper vừa giao một đơn. Nhãn là số phút còn lại để giao đơn mục tiêu, giữ cặp có nhãn trong (0, 120]. |
| **Protocol B · LightGBM** | Delta `delivery_bronze` | Một đơn tại lúc nhận, sau khi lấy mẫu tối đa 12 đơn mỗi shipper mỗi ngày | Truy vấn khi nhận đơn. Nhãn là toàn bộ thời gian từ lúc nhận đến lúc giao. |
| **Spark LR/GBT** | Bronze để dựng đặc trưng và Delta `fact_delivery_task_gold` để lấy nhãn | Một đơn Fact có nhãn hợp lệ | Truy vấn khi nhận đơn. Nhãn `delivery_minutes` tính bằng phút. |

Ví dụ, một đơn được nhận lúc **08:00** và giao lúc **10:10** có nhãn **130 phút** trong Protocol B và Spark nếu đủ điều kiện vào tập. Nếu shipper giao một đơn khác lúc **09:00**, Protocol P có thể tạo một mẫu cho đơn còn cầm đó với nhãn **70 phút**. Một đơn có thể xuất hiện trong nhiều cặp truy vấn của Protocol P.

```mermaid
flowchart LR
    CSV["CSV LaDe-D Yantai"] --> Audit["Audit chất lượng · reports/data"]
    CSV --> Bronze["Bronze · dữ liệu nguồn"]
    Bronze --> Silver["Silver · đơn hợp lệ và cột phái sinh"]
    Bronze --> Rejected["Rejected · đơn bị loại và lý do"]
    Silver --> Dims["Dimension · shipper, khu, AOI, ngày, giờ"]
    Silver --> Fact["Gold fact · một dòng mỗi đơn"]
    Dims --> Fact
    Bronze --> P["Protocol P · cặp truy vấn–đơn"]
    Bronze --> B["Protocol B · mẫu tại lúc nhận"]
    Fact --> Spark["Spark LR/GBT · cohort ML"]
    P --> Results["Dự đoán và metric"]
    B --> Results
    Spark --> Results
    Results -->|"Metric và mẫu dự đoán Spark"| Serving["MySQL serving"]
    Fact --> Serving
    Dims --> Serving
    Serving --> Dashboard["Dashboard"]
```

## 1. Dữ liệu gốc và bước audit

Bản CSV trong repo là tập **giao hàng LaDe-D của Yantai**, gồm **206.431 đơn,
17 cột**, với `order_id` duy nhất. Các nhóm cột gồm mã đơn, shipper, vùng và
AOI, tọa độ điểm giao `lat/lng`, thời điểm nhận/giao, GPS quanh hai thời điểm
đó và mã ngày `ds`. CSV ghi giờ theo dạng `MM-DD HH:MM:SS`. Spark dựng
timestamp với năm mặc định 2022 và múi giờ `Asia/Shanghai`. Pandas trong
Protocol B cũng dùng năm 2022 để dựng timestamp từ chuỗi.
[CSV nguồn](../../data/delivery_yt.csv) · [Hợp đồng dữ liệu](../../config/schema.yaml)

Data card mô tả `ds` là ngày giao. Trên bản CSV Yantai đang dùng, `ds` trùng
ngày nhận ở **206.431/206.431** dòng và trùng ngày giao ở **203.444/206.431**
dòng. Protocol P lấy `ds` nguyên gốc để nhóm và chia ngày. Protocol B và Spark
lấy ngày từ thời điểm nhận đã parse.
[Data card LaDe-D lưu trong repo](https://huggingface.co/datasets/Cainiao-AI/LaDe-D) · [Thống kê audit](../../reports/data/summary.json)

Notebook **00** gọi `scripts/checks/profile_delivery.py` trên CSV để thống kê dữ liệu thiếu, ngày nhận/giao, GPS bất thường, thời lượng và khoảng cách. Đầu ra là `reports/data/summary.json`, `row_quality.csv.gz`, các bảng thống kê và biểu đồ. Luồng audit dừng ở báo cáo. Ba nhánh huấn luyện lấy dữ liệu từ Bronze hoặc fact như sơ đồ trên. [Script audit](../../scripts/checks/profile_delivery.py) · [Chi tiết audit](../00_data_audit.md)

## 2. Từ CSV tới Bronze, Silver và fact

| Khâu | Đầu vào | Cách tạo và đầu ra |
| --- | --- | --- |
| **01 · Bronze** | Spark đọc URI `SOURCE_PATH` nếu được cấu hình, mặc định là `s3a://lakehouse/delivery_yt.csv`. Khi đường dẫn này không tồn tại, code dùng `LOCAL_SOURCE`, mặc định là CSV repo được mount tại `/home/jovyan/work/data/delivery_yt.csv`. | Đọc 17 cột dưới dạng chuỗi, kiểm tra header và `order_id` có giá trị, duy nhất. Ghi và đối soát khóa trong `delivery_bronze`, **206.431 dòng**. Log Bronze ghi URI thực tế đã đọc. |
| **02 · Silver và Rejected** | `delivery_bronze` | Parse thời điểm nhận/giao, ép kiểu tọa độ và mã, tính `delivery_minutes`, số ngày chênh và khoảng cách Haversine. Đơn có timestamp hợp lệ và thời lượng dương vào `delivery_silver` (**206.062 dòng**). Các dòng còn lại vào `delivery_rejected` kèm `reject_reason` (**369 dòng**, đều có thời lượng bằng 0 trên CSV này). |
| **03/03b · Dimension** | `delivery_silver` | Dựng lịch sử khu vực của shipper và các bảng tra cứu khu, AOI, ngày, giờ. |
| **04 · Gold fact** | Silver và năm bảng dimension | Gắn khóa shipper có hiệu lực tại ngày nhận, cùng khóa vùng, AOI, ngày nhận, ngày giao và giờ nhận. `fact_delivery_task_gold` giữ **206.062 dòng**, một dòng mỗi đơn Silver. |

Silver gắn cờ GPS, khoảng cách quá 50 km và thời lượng quá 24 giờ. Những đơn có timestamp hợp lệ và thời lượng dương được giữ trong Silver và fact. Báo cáo Spark thêm lát **≤24 giờ** sau dự đoán. Các bảng Delta nằm dưới `s3a://lakehouse/warehouse/`. [Code lọc Silver](../../notebooks/02_transform_silver.ipynb) · [Code dựng fact và tập Spark](../../notebooks/04_fact.ipynb)

### Năm bảng dimension được dựng thế nào

| Bảng | Cách tạo từ Silver |
| --- | --- |
| `dim_courier_silver` | Theo từng shipper và ngày nhận, lấy vùng có nhiều đơn nhất. Nếu hòa, lấy `region_id` nhỏ hơn. Tạo một phiên bản SCD2 khi vùng chính đổi. Fact nối bằng khoảng hiệu lực chứa ngày nhận. |
| `dim_region_gold` | Một dòng mỗi vùng. Từ tọa độ điểm giao hợp lệ, tính tâm, giới hạn tọa độ và số AOI quan sát được. |
| `dim_aoi_gold` | Một dòng mỗi AOI, gồm loại AOI và tâm của các điểm giao hợp lệ. Code kiểm tra mỗi mã AOI gắn với tối đa một loại. |
| `dim_date_gold` | Sinh một dòng cho mỗi ngày từ ngày sớm nhất đến ngày muộn nhất trong các sự kiện nhận/giao của Silver. |
| `dim_hour_gold` | Sinh 24 dòng cho các giờ 0–23 và gán nhóm buổi trong ngày. |

Luồng Prefect tương ứng với các notebook **01 → 02 → 03 → 06 → 07 → 04 → 05**. Notebook 03 gộp hai task dựng dimension. B và P đọc Bronze. Fact đọc Silver và dimension. [Hàm dựng dimension và nối fact](../../src/lakehouse/common.py) · [Thứ tự Prefect](../../pipeline/flow.py) · [Thứ tự trình bày notebook](../../notebooks/README.md)

## 3. Ba nhánh tạo tập học

### 3.1. Protocol P · ETA còn lại sau một lần giao

1. `build_eta_paper()` lấy bảy cột từ Bronze gồm `courier_id`, `ds`, `lat`, `lng`, `aoi_type`, `accept_time` và `delivery_time`. Spark chuyển bảng này sang pandas để dựng tập mẫu và huấn luyện LightGBM.
2. `clean()` loại trùng trên các cột đã chọn, đổi giờ sang phút trong ngày và sắp theo `ds`, shipper, thời điểm giao. Code trừ 1.440 phút ở thời điểm nhận khi ngày nhận khác ngày giao, theo quy ước của phép tái lập. Bộ lọc bỏ một điểm nếu bước tới điểm đó và bước kế tiếp đều vượt 500 m/phút. Code còn bỏ lượt lặp vị trí sau điểm đầu khi chênh lệch phút nhỏ hơn 1. Bản Yantai còn **206.063 dòng quỹ đạo** sau bước này.
3. Trong mỗi nhóm `(courier_id, ds)` đã sắp thứ tự giao, code duyệt cửa sổ tối đa 24 đơn, bước trượt 12 và tối đa 12 truy vấn hợp lệ mỗi cửa sổ. Ứng viên là các đơn đứng sau trong thứ tự giao đã nhận trước mốc truy vấn. Nhãn `target_remaining_minutes` là phút giao của ứng viên trừ phút giao của đơn làm mốc. Code giữ nhãn `0 < nhãn <= 120` và cần ít nhất hai ứng viên. Train/validation cắt ứng viên về trong cửa sổ. Test bỏ cả cửa sổ nếu có ứng viên đạt ngưỡng nhãn nhưng nằm ngoài cửa sổ.
4. Mỗi mẫu có tám đặc trưng. `dis` là khoảng cách giữa hai điểm giao tính bằng mét, `elapsed` là thời gian ứng viên đã chờ từ lúc nhận và `tmin` là phút trong ngày của truy vấn. `ndone`, `ntodo` và `rank` lần lượt là vị trí trên quỹ đạo, số ứng viên đã lọc và hạng khoảng cách. `aoi` lấy từ loại AOI của đơn mục tiêu, còn `spd` là tốc độ shipper tính trên cả file, theo m/phút.
5. Chia theo thứ tự ngày của `ds` ở mốc 60% và 80%, bỏ hai ngày tại ranh giới theo code tái lập. Tập train dùng để fit LightGBM và tính median. Validation dùng để dừng sớm LightGBM. Tập test dùng để chấm LightGBM, median và SPEED với công thức khoảng cách chia tốc độ.

Các cửa sổ chồng lấn và nhiều đơn cho mỗi truy vấn giải thích vì sao số **cặp** lớn hơn số **đơn** CSV. Protocol P chọn cohort theo nhãn và có đặc trưng phụ thuộc dữ liệu tương lai, nên metric mô tả đánh giá ngoại tuyến trên cohort này. [Builder và cách tính metric Protocol P](../../src/eta/paper_protocol.py) · [Audit leakage](leak_inventory.md#audit-protocol-p) · [Đo độ nhạy theo cohort](leak_measurement.md)

### 3.2. Protocol B · ETA tại lúc nhận đơn

Mô tả dưới đây là Protocol B gốc dùng trong nghiên cứu đăng ký. Cấu hình
Prefect hiện tại dùng cùng mã đơn và dựng trạng thái theo sự kiện đã quan sát
như mục 3.5.

1. `build_eta_safe()` tải toàn bộ Bronze sang pandas. `parse_lade_frame()` ép kiểu các cột và dựng timestamp. `build_eta_samples()` giữ đơn có timestamp nhận/giao hợp lệ và thời lượng dương. Trên Yantai bước này có **206.062 đơn** trước khi lấy mẫu.
2. Với mỗi đơn, `query_ts = accept_ts`. Nhãn `target_remaining_minutes` bằng `delivery_minutes` của đơn. Code dựng lịch sử theo từng shipper từ các đơn đủ điều kiện. Số đơn đã giao, thời lượng giao trung bình và GPS của đơn giao gần nhất lấy từ các lần giao có `delivery_ts <= query_ts`. Số đơn đang cầm lấy số đơn đã nhận tới query, trừ số đã giao và trừ đơn đang xét.
3. Mô hình dùng chín đặc trưng. Bốn đặc trưng lịch sử là `pending_order_count`, `completed_order_count`, `courier_avg_min`, `distance_to_last_delivery_km`. Năm đặc trưng của đơn đang xét là `candidate_lat`, `candidate_lng`, `candidate_aoi_type`, `candidate_region_id` và `accept_hour`. Khoảng cách lịch sử đo từ GPS của lần giao gần nhất tới điểm giao đích của đơn đang xét.
4. `query_grid()` chọn tối đa 12 đơn cho mỗi shipper mỗi ngày bằng SHA-256 với seed lấy mẫu 42. Đặc trưng lịch sử được dựng trước khi lấy mẫu này. Sau khi lấy mẫu còn **123.117 đơn**, chia theo ngày nhận ở mốc 60% và 80%. Giá trị thiếu trong cả ba tập được điền bằng median của từng đặc trưng tính trên train. LightGBM dùng validation để dừng sớm.

`courier_avg_min` ở đây đổi theo mốc nhận từng đơn. Biến thể **A-lite** trong script kiểm tra leakage dùng trung bình cả file của shipper trên cùng tập truy vấn để so với B. [Builder Protocol B](../../src/eta/dataset.py) · [Lấy mẫu và chia ngày](../../src/eta/splits.py) · [Script so A-lite và B](../../scripts/experiments/run_eta_experiment.py)

### 3.3. Spark LR/GBT · baseline thời lượng giao hàng

1. `spark_baseline_features()` dựng hai loại sự kiện từ Bronze gồm nhận và giao hợp lệ. Window lấy các sự kiện có timestamp nhỏ hơn lúc nhận hiện tại. Hạng nhận và khoảng cách trung bình trong ngày dùng các lần nhận sớm hơn, kể cả đơn còn thiếu nhãn giao. Thời lượng trung bình và số đơn đã giao chỉ dùng lần giao đã quan sát.
2. GPS nhận có thời điểm ghi tới lúc nhận và tọa độ hợp lệ mới được dùng. Giá trị thiếu đi cùng cờ GPS thiếu. Median điền thiếu, phân loại AOI và vùng được học từ train.
3. Fact cung cấp nhãn và các khóa báo cáo. Với ngày cắt mặc định **01/10/2022**, train chỉ gồm đơn nhận và đã giao trước ngày đó. Test gồm mọi đơn nhận từ ngày cắt có nhãn hợp lệ trong Fact.
4. LR và GBT học số phút. Đầu vào gồm tám cột số trong `SPARK_BASELINE_FEATURES` cùng AOI và vùng. GBT dùng absolute loss, 40 cây, step size 0,10, độ sâu 5, tối đa 256 bin và seed 42. Hai mốc so sánh là median và trung bình thời lượng trên train. Báo cáo chấm toàn bộ test và thêm lát ≤1.440 phút từ các dự đoán đã tạo. Dashboard đọc các điểm này từ `ml_metrics` theo lần chạy Prefect.

Cohort GPS/24 giờ/50 km thuộc baseline lịch sử. Khi đó `order_rank` và
`cum_avg_dist` tính sau bộ lọc nhãn. `courier_train_avg_min` trên dòng
train có đóng góp từ chính nhãn. GBT cũ học squared loss trên nhãn log.
Phép audit dùng LightGBM để xấp xỉ ảnh hưởng của trung bình shipper trên
cohort cũ. [Huấn luyện Spark hiện tại](../../notebooks/04_fact.ipynb) · [Phép đo lịch sử](leak_measurement.md#3-baseline-spark-courier_train_avg_min)

### Số dòng dùng để fit và chấm trên CSV Yantai

| Nhánh | Đơn vị đếm | Train | Validation | Test |
| --- | --- | --- | --- | --- |
| Protocol P | Cặp truy vấn–đơn theo cửa sổ | 567.672 · `ds` 01/05–18/08 | 202.889 · `ds` 20/08–24/09 | 198.463 · `ds` 26/09–31/10 |
| Protocol B | Đơn được lấy mẫu tại lúc nhận | 73.125 · nhận 01/05–18/08 | 24.647 · nhận 19/08–24/09 | 25.345 · nhận 25/09–31/10 |
| Spark LR/GBT hiện tại | Đơn Fact có nhãn hợp lệ | 169.412 · nhận và giao trước 01/10 | — | 36.625 · nhận từ 01/10 |
| Spark LR/GBT lịch sử | Đơn thuộc cohort GPS/24 giờ/50 km | 165.222 · nhận trước 01/10 | — | 35.778 · nhận từ 01/10 |

Các mốc trên dùng năm **2022**, cấu hình lấy mẫu mặc định và CSV có SHA-256
`aa5bca7fdaa8c89d4ab50a2ed2b94fb09f608acc49fc1108a1a612e9c0f5a7ad`.
Hai ngày `ds` 19/08 và 25/09 được bỏ ở Protocol P. Tùy chọn
`--train-end` của Prefect đặt ngày cắt cho Spark LR/GBT. P và B tự chọn mốc
chia theo danh sách ngày của từng nhánh.
Trong Spark hiện tại, 25 đơn nhận trước mốc 01/10 có nhãn xuất hiện từ
mốc đó trở đi. Điều kiện `delivery_ts < cutoff` loại chúng khỏi tập fit.
Lần giao chỉ cập nhật `courier_completed_count` và `courier_completed_avg_min`
ở các truy vấn có thời điểm nhận sau lần giao đó. Tập test có 36.060 đơn
thuộc lát ≤24 giờ.

Đối soát bằng Spark trên CSV đúng checksum che riêng `delivery_time` của
25 đơn và giữ các sự kiện nhận. Cả hai đặc trưng hoàn tất cho kết quả trùng
trên **169.736 truy vấn Bronze trước ngày cắt**. Từ ngày cắt trở đi, có
2.684 truy vấn đổi số đơn đã giao khi che các sự kiện này, phù hợp với việc
cập nhật lịch sử khi lần giao được quan sát.

Train là tập fit mô hình, validation hướng dẫn dừng sớm của LightGBM, còn test
dùng để báo sai số trên dữ liệu giữ lại. So sánh MAE trong từng bài toán trên
cùng tập đánh giá. [Cấu hình dữ liệu](../../config/prereg.yaml) · [Phạm vi so sánh ETA](analysis_limits.md#4-phạm-vi-so-sánh-kết-quả-eta)

### 3.4. Thử nghiệm thêm lịch sử AOI và GPS cho Protocol B

Script xác nhận SHA-256 của CSV Yantai trước khi tạo tập mẫu. Cả ba biến thể dùng cùng mã đơn, mốc nhận và nhãn là số phút từ nhận đến giao. Có **73.107 đơn train** sau khi bỏ 18 đơn có nhãn từ ngày bắt đầu validation trở đi. Chế độ mặc định chấm **24.647 đơn validation**. Tập **25.345 đơn test** được chấm riêng sau khi khóa cấu hình để kiểm tra khả năng dùng cho vận hành. [Thiết kế và kết quả test](analysis_limits.md#5-kiểm-tra-eta-lúc-nhận-đơn-cho-vận-hành)

- `aoi_past_count` và `aoi_past_avg_min` dùng các đơn cùng `aoi_id` đã giao tới lúc nhận đơn đang xét. `dataset.py` chọn các cột thuộc role `completed` và tính thời lượng lịch sử từ hiệu hai timestamp.
- `accept_to_stop_km` dùng GPS lúc nhận và tọa độ đích từ role `candidate`. GPS cần có tọa độ hợp lệ và `accept_gps_ts <= accept_ts`.
- Baseline dùng median của nhãn train. Ba biến thể LightGBM dùng cùng cấu hình, điền giá trị thiếu bằng median từng đặc trưng trên train và chạy seed mô hình 42, 43, 44 theo `prereg.yaml`.

`ACC@20` và `ACC@30` là phần trăm đơn có sai số tuyệt đối tối đa 20 và 30 phút. Các hàng LightGBM ghi trung bình qua ba seed, với độ lệch chuẩn mẫu bên cạnh MAE. Seed chọn đơn giữ ở 42, nên độ lệch chuẩn chỉ phản ánh biến động theo seed mô hình. [CSV kết quả trên LightGBM 4.7.0](../../reports/eta/eta_accept_benchmark_validation.csv)

| Biến thể | MAE (phút) | ACC@20 | ACC@30 | P90 sai số tuyệt đối (phút) |
| --- | ---: | ---: | ---: | ---: |
| Trung vị train | 182,15 | 9,08% | 13,47% | 327,00 |
| Protocol B | 115,12 ± 0,48 | 19,61% | 28,71% | 231,58 |
| B + AOI | 111,14 ± 0,31 | 21,64% | 31,14% | 224,12 |
| B + AOI + GPS | 109,60 ± 0,14 | 22,66% | 32,51% | 223,04 |

Thử nghiệm dùng cùng builder và hàm fit với notebook. Chế độ mặc định tái
lập bảng validation lịch sử. [Script thử nghiệm](../../scripts/experiments/run_eta_accept_benchmark.py)

~~~bash
python -m scripts.experiments.run_eta_accept_benchmark
~~~

### 3.5. So sánh rolling-origin trên cùng mã đơn

Benchmark và notebook 06 dùng `build_protocol_b_samples()` cùng
`fit_accept_eta()`. Mỗi dòng chấm giữ đúng mã đơn, thời điểm nhận và nhãn
của tập Protocol B gốc. Các cửa sổ tăng dần theo ngày nhận là 19/08–31/08,
01/09–12/09 và 13/09–24/09/2022. Mỗi cửa sổ dùng tối đa 14 ngày có mẫu ngay
trước đó để hiệu chỉnh dừng sớm. Đoạn fit đứng trước đoạn hiệu chỉnh.
Nhãn fit phải xuất hiện trước ngày hiệu chỉnh, nhãn hiệu chỉnh phải xuất
hiện trước ngày chấm. Median điền thiếu chỉ học trên tập fit.

| Đặc trưng | Sự kiện dùng để dựng giá trị |
| --- | --- |
| Hàng chờ và tiến độ shipper | Lần nhận và giao hợp lệ trước query. Đơn nhận trước query vẫn góp vào hàng chờ khi thiếu sự kiện giao. Số đơn đã nhận, đã giao trong ngày và số phút từ lần giao cuối mô tả trạng thái ở lúc nhận. |
| AOI và vùng × giờ | Thời lượng của các đơn giao trong 28 ngày trước query, cùng AOI hoặc cùng vùng và giờ nhận. Trung bình AOI toàn lịch sử và khoảng cách từ GPS nhận vẫn nằm trong bộ đặc trưng. |
| Nhịp của shipper | Thời lượng giao và trung bình khoảng cách thời gian giữa hai lần giao trong cùng ngày. Lịch sử chọn theo các lần giao trong bảy ngày trước query. |
| Lô cùng phút | Số đơn cùng shipper và timestamp nhận, kèm hạng khoảng cách GPS–đích. Biến thể này giả định mọi đơn cùng timestamp đã được thấy cùng lúc. |

Biến thể `protocol_b_observed` dựng lại bộ B + AOI + GPS từ sự kiện trước
query. Khoảng cách tới lần giao gần nhất dùng tọa độ đích của đơn đã giao.
`protocol_b_queue_history` bổ sung hàng chờ trong ngày và lịch sử gần đây.
Biến thể lô thêm `batch_size` và `batch_distance_rank`. Trên CSV Yantai,
khoảng 77,6% dòng chia sẻ timestamp nhận với ít nhất một đơn khác của
cùng shipper. Timestamp nhận có độ phân giải phút.

| Biến thể | MAE (phút) | WAPE | ACC@30 | ACC@60 |
| --- | ---: | ---: | ---: | ---: |
| Trung vị theo vùng và khung giờ | 147,93 | 51,49% | 18,05% | 35,24% |
| B + AOI + GPS lịch sử | 109,52 | 38,13% | 31,43% | 53,36% |
| B + AOI + GPS theo sự kiện trước query | 109,82 | 38,23% | 30,79% | 52,79% |
| Thêm hàng chờ và lịch sử gần đây | **104,16** | **36,26%** | **31,88%** | **54,49%** |
| Thêm cả lô cùng phút | 104,90 | 36,52% | 31,82% | 54,24% |

Các hàng LightGBM lấy trung bình qua seed 42–44 trên **24.647 đơn
validation**. Thêm hàng chờ và lịch sử vào bộ B + AOI + GPS dựng theo sự
kiện giảm MAE từ **109,82 xuống 104,16 phút**, tức **5,66 phút**. So với
cấu hình lịch sử, mức giảm trung bình là **5,36 phút**. Với seed 42, mức giảm so với cấu
hình lịch sử là **6,08 phút**. Bootstrap ghép cặp theo 3.579 cụm
shipper–ngày cho khoảng 95% từ **−7,50 đến −4,73 phút** với 2.000 lượt.
Các cửa sổ này dùng để phát triển đặc trưng `protocol_b_queue_history`.
WAPE là tổng sai số tuyệt đối chia tổng thời lượng thực tế. CSV có điểm
từng cửa sổ, kích thước lô, khung bốn giờ, vùng và nhóm thời lượng. Hai
nhóm thời lượng được tách sau khi dự đoán. [Kết quả rolling-origin](../../reports/eta/eta_accept_benchmark_rolling.csv)

Prefect chọn `protocol_b_queue_history` vì MAE giảm ở cả ba cửa sổ và
bộ này dùng được khi nguồn gửi từng đơn tại lúc nhận. Notebook 06 đối
chiếu nhãn, hàng chờ, AOI, GPS và tổng giá trị lá cây với
`predict()`. [Đối chiếu một đơn thật](../../notebooks/06_eta_accept.ipynb)

~~~bash
python -m scripts.experiments.run_eta_accept_benchmark --rolling-origin
~~~

#### Đối chiếu LightGBM và Spark

Chế độ `--spark` chạy LightGBM, Spark LR và Spark GBT trên cùng ba cửa sổ,
cùng mã đơn và cùng bộ đặc trưng hàng chờ và lịch sử. Nhãn là số phút.
GBT cũ dùng `validationIndicatorCol` với giới hạn 80 cây. `getNumTrees`
xác nhận mô hình chỉ giữ một cây ở cả ba cửa sổ. GBT hiệu chỉnh fit đủ
số vòng trên tập train, thử 40 cây với step size 0,05 và 80 cây với
step size 0,1. Cả hai giữ absolute loss, độ sâu 6 và tối đa 256 bin.
Cấu hình 80 cây có MAE thấp hơn trên cả ba tập hiệu chỉnh nên được
chọn để chấm từng cửa sổ tiếp theo. CSV ghi tham số, số cây thực tế và
MAE hiệu chỉnh của mỗi lần thử. LR dùng squared loss. Lát ≤1.440 phút
được tính sau dự đoán.

| Mô hình, seed 42 | MAE (phút), 24.647 đơn | MAE (phút), 24.053 đơn ≤1.440 phút | ACC@30 toàn bộ |
| --- | ---: | ---: | ---: |
| LightGBM | 103,71 | 90,24 | 31,82% |
| Spark LR | 129,15 | 113,38 | 17,78% |
| Spark GBT dừng sớm, 1 cây | 127,41 | 112,13 | 20,43% |
| Spark GBT hiệu chỉnh, 80 cây | 126,39 | 111,11 | 20,98% |

Spark GBT hiệu chỉnh cao hơn LightGBM **22,68 phút** trên cùng đơn và
cùng đặc trưng. Khoảng tin cậy bootstrap ghép cặp 95% theo cụm
shipper–ngày từ **20,03 đến 25,68 phút**. Hai cấu hình GBT đều dùng
absolute loss và thay đồng thời số cây cùng step size. Ảnh hưởng riêng
của loss và step size chưa được đo. Kết quả mô tả các cấu hình đã thử.
[CSV đối chứng và số cây](../../reports/eta/eta_accept_spark_calibrated_rolling.csv) · [CSV đối chứng lịch sử](../../reports/eta/eta_accept_spark_rolling.csv)

| Nguồn kết quả GBT | Đặc trưng và tập chấm mặc định | Cây · step size · độ sâu | Nguồn MAE |
| --- | --- | --- | --- |
| Notebook 04 trong Prefect | Tám cột số cùng AOI và vùng. 36.625 đơn Fact Yantai từ 01/10/2022. | 40 · 0,10 · 5 | `ml_metrics` của lần chạy pipeline |
| Benchmark cùng mẫu Protocol B | Cùng bộ `protocol_b_queue_history` với LightGBM. 24.647 đơn validation Yantai từ 19/08 đến 24/09/2022. | 80 · 0,10 · 6 | 126,39 phút trong CSV benchmark |

Spark LR/GBT trong Prefect là baseline tham chiếu của Fact. Benchmark dùng
cấu hình Spark đã hiệu chỉnh trên mẫu Protocol B để so với LightGBM.

Trong Spark 3.5.3, `validationTol=0` vẫn cho phép dừng sớm nếu sai số
hiệu chỉnh tăng. Script dùng thư mục checkpoint tạm cho các lần fit.
[Điều kiện dừng trong Spark](https://github.com/apache/spark/blob/v3.5.3/mllib/src/main/scala/org/apache/spark/ml/tree/impl/GradientBoostedTrees.scala#L430-L437)

Chạy với image Jupyter đã build. Bài đo chỉ dùng Spark cục bộ trong container.

~~~powershell
docker compose run -T --rm --no-deps -e "PYSPARK_SUBMIT_ARGS=--master local[2] --driver-memory 1g pyspark-shell" jupyter python -m scripts.experiments.run_eta_accept_benchmark --rolling-origin --spark --seeds 42
~~~

## 4. Dữ liệu được lưu và đưa lên dashboard

Các tập train, validation và test được dựng thành DataFrame trong lúc chạy.
Pipeline lưu dự đoán và metric theo các nhánh sau.

| Dữ liệu đã tạo | Nơi lưu | Nội dung dùng tiếp |
| --- | --- | --- |
| Kết quả Protocol P | Delta `eta_paper_gold`, `eta_paper_metrics_gold` · MySQL `eta_paper_metrics` | Delta giữ dự đoán từng cặp test của seed đầu. Metric test gồm median, SPEED và LightGBM theo seed. Dashboard đọc metric MySQL. |
| Kết quả Protocol B | Delta `eta_safe_gold`, `eta_safe_metrics_gold` · MySQL `eta_safe_metrics` | Delta giữ mã đơn và dự đoán validation của seed đầu. Metric có mã biến thể, seed, phạm vi và mốc cửa sổ chấm. Flow công bố cửa sổ cuối của rolling-origin. |
| Kết quả Spark LR/GBT | Delta `eta_predictions_gold` · MySQL `ml_metrics`, `ml_feature_importance` | Delta giữ dự đoán từng đơn test cho LR, GBT, median và mean. MySQL dùng metric test và độ quan trọng feature của GBT để hiển thị. |
| Tổng hợp vận hành | MySQL `agg_*` và các bảng `dim_*` | Tổng đơn, thời lượng và tỷ lệ trễ lấy từ toàn bộ fact. Số liệu sai số GBT theo shipper/khu vực lấy từ dự đoán test. |
| Mẫu dự đoán và thông tin lần chạy | MySQL `eta_prediction_sample`, `pipeline_run_info` | Mẫu tối đa 3.000 đơn lấy từ dự đoán Spark trên test để vẽ biểu đồ. `pipeline_run_info` ghi ngày cắt, kích thước tập Spark và ngưỡng tính trễ. |

Dashboard chỉ đọc bộ bảng MySQL của một lần chạy `COMPLETED` có cùng mã lần chạy.
Báo cáo audit trong `reports/` và các script chạy độc lập là nhánh nghiên cứu.
Hai biến thể `--variant pit` và `--variant cohort` của `run_paper_protocol` đọc
CSV để đo độ nhạy ngoại tuyến của Protocol P. `check_courier_avg_leak` dùng
cùng CSV để kiểm tra đặc trưng lịch sử shipper của baseline Spark. Prefect gọi
các bước huấn luyện ở mục 3.
[Serving MySQL](../../notebooks/05_serving.ipynb) · [Kiểm tra snapshot của dashboard](../../dashboard/serving.py) · [Các phép đo audit](leak_measurement.md)
