# Star schema hien tai

So do bang va quan he day du nam trong [data_warehouse.txt](data_warehouse.txt).

Day la star schema toi gian hien tai: `fact_delivery_task_gold` la fact trung tam, `dim_courier_silver` la dimension SCD2. `region_id` va `aoi_id` dang la degenerate dimensions trong fact vi pipeline chua tao `dim_region` va `dim_aoi`.

## Fact trung tam

`fact_delivery_task_gold` co grain mot dong cho mot don giao hang. Khoa chinh nghiep vu la `order_id`. `courier_id` duoc point-in-time join vao dimension courier. `delivery_minutes` la nhan cua bai toan phu.

## Dimension courier

`dim_courier_silver` co khoa ghep `courier_id + effective_date`, voi `end_date` de bieu dien khoang hieu luc SCD2. Fact chi duoc join voi ban ghi dimension bao phu `order_date`.

## Fact ETA

`eta_safe_gold` co grain mot candidate tai mot query time, khoa ghep `order_id + query_ts`. Day la fact phuc vu bai toan ETA chinh, khong phai dimension. `target_remaining_minutes` la nhan va khong duoc di vao feature matrix. `eta_safe_metrics_gold` luu metric theo run, split va seed.

## Bang chat luong

`delivery_rejected` luu cac dong bi tu choi o Silver cung `reject_reason`. Bang nay phuc vu audit va lineage, khong tham gia vao fact ETA.
