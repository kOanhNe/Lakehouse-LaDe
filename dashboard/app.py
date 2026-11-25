"""Dashboard tiếng Việt cho Lakehouse LaDe-D Yantai.

Dashboard chỉ đọc các bảng serving trong MySQL. Nó không chạy lại pipeline và
không tính lại metric từ dữ liệu raw, vì vậy số liệu hiển thị có thể truy vết
về một lần chạy Prefect cụ thể.
"""
from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import insights
from serving import LATEST_RUN_SQL, load_snapshot
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

st.set_page_config(page_title="LaDe-D | Lakehouse Analytics", page_icon="🚚", layout="wide",
                   initial_sidebar_state="expanded")

st.markdown("""
<style>
.block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1500px;}
[data-testid="stMetricValue"] {font-size: 1.65rem;}
.hero {background: linear-gradient(120deg,#102a43,#167d9a); color:white; padding:1.5rem 2rem;
       border-radius:16px; margin-bottom:1rem;}
.hero h1 {margin:0 0 .35rem 0; font-size:2.1rem;}
.hero p {margin:0; opacity:.9; font-size:1rem;}
.note {background:#f0f7fa; border-left:4px solid #168aad; padding:.7rem 1rem; border-radius:4px;}
</style>
""", unsafe_allow_html=True)


DB_URL = URL.create("mysql+pymysql", username=os.getenv("MYSQL_USER", "delivery"),
                    password=os.getenv("MYSQL_PASSWORD", "delivery"), host=os.getenv("MYSQL_HOST", "mysql"),
                    port=int(os.getenv("MYSQL_PORT", "3306")), database=os.getenv("MYSQL_DB", "delivery_source"))


@st.cache_resource
def db_engine():
    return create_engine(DB_URL, pool_pre_ping=True, pool_recycle=1800)


def read_serving():
    with db_engine().connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        def read_run():
            try:
                result = connection.execute(text(LATEST_RUN_SQL)).mappings().first()
            except ProgrammingError as error:
                if error.orig.args[0] == 1146:
                    return None
                raise
            return dict(result) if result else None

        def read_table(name):
            try:
                return pd.read_sql(text(f"SELECT * FROM `{name}`"), connection)
            except ProgrammingError as error:
                if error.orig.args[0] == 1146:
                    return None
                raise

        return load_snapshot(read_run, read_table)


def fmt_minutes(value) -> str:
    return "—" if pd.isna(value) else f"{float(value):,.1f} phút"


def pct(value) -> str:
    return "—" if pd.isna(value) else f"{float(value) * 100:.1f}%"


def chart(fig, height=380):
    fig.update_layout(template="plotly_white", height=height, margin=dict(l=10, r=10, t=55, b=10),
                      legend=dict(orientation="h", y=1.08, x=0))
    st.plotly_chart(fig, width="stretch", config={"displaylogo": False})


if st.sidebar.button("🔄 Làm mới dữ liệu", width="stretch"):
    st.rerun()

try:
    status, run, tables = read_serving()
except SQLAlchemyError:
    status, run, tables = "UNAVAILABLE", None, {}

if status != "COMPLETED":
    st.markdown('<div class="hero"><h1>🚚 LaDe-D Yantai</h1><p>Dashboard phân tích giao hàng trên kiến trúc Lakehouse</p></div>', unsafe_allow_html=True)
    messages = {
        "EMPTY": "Chưa có lần chạy Prefect được ghi nhận.",
        "RUNNING": "Pipeline đang xử lý.",
        "FAILED": "Lần chạy gần nhất thất bại.",
        "INTERRUPTED": "Lần chạy gần nhất bị gián đoạn.",
        "INCOMPLETE": "Dữ liệu serving thiếu hoặc không cùng mã lần chạy.",
        "CHANGED": "Trạng thái pipeline vừa thay đổi. Bấm làm mới để đọc lại.",
        "UNAVAILABLE": "Hiện không đọc được dữ liệu serving từ MySQL.",
    }
    st.warning(messages.get(status, "Trạng thái lần chạy chưa xác định."))
    if run:
        st.caption(f"Run {run['run_id']} · {run['stage']}")
    st.stop()

tables["ml_metrics"] = insights.spark_baseline_metrics(tables["ml_metrics"])
o = tables["agg_overview"].iloc[0]
run_text = f"Run {run['run_id']} · hoàn tất {run['ended_at']} UTC"

st.markdown(f'<div class="hero"><h1>🚚 LaDe-D Yantai</h1><p>Phân tích chất lượng giao hàng, hiệu suất shipper và mô hình ETA · {run_text}</p></div>', unsafe_allow_html=True)

with st.sidebar:
    st.header("Bộ lọc dashboard")
    st.caption("Các biểu đồ đọc từ bảng serving đã được Prefect ghi vào MySQL.")
    st.divider()
    st.subheader("Thông tin hệ thống")
    st.write("**Nguồn:** LaDe-D · Yantai")
    st.write("**Grain fact:** một dòng / đơn hàng")
    st.write("**Đơn vị:** phút, km")
    st.caption("Dashboard không thay thế kiểm định dữ liệu và không dùng để chọn model trên test.")

tab_overview, tab_ops, tab_paper, tab_protocol_b, tab_spark, tab_data = st.tabs([
    "📊 Tổng quan", "🛵 Shipper & khu vực", "🎯 Protocol P", "Protocol B", "Spark LR/GBT", "🔎 Chất lượng & lineage"
])


def comments(lines, title="Nhận xét"):
    if lines:
        st.markdown(f"<div class='note'><b>{title}</b><ul style='margin:.3rem 0 0 1.1rem'>"
                    + "".join(f"<li>{x}</li>" for x in lines) + "</ul></div>", unsafe_allow_html=True)


with tab_overview:
    st.subheader("Bức tranh chung")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Tổng đơn", f"{int(o['total_orders']):,}")
    c2.metric("Shipper", f"{int(o['couriers']):,}")
    c3.metric("Thời gian trung vị", fmt_minutes(o['median_delivery_minutes']))
    c4.metric("Tỷ lệ đơn chậm", pct(o['late_rate']))
    c5.metric("Đơn trên 24 giờ", pct(o['over_24h_rate']))
    st.markdown("<div class='note'>Thời gian xử lý là từ lúc nhận đơn đến lúc hoàn tất giao, không phải thời gian lái xe. Trung vị được ưu tiên vì dữ liệu có một số đơn rất dài.</div>", unsafe_allow_html=True)

    comments(insights.overview_insights(o, tables["agg_hourly"], tables["agg_region_summary"]))

    daily = tables["agg_region_daily"]
    if daily is not None and not daily.empty:
        regions = sorted(daily.region_id.dropna().unique().tolist())
        selected = st.multiselect("Chọn khu vực", regions, default=regions, key="overview_regions")
        d = daily[daily.region_id.isin(selected)].copy()
        d["order_date"] = pd.to_datetime(d["order_date"])
        d["weighted_minutes"] = d.avg_delivery_minutes * d.total_orders
        trend = d.groupby("order_date", as_index=False).agg(total_orders=("total_orders", "sum"), weighted_minutes=("weighted_minutes", "sum"))
        trend["avg_delivery_minutes"] = trend.weighted_minutes / trend.total_orders
        a, b = st.columns(2)
        with a:
            chart(px.area(trend, x="order_date", y="total_orders", labels={"order_date": "Ngày", "total_orders": "Số đơn"}, title="Số đơn theo ngày"))
        with b:
            chart(px.line(trend, x="order_date", y="avg_delivery_minutes", markers=True, labels={"order_date": "Ngày", "avg_delivery_minutes": "Phút"}, title="Thời gian trung bình theo ngày"))

    sample = tables["eta_prediction_sample"]
    if sample is not None and not sample.empty:
        st.subheader("Phân phối thời gian xử lý")
        cap = sample.actual_minutes.quantile(.99)
        chart(px.histogram(sample[sample.actual_minutes <= cap], x="actual_minutes", nbins=55,
                           labels={"actual_minutes": "Thời gian (phút)"}, title="Mẫu 3.000 đơn kiểm tra, cắt hiển thị 1% đuôi dài nhất"), 350)

with tab_ops:
    courier = tables["agg_courier_performance"]
    region = tables["agg_region_summary"]
    if courier is not None and not courier.empty:
        st.subheader("Hiệu suất shipper")
        f1, f2, f3 = st.columns(3)
        region_options = sorted(courier.main_region_id.dropna().unique().tolist())
        chosen_regions = f1.multiselect("Khu vực chính", region_options, default=region_options, key="courier_regions")
        min_orders = f2.slider("Số đơn tối thiểu", 0, int(courier.total_orders.max()), min(200, int(courier.total_orders.max())), step=25)
        rank_key = f3.selectbox("Xếp hạng theo", ["late_rate", "median_delivery_minutes", "avg_delivery_minutes", "total_orders"], format_func=lambda x: {"late_rate":"Tỷ lệ chậm", "median_delivery_minutes":"Trung vị", "avg_delivery_minutes":"Trung bình", "total_orders":"Số đơn"}[x])
        comments(insights.courier_insights(courier), "Nhận xét về shipper")
        c = courier[(courier.total_orders >= min_orders) & courier.main_region_id.isin(chosen_regions)].copy().sort_values(rank_key, ascending=rank_key != "total_orders")
        if c.empty:
            st.info("Không có shipper phù hợp bộ lọc.")
        else:
            top = c.head(15).copy()
            top["shipper"] = "Shipper " + top.courier_id.astype(str)
            chart(px.bar(top, x=rank_key, y="shipper", orientation="h", text=rank_key, title="15 shipper theo tiêu chí đã chọn", labels={rank_key: rank_key, "shipper":""}), 500)
            chart(px.scatter(c, x="total_orders", y="median_delivery_minutes", color="late_rate", hover_data=["courier_id", "main_region_id"], title="Số đơn, thời gian trung vị và tỷ lệ chậm"), 430)
            show = c.copy()
            show["late_rate"] = show.late_rate.map(lambda x: f"{x:.1%}")
            st.dataframe(show, width="stretch", hide_index=True)
    if region is not None and not region.empty:
        st.subheader("So sánh khu vực")
        rs = region.copy(); rs["khu_vuc"] = "KV " + rs.region_id.astype(str)
        a, b = st.columns(2)
        with a:
            chart(px.bar(rs.sort_values("median_delivery_minutes"), x="khu_vuc", y="median_delivery_minutes", title="Thời gian trung vị theo khu vực", labels={"khu_vuc":"Khu vực", "median_delivery_minutes":"Phút"}))
        with b:
            chart(px.bar(rs.sort_values("late_rate", ascending=False), x="khu_vuc", y="late_rate", title="Tỷ lệ đơn chậm theo khu vực", labels={"khu_vuc":"Khu vực", "late_rate":"Tỷ lệ"}), 380)
            st.dataframe(rs, width="stretch", hide_index=True)
    hourly = tables["agg_hourly"]
    if hourly is not None and not hourly.empty:
        st.subheader("Theo giờ nhận đơn")
        chart(px.line(hourly.sort_values("accept_hour"), x="accept_hour", y=["total_orders", "median_delivery_minutes"], markers=True, title="Số đơn và thời gian trung vị theo giờ nhận"), 420)

with tab_paper:
    st.subheader("Protocol P · ETA chính, tái lập LaDe")
    st.markdown("Tại thời điểm shipper vừa giao xong một đơn, mô hình dự đoán **mỗi đơn còn đang cầm sẽ giao sau bao nhiêu phút**. "
                "Đây là phép tái lập ngoại tuyến để đối chiếu mô tả với bài báo. Cohort dùng nhãn 1–120 phút.")
    pm = tables["eta_paper_metrics"]
    if pm is None or pm.empty:
        st.info("Chưa có bảng `eta_paper_metrics`. Chạy lại pipeline (task 4c) rồi bấm làm mới.")
    else:
        wide = insights.paper_summary(pm)
        ours = wide.reset_index().rename(columns={"index": "Mô hình", "mae": "MAE", "rmse_paper": "RMSE", "acc20": "ACC@20 (%)"})
        paper_rows = pd.DataFrame([{"Mô hình": f"Bài báo: {k}", "MAE": v[0], "RMSE": v[1], "ACC@20 (%)": v[2]} for k, v in insights.PAPER.items()])
        both = pd.concat([ours[["Mô hình", "MAE", "RMSE", "ACC@20 (%)"]], paper_rows], ignore_index=True)
        both["Nguồn"] = ["Đồ án"] * len(ours) + ["Bài báo"] * len(paper_rows)
        k1, k2, k3 = st.columns(3)
        if "LightGBM" in wide.index and "SPEED" in wide.index:
            k1.metric("MAE LightGBM · Protocol P", f"{wide.loc['LightGBM', 'mae']:.1f} phút",
                      f"{wide.loc['LightGBM', 'mae'] - wide.loc['SPEED', 'mae']:.1f} so với SPEED", delta_color="inverse")
            k2.metric("ACC@20 LightGBM", f"{wide.loc['LightGBM', 'acc20']:.1f}%")
            k3.metric("MAE SPEED", f"{wide.loc['SPEED', 'mae']:.1f} phút", f"bài báo: {insights.PAPER['speed'][0]}", delta_color="off")
        a, b = st.columns(2)
        with a:
            chart(px.bar(both, x="Mô hình", y="MAE", color="Nguồn", title="MAE (phút, càng thấp càng tốt)"), 420)
        with b:
            chart(px.bar(both, x="Mô hình", y="ACC@20 (%)", color="Nguồn", title="Tỷ lệ sai không quá 20 phút (%)"), 420)
        st.dataframe(both.round(2), width="stretch", hide_index=True)
        comments(insights.paper_insights(pm))
        st.markdown("<div class='note'><b>Lưu ý khi diễn giải.</b> Bộ lọc nhãn 1–120 phút loại khoảng một nửa số cặp (truy vấn, đơn), "
                    "nên sai số chỉ áp dụng cho cohort này. Tốc độ shipper dùng cả file. Số đơn còn lại và thứ hạng khoảng cách "
                    "phụ thuộc nhãn tương lai. LightGBM lấy trung bình 3 seed.</div>", unsafe_allow_html=True)

with tab_protocol_b:
    st.subheader("Protocol B · ETA tại lúc nhận đơn")
    st.markdown("Đánh giá ETA vận hành trong đồ án tại lúc shipper nhận đơn. "
                "Các chỉ số áp dụng cho cửa sổ và phiên bản ghi trong lần chạy.")
    safe = tables["eta_safe_metrics"]
    if safe is not None and not safe.empty:
        columns = ["variant", "evaluation_scope", "score_start", "score_end_exclusive",
                   "split", "seed", "n", "mae", "wape_pct", "acc30_pct", "acc60_pct", "p90_abs_error_min"]
        if "variant" not in safe:
            st.caption("Kết quả lịch sử của lần chạy Protocol B gốc.")
        else:
            st.caption("rolling_validation là cửa sổ validation cuối. calibration là điểm trên tập hiệu chỉnh.")
        st.dataframe(safe[[name for name in columns if name in safe]], width="stretch", hide_index=True)

with tab_spark:
    st.subheader("Spark LR/GBT · Baseline thời lượng giao hàng")
    st.markdown("Cohort Spark gồm đơn có GPS nhận hợp lệ, khoảng cách tối đa 50 km và thời lượng tối đa 24 giờ. "
                "So sánh sai số giữa các baseline thuộc cohort này.")
    metrics = tables["ml_metrics"]
    st.dataframe(metrics, width="stretch", hide_index=True)
    plot = metrics.dropna(subset=["mae", "rmse"])
    if not plot.empty:
        chart(px.bar(plot, x="model", y=["mae", "rmse"], barmode="group", title="Sai số trên cohort Spark",
                     labels={"model": "Mô hình", "value": "Phút"}), 400)
    imp = tables["ml_feature_importance"]
    if imp is not None and not imp.empty:
        st.subheader("Độ quan trọng feature của GBT phụ")
        chart(px.bar(imp.sort_values("importance"), x="importance", y="feature", orientation="h", title="Feature importance"), 400)

with tab_data:
    st.subheader("Lineage và trạng thái dữ liệu")
    st.write("Silver giữ các cờ chất lượng. Ngưỡng 24 giờ và 50 km áp dụng khi dựng cohort mô hình Spark.")
    info = tables["pipeline_run_info"]
    if info is not None and not info.empty:
        st.dataframe(info, width="stretch", hide_index=True)
    st.subheader("Tải bảng đang hiển thị")
    choices = {key: value for key, value in tables.items() if not value.empty}
    selected_table = st.selectbox("Chọn bảng", list(choices), key="download_table")
    if selected_table:
        st.download_button("⬇️ Tải CSV", choices[selected_table].to_csv(index=False).encode("utf-8-sig"), file_name=f"{selected_table}.csv", mime="text/csv")
