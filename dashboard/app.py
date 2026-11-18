"""Dashboard tiếng Việt cho Lakehouse LaDe-D Yantai.

Dashboard chỉ đọc các bảng serving trong MySQL. Nó không chạy lại pipeline và
không tính lại metric từ dữ liệu raw, vì vậy số liệu hiển thị có thể truy vết
về một lần chạy Prefect cụ thể.
"""
from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

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


DB_URL = (f"mysql+pymysql://{os.getenv('MYSQL_USER', 'delivery')}:{os.getenv('MYSQL_PASSWORD', 'delivery')}"
          f"@{os.getenv('MYSQL_HOST', 'mysql')}:{os.getenv('MYSQL_PORT', '3306')}/"
          f"{os.getenv('MYSQL_DB', 'delivery_source')}")


@st.cache_resource
def db_engine():
    return create_engine(DB_URL, pool_pre_ping=True, pool_recycle=1800)


@st.cache_data(ttl=120, show_spinner=False)
def read_table(name: str) -> pd.DataFrame | None:
    allowed = {
        "agg_overview", "agg_region_daily", "agg_hourly", "agg_region_summary",
        "agg_region_error", "agg_courier_performance", "ml_metrics",
        "ml_feature_importance", "eta_prediction_sample", "eta_safe_metrics",
        "pipeline_run_info",
    }
    if name not in allowed:
        raise ValueError(name)
    try:
        with db_engine().connect() as connection:
            return pd.read_sql(text(f"SELECT * FROM `{name}`"), connection)
    except Exception:
        return None


def fmt_minutes(value) -> str:
    return "—" if pd.isna(value) else f"{float(value):,.1f} phút"


def pct(value) -> str:
    return "—" if pd.isna(value) else f"{float(value) * 100:.1f}%"


def chart(fig, height=380):
    fig.update_layout(template="plotly_white", height=height, margin=dict(l=10, r=10, t=55, b=10),
                      legend=dict(orientation="h", y=1.08, x=0))
    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})


tables = {name: read_table(name) for name in [
    "agg_overview", "agg_region_daily", "agg_hourly", "agg_region_summary",
    "agg_region_error", "agg_courier_performance", "ml_metrics",
    "ml_feature_importance", "eta_prediction_sample", "eta_safe_metrics",
    "pipeline_run_info",
]}

overview = tables["agg_overview"]
if overview is None or overview.empty:
    st.markdown('<div class="hero"><h1>🚚 LaDe-D Yantai</h1><p>Dashboard phân tích giao hàng trên kiến trúc Lakehouse</p></div>', unsafe_allow_html=True)
    st.warning("Chưa có dữ liệu serving trong MySQL. Hãy chạy pipeline Prefect trước, sau đó bấm Rerun.")
    st.code("docker compose exec jupyter python /home/jovyan/pipeline/flow.py", language="powershell")
    st.stop()

o = overview.iloc[0]
run_info = tables["pipeline_run_info"]
run_text = "Chưa có thông tin lần chạy"
if run_info is not None and not run_info.empty and "run_ts" in run_info:
    run_text = f"Lần chạy fact gần nhất: {run_info.iloc[0]['run_ts']}"

st.markdown(f'<div class="hero"><h1>🚚 LaDe-D Yantai</h1><p>Phân tích chất lượng giao hàng, hiệu suất shipper và mô hình ETA · {run_text}</p></div>', unsafe_allow_html=True)

with st.sidebar:
    st.header("Bộ lọc dashboard")
    st.caption("Các biểu đồ đọc từ bảng serving đã được Prefect ghi vào MySQL.")
    if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.subheader("Thông tin hệ thống")
    st.write("**Nguồn:** LaDe-D · Yantai")
    st.write("**Grain fact:** một dòng / đơn hàng")
    st.write("**Đơn vị:** phút, km")
    st.caption("Dashboard không thay thế kiểm định dữ liệu và không dùng để chọn model trên test.")

tab_overview, tab_ops, tab_model, tab_data = st.tabs([
    "📊 Tổng quan", "🛵 Shipper & khu vực", "🤖 Mô hình ETA", "🔎 Chất lượng & lineage"
])

with tab_overview:
    st.subheader("Bức tranh chung")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Tổng đơn", f"{int(o['total_orders']):,}")
    c2.metric("Shipper", f"{int(o['couriers']):,}")
    c3.metric("Thời gian trung vị", fmt_minutes(o['median_delivery_minutes']))
    c4.metric("Tỷ lệ đơn chậm", pct(o['late_rate']))
    c5.metric("Đơn trên 24 giờ", pct(o['over_24h_rate']))
    st.markdown("<div class='note'>Thời gian xử lý là từ lúc nhận đơn đến lúc hoàn tất giao, không phải thời gian lái xe. Trung vị được ưu tiên vì dữ liệu có một số đơn rất dài.</div>", unsafe_allow_html=True)

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
            st.dataframe(show, use_container_width=True, hide_index=True)
    if region is not None and not region.empty:
        st.subheader("So sánh khu vực")
        rs = region.copy(); rs["khu_vuc"] = "KV " + rs.region_id.astype(str)
        a, b = st.columns(2)
        with a:
            chart(px.bar(rs.sort_values("median_delivery_minutes"), x="khu_vuc", y="median_delivery_minutes", title="Thời gian trung vị theo khu vực", labels={"khu_vuc":"Khu vực", "median_delivery_minutes":"Phút"}))
        with b:
            chart(px.bar(rs.sort_values("late_rate", ascending=False), x="khu_vuc", y="late_rate", title="Tỷ lệ đơn chậm theo khu vực", labels={"khu_vuc":"Khu vực", "late_rate":"Tỷ lệ"}), 380)
            st.dataframe(rs, use_container_width=True, hide_index=True)
    hourly = tables["agg_hourly"]
    if hourly is not None and not hourly.empty:
        st.subheader("Theo giờ nhận đơn")
        chart(px.line(hourly.sort_values("accept_hour"), x="accept_hour", y=["total_orders", "median_delivery_minutes"], markers=True, title="Số đơn và thời gian trung vị theo giờ nhận"), 420)

with tab_model:
    st.subheader("Đánh giá mô hình")
    st.markdown("Mô hình **ETA leakage-safe** trả lời: tại thời điểm query, đơn còn khoảng bao nhiêu phút nữa sẽ hoàn tất? `delivery_gps` của đơn pending/candidate chưa giao không được dùng.")
    safe = tables["eta_safe_metrics"]
    if safe is not None and not safe.empty:
        st.success("Đã có metric của Protocol B leakage-safe.")
        st.dataframe(safe, use_container_width=True, hide_index=True)
        test_safe = safe[safe.split.astype(str).str.lower().eq("test")]
        if not test_safe.empty:
            chart(px.bar(test_safe, x="seed", y=["mae", "rmse"], barmode="group", title="ETA leakage-safe theo seed", labels={"seed":"Seed", "value":"Phút"}), 380)
    else:
        st.info("Chưa có bảng `eta_safe_metrics`. Pipeline cũ vẫn có thể hiển thị bên dưới; chạy lại image/pipeline mới để ghi thêm bảng này.")
    metrics = tables["ml_metrics"]
    if metrics is not None and not metrics.empty:
        st.subheader("Baseline và mô hình cũ")
        st.caption("Bảng này là bài toán phụ: dự đoán delivery_minutes từ lúc nhận đơn. Không so trực tiếp với Protocol B.")
        st.dataframe(metrics, use_container_width=True, hide_index=True)
        plot = metrics.dropna(subset=["mae", "rmse"]).copy()
        if not plot.empty:
            chart(px.bar(plot, x="model", y=["mae", "rmse"], barmode="group", title="Sai số bài toán phụ", labels={"model":"Mô hình", "value":"Phút"}), 400)
    imp = tables["ml_feature_importance"]
    if imp is not None and not imp.empty:
        st.subheader("Độ quan trọng feature của GBT phụ")
        chart(px.bar(imp.sort_values("importance"), x="importance", y="feature", orientation="h", title="Feature importance"), 400)

with tab_data:
    st.subheader("Lineage và trạng thái dữ liệu")
    st.write("Bảng phân tích được sinh từ Bronze → Silver → Gold/Serving. Dataset local: 206.431 dòng, SHA-256 `AA5BCA7FDAA8C89D4AB50A2ED2B94FB09F608ACC49FC1108A1A612E9C0F5A7AD`.")
    st.write("Các cảnh báo chất lượng được giữ ở Silver; ngưỡng 24 giờ và 50 km chỉ là cohort phân tích, không phải bộ lọc âm thầm.")
    info = tables["pipeline_run_info"]
    if info is not None and not info.empty:
        st.dataframe(info, use_container_width=True, hide_index=True)
    st.subheader("Tải bảng đang hiển thị")
    choices = {key: value for key, value in tables.items() if value is not None and not value.empty}
    selected_table = st.selectbox("Chọn bảng", list(choices), key="download_table")
    if selected_table:
        st.download_button("⬇️ Tải CSV", choices[selected_table].to_csv(index=False).encode("utf-8-sig"), file_name=f"{selected_table}.csv", mime="text/csv")

st.caption(f"Cập nhật giao diện: {datetime.now().strftime('%Y-%m-%d %H:%M')} · Dữ liệu được cache tối đa 120 giây.")
