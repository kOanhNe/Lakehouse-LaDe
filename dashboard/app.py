import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="Giao hàng Yantai - Dashboard", layout="wide")

URL = (f"mysql+pymysql://{os.getenv('MYSQL_USER', 'delivery')}:{os.getenv('MYSQL_PASSWORD', 'delivery')}"
       f"@{os.getenv('MYSQL_HOST', 'mysql')}:{os.getenv('MYSQL_PORT', '3306')}/"
       f"{os.getenv('MYSQL_DB', 'delivery_source')}")


@st.cache_resource
def engine():
    return create_engine(URL, pool_pre_ping=True)


@st.cache_data(ttl=120)
def load(table):
    try:
        return pd.read_sql(f"SELECT * FROM {table}", engine())
    except Exception:
        return None


courier = load("agg_courier_performance")
daily = load("agg_region_daily")
hourly = load("agg_hourly")
metrics = load("ml_metrics")
imp = load("ml_feature_importance")
reg_err = load("agg_region_error")
sample = load("eta_prediction_sample")
info = load("pipeline_run_info")
reg_sum = load("agg_region_summary")
overview = load("agg_overview")

st.title("Dashboard giao hàng Yantai (LaDe-D)")
if courier is None or daily is None:
    st.warning("Chưa có dữ liệu. Hãy chạy pipeline Prefect trước, rồi tải lại trang.")
    st.stop()

thr = float(courier["late_threshold_min"].iloc[0])
if info is not None and len(info):
    st.caption(f"Lần chạy pipeline gần nhất: {info['run_ts'].iloc[0]}.")

with st.expander("Cách đọc các chỉ số"):
    st.markdown(
        f"- **Thời gian xử lý**: từ lúc shipper nhận đơn đến lúc giao xong, tính bằng phút.\n"
        f"- **Trung vị**: mốc mà một nửa số đơn nhanh hơn. Dùng trung vị vì một số đơn kéo dài hàng chục giờ "
        f"làm số trung bình cao hơn thực tế.\n"
        f"- **Đơn chậm**: đơn có thời gian xử lý hơn {thr:.0f} phút (mốc mà 10% đơn lâu nhất của dữ liệu huấn luyện vượt qua).\n"
        f"- **Khu vực**: mã số khu vực do bộ dữ liệu LaDe cung cấp (đã ẩn danh), không có tên quận hoặc huyện.\n"
        f"- **ETA**: thời gian giao dự kiến, mô hình dự đoán ngay khi shipper nhận đơn.")

tab1, tab2, tab3, tab4 = st.tabs(["Tổng quan", "Shipper", "Khu vực & giờ", "Mô hình ETA"])

# ---------------------------------------------------------------- Tổng quan
with tab1:
    if overview is not None and len(overview):
        o = overview.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Tổng số đơn", f"{int(o['total_orders']):,}")
        c2.metric("Thời gian xử lý trung vị", f"{o['median_delivery_minutes']:,.0f} phút")
        c3.metric("Tỷ lệ đơn chậm", f"{o['late_rate']:.1%}")
        c4.metric("Đơn mất hơn 24 giờ", f"{o['over_24h_rate']:.1%}")
        c5.metric("Số shipper", f"{int(o['couriers']):,}")
        st.caption(f"Trung bình {o['avg_delivery_minutes']:,.0f} phút, cao hơn trung vị vì có các đơn kéo dài rất lâu. "
                   f"10% đơn lâu nhất mất hơn {o['p90_delivery_minutes']:,.0f} phút.")
    else:
        total = int(daily["total_orders"].sum())
        c1, c2 = st.columns(2)
        c1.metric("Tổng số đơn", f"{total:,}")
        c2.metric("Số shipper", f"{courier['courier_id'].nunique():,}")
        st.info("Chạy lại pipeline để có thêm các chỉ số tổng quan (trung vị, đơn mất hơn 24 giờ).")

    regions = sorted(daily["region_id"].unique())
    pick = st.multiselect("Khu vực (mã số)", regions, default=regions)
    d = daily[daily["region_id"].isin(pick)].copy()
    d["wm"] = d["avg_delivery_minutes"] * d["total_orders"]
    g = d.groupby("order_date").agg(total_orders=("total_orders", "sum"), wm=("wm", "sum")).reset_index()
    g["avg_delivery_minutes"] = g["wm"] / g["total_orders"]
    lbl = {"order_date": "Ngày", "total_orders": "Số đơn", "avg_delivery_minutes": "Phút"}
    a, b = st.columns(2)
    a.plotly_chart(px.line(g, x="order_date", y="total_orders", labels=lbl, title="Số đơn theo ngày"),
                   use_container_width=True)
    b.plotly_chart(px.line(g, x="order_date", y="avg_delivery_minutes", labels=lbl,
                           title="Thời gian xử lý trung bình theo ngày (phút)"), use_container_width=True)
    if sample is not None:
        cap = float(sample["actual_minutes"].quantile(0.99))
        fig = px.histogram(sample[sample["actual_minutes"] <= cap], x="actual_minutes", nbins=50,
                           labels={"actual_minutes": "Thời gian xử lý (phút)", "count": "Số đơn"},
                           title="Phân bố thời gian xử lý (mẫu 3.000 đơn thuộc tập kiểm tra, đã loại 1% đơn dài nhất)")
        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------ Shipper
with tab2:
    st.write("Xếp hạng shipper. Mỗi khu vực có đặc thù riêng (xem tab Khu vực & giờ), "
             "nên nên so sánh shipper trong cùng một khu vực.")
    f1, f2 = st.columns(2)
    reg_opts = sorted(courier["main_region_id"].dropna().unique())
    reg_pick = f1.multiselect("Khu vực chính của shipper", reg_opts, default=reg_opts)
    min_orders = f2.slider("Số đơn tối thiểu", 0, int(courier["total_orders"].max()), 200, step=50)
    metric_labels = {"late_rate": "Tỷ lệ đơn chậm", "median_delivery_minutes": "Thời gian xử lý trung vị",
                     "avg_delivery_minutes": "Thời gian xử lý trung bình", "total_orders": "Tổng số đơn"}
    key = st.selectbox("Xếp hạng theo", list(metric_labels), format_func=metric_labels.get)
    asc = st.checkbox("Thấp nhất trước (tốt nhất với thời gian và tỷ lệ chậm)", value=key != "total_orders")
    c = courier[(courier["total_orders"] >= min_orders) & (courier["main_region_id"].isin(reg_pick))]
    c = c.sort_values(key, ascending=asc)
    if c.empty:
        st.info("Không có shipper nào thỏa bộ lọc.")
    else:
        top = c.head(15).copy()
        top["shipper"] = "Shipper " + top["courier_id"].astype(str)
        direction = "thấp" if asc else "cao"
        fig = px.bar(top, x=key, y="shipper", orientation="h", text=key,
                     labels={key: metric_labels[key], "shipper": ""},
                     title=f"15 shipper có {metric_labels[key].lower()} {direction} nhất")
        fig.update_yaxes(type="category", autorange="reversed")
        fig.update_traces(texttemplate="%{text:.3g}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
        st.plotly_chart(
            px.scatter(c, x="total_orders", y="median_delivery_minutes", color="late_rate",
                       hover_data=["courier_id", "main_region_id"],
                       labels={"total_orders": "Số đơn", "median_delivery_minutes": "Thời gian xử lý trung vị (phút)",
                               "late_rate": "Tỷ lệ chậm", "courier_id": "Mã shipper",
                               "main_region_id": "Khu vực chính"},
                       title="Mỗi chấm là một shipper"), use_container_width=True)
        t = c.drop(columns=["late_threshold_min", "max_delivery_minutes"]).copy()
        t["late_rate"] = (t["late_rate"] * 100).round(1)
        t = t.rename(columns={
            "courier_id": "Mã shipper", "main_region_id": "Khu vực chính", "total_orders": "Số đơn",
            "avg_distance_km": "Khoảng cách TB (km)", "avg_delivery_minutes": "Thời gian TB (phút)",
            "median_delivery_minutes": "Trung vị (phút)", "late_rate": "Tỷ lệ chậm (%)",
            "avg_pred_eta_minutes": "ETA dự đoán TB (phút)",
            "mae_gbt_test": "Sai số dự đoán MAE (phút)"})
        st.dataframe(t, use_container_width=True, hide_index=True)
        st.caption("Hai cột ETA và sai số chỉ tính trên các đơn thuộc tập kiểm tra (giai đoạn cuối dữ liệu), "
                   "nên có thể trống với shipper không có đơn trong giai đoạn này.")

# ------------------------------------------------------------ Khu vực & giờ
with tab3:
    st.subheader("Theo khu vực")
    if reg_sum is None:
        st.info("Chạy lại pipeline để có bảng tổng hợp theo khu vực.")
    else:
        rs = reg_sum.copy()
        rs["khu_vuc"] = "KV " + rs["region_id"].astype(str)
        bad = rs[rs["over_24h_rate"] > 0.10].sort_values("over_24h_rate", ascending=False)
        for _, r_ in bad.iterrows():
            st.warning(f"Khu vực {int(r_['region_id'])}: {r_['over_24h_rate']:.0%} số đơn mất hơn 24 giờ "
                       f"(trung vị {r_['median_delivery_minutes']:,.0f} phút, {int(r_['total_orders']):,} đơn). "
                       f"Khu vực này vận hành khác các khu vực còn lại nên kéo số trung bình chung lên.")
        order = rs.sort_values("median_delivery_minutes")["khu_vuc"].tolist()
        a, b = st.columns(2)
        fig = px.bar(rs, x="khu_vuc", y="median_delivery_minutes",
                     labels={"khu_vuc": "Khu vực", "median_delivery_minutes": "Phút"},
                     title="Thời gian xử lý trung vị theo khu vực")
        fig.update_xaxes(type="category", categoryorder="array", categoryarray=order)
        a.plotly_chart(fig, use_container_width=True)
        fig = px.bar(rs, x="khu_vuc", y="late_rate",
                     labels={"khu_vuc": "Khu vực", "late_rate": "Tỷ lệ đơn chậm"},
                     title="Tỷ lệ đơn chậm theo khu vực")
        fig.update_xaxes(type="category", categoryorder="array", categoryarray=order)
        fig.update_yaxes(tickformat=".0%")
        b.plotly_chart(fig, use_container_width=True)
        t = rs.drop(columns=["khu_vuc"]).copy()
        for col in ["late_rate", "over_24h_rate"]:
            t[col] = (t[col] * 100).round(1)
        t = t.sort_values("median_delivery_minutes").rename(columns={
            "region_id": "Khu vực", "total_orders": "Số đơn", "couriers": "Số shipper",
            "avg_delivery_minutes": "TB (phút)", "median_delivery_minutes": "Trung vị (phút)",
            "p90_delivery_minutes": "P90 (phút)", "late_rate": "Tỷ lệ chậm (%)",
            "over_24h_rate": "Đơn > 24 giờ (%)"})
        st.dataframe(t, use_container_width=True, hide_index=True)

    st.subheader("Theo giờ nhận đơn")
    if hourly is not None:
        h = hourly.sort_values("accept_hour")
        a, b = st.columns(2)
        a.plotly_chart(px.bar(h, x="accept_hour", y="total_orders",
                              labels={"accept_hour": "Giờ nhận đơn", "total_orders": "Số đơn"},
                              title="Số đơn theo giờ nhận đơn"), use_container_width=True)
        hv = h[h["total_orders"] >= 300]
        b.plotly_chart(px.line(hv, x="accept_hour", y="median_delivery_minutes", markers=True,
                               labels={"accept_hour": "Giờ nhận đơn", "median_delivery_minutes": "Phút"},
                               title="Thời gian xử lý trung vị theo giờ nhận đơn"), use_container_width=True)
        st.caption("Phần lớn đơn được nhận trong khoảng 8-11 giờ sáng. Đơn nhận càng muộn trong ngày thì thời gian "
                   "xử lý càng ngắn. Biểu đồ bên phải chỉ vẽ các giờ có ít nhất 300 đơn vì giờ ít đơn thì số liệu "
                   "không ổn định.")

# ---------------------------------------------------------------- Mô hình ETA
with tab4:
    if metrics is None:
        st.info("Chưa có kết quả mô hình.")
    else:
        st.write("Dự đoán thời gian xử lý đơn ngay lúc shipper nhận đơn. Đánh giá trên tập kiểm tra theo thời gian "
                 "(các đơn sau mốc cắt), so với baseline đoán bằng một con số cố định.")
        st.dataframe(metrics.rename(columns={"model": "Mô hình", "cohort": "Tập đánh giá", "n": "Số đơn",
                                             "mae": "MAE (phút)", "rmse": "RMSE (phút)", "r2": "R²",
                                             "run_ts": "Thời điểm chạy"}),
                     use_container_width=True, hide_index=True)
        st.plotly_chart(px.bar(metrics, x="model", y=["mae", "rmse"], barmode="group",
                               labels={"value": "Phút", "model": "Mô hình", "variable": "Chỉ số"},
                               title="So sánh sai số giữa các mô hình (thấp hơn là tốt hơn)"),
                        use_container_width=True)
        st.caption("MAE là sai lệch trung bình giữa dự đoán và thực tế, tính bằng phút. "
                   "RMSE tương tự nhưng phạt nặng hơn các đơn sai nhiều. R² càng gần 1 càng tốt, "
                   "gần 0 hoặc âm nghĩa là mô hình không hơn đoán một con số cố định. "
                   "Benchmark bài báo gốc (nếu có) có thể đo trên bài toán và cách lọc dữ liệu khác, "
                   "nên chỉ mang tính tham khảo.")
    a, b = st.columns(2)
    if imp is not None:
        names = {"distance_km": "Khoảng cách", "accept_hour": "Giờ nhận đơn", "dow": "Thứ trong tuần",
                 "order_rank": "Thứ tự đơn trong ngày", "cum_avg_dist": "Khoảng cách TB các đơn trước",
                 "courier_train_avg_min": "Thời gian TB lịch sử của shipper", "aoi_type": "Loại điểm giao",
                 "region_id": "Khu vực"}
        i2 = imp.copy()
        i2["feature"] = i2["feature"].map(names).fillna(i2["feature"])
        a.plotly_chart(px.bar(i2.sort_values("importance"), x="importance", y="feature", orientation="h",
                              labels={"importance": "Mức quan trọng", "feature": ""},
                              title="Mô hình GBT dựa vào yếu tố nào nhiều nhất"), use_container_width=True)
    if reg_err is not None:
        re_ = reg_err.copy()
        re_["khu_vuc"] = "KV " + re_["region_id"].astype(str)
        fig = px.bar(re_, x="khu_vuc", y=["mae_baseline", "mae_lr", "mae_gbt"], barmode="group",
                     labels={"khu_vuc": "Khu vực", "value": "MAE (phút)", "variable": ""},
                     title="Sai số MAE theo khu vực (thấp hơn là tốt hơn)")
        fig.update_xaxes(type="category")
        mnames = {"mae_baseline": "Baseline (trung vị)", "mae_lr": "Linear Regression", "mae_gbt": "GBT"}
        fig.for_each_trace(lambda t: t.update(name=mnames.get(t.name, t.name)))
        b.plotly_chart(fig, use_container_width=True)
    if sample is not None:
        fig = px.scatter(sample, x="actual_minutes", y="pred_gbt", opacity=0.4,
                         labels={"actual_minutes": "Thực tế (phút)", "pred_gbt": "Dự đoán GBT (phút)"},
                         title="Thực tế và dự đoán (mẫu 3.000 đơn). Càng sát đường nét đứt càng chính xác")
        m = float(sample["actual_minutes"].quantile(0.99))
        fig.add_shape(type="line", x0=0, y0=0, x1=m, y1=m, line=dict(dash="dash"))
        fig.update_xaxes(range=[0, m])
        fig.update_yaxes(range=[0, m])
        st.plotly_chart(fig, use_container_width=True)