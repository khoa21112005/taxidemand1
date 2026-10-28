"""Streamlit website for the NYC Taxi Big Data project."""

from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="NYC Taxi Demand",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
PROJECT_DIR = APP_DIR.parent

st.markdown(
    """
    <style>
    .stApp { background: #f7f8fc; }
    .hero {
        padding: 1.4rem 1.6rem;
        border-radius: 18px;
        color: white;
        background: linear-gradient(120deg, #172554 0%, #1d4ed8 65%, #06b6d4 100%);
        margin-bottom: 1rem;
        box-shadow: 0 12px 32px rgba(29, 78, 216, .18);
    }
    .hero h1 { margin: 0; font-size: 2.05rem; }
    .hero p { margin: .45rem 0 0; opacity: .9; }
    .eyebrow {
        color: #2563eb;
        text-transform: uppercase;
        font-size: .78rem;
        font-weight: 800;
        letter-spacing: .12em;
        margin-bottom: .2rem;
    }
    .level-low, .level-medium, .level-high {
        padding: .55rem .8rem;
        border-radius: 10px;
        font-weight: 800;
        text-align: center;
    }
    .level-low { background: #dcfce7; color: #166534; }
    .level-medium { background: #fef3c7; color: #92400e; }
    .level-high { background: #fee2e2; color: #991b1b; }
    div[data-testid="stMetric"] {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 14px;
        padding: .9rem 1rem;
    }
    section[data-testid="stSidebar"] { background: #0f172a; }
    section[data-testid="stSidebar"] * { color: #e2e8f0; }
    section[data-testid="stSidebar"] div[role="radiogroup"] label {
        padding: .35rem .2rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data
def load_core_data():
    return {
        "hourly": pd.read_csv(DATA_DIR / "hourly_demand.csv"),
        "monthly": pd.read_csv(DATA_DIR / "monthly_demand.csv"),
        "top_zones": pd.read_csv(DATA_DIR / "top_zones.csv"),
        "weekday": pd.read_csv(DATA_DIR / "weekday_weekend.csv"),
        "kpi": pd.read_csv(DATA_DIR / "kpi.csv"),
        "zone_directory": pd.read_csv(DATA_DIR / "taxi_zone_lookup.csv"),
    }


@st.cache_data
def load_optional_csv(filename: str):
    path = DATA_DIR / filename
    return pd.read_csv(path) if path.exists() else None


@st.cache_data
def load_zone_lookup(zone_id: int):
    path = DATA_DIR / "prediction_lookup"
    if not path.exists():
        return None
    return pd.read_parquet(
        path,
        filters=[("zone_id", "==", int(zone_id))],
        engine="pyarrow",
    )


@st.cache_data(ttl=30)
def load_realtime():
    path = DATA_DIR / "realtime_events.csv"
    return pd.read_csv(path) if path.exists() else None


def render_header(title: str, subtitle: str):
    st.markdown(
        f"""
        <div class="hero">
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


required_files = [
    "hourly_demand.csv",
    "monthly_demand.csv",
    "top_zones.csv",
    "weekday_weekend.csv",
    "kpi.csv",
    "taxi_zone_lookup.csv",
]
missing_files = [name for name in required_files if not (DATA_DIR / name).exists()]
if missing_files:
    st.error(
        "Chưa có dữ liệu dashboard: "
        + ", ".join(missing_files)
        + ". Hãy chạy src/08_export_dashboard.py."
    )
    st.stop()

data = load_core_data()
hourly = data["hourly"]
monthly = data["monthly"]
top_zones = data["top_zones"]
weekday = data["weekday"]
kpi = data["kpi"]
zone_directory = (
    data["zone_directory"]
    .rename(columns={"LocationID": "zone_id"})
    .assign(zone_id=lambda frame: frame["zone_id"].astype(int))
    .sort_values("zone_id")
)


def enrich_zone_data(frame: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Attach the human-readable TLC zone name and borough."""
    zone_col = "zone_id" if "zone_id" in frame.columns else "pickup_zone_id"
    enriched = frame.copy()
    enriched[zone_col] = enriched[zone_col].astype(int)
    enriched = enriched.merge(
        zone_directory[["zone_id", "Zone", "Borough", "service_zone"]],
        left_on=zone_col,
        right_on="zone_id",
        how="left",
    )
    enriched["Zone"] = enriched["Zone"].fillna(
        "Zone " + enriched[zone_col].astype(str)
    )
    return enriched, zone_col

st.sidebar.markdown("## 🚕 Taxi Demand")
st.sidebar.caption("Big Data Demo • NYC 2023–2024")
page = st.sidebar.radio(
    "Điều hướng",
    [
        "Overview",
        "Analysis",
        "Prediction",
        "Realtime Kafka",
        "System / Pipeline",
    ],
)
st.sidebar.divider()
st.sidebar.caption("HDFS • Spark • Kafka • Airflow • Streamlit")


if page == "Overview":
    render_header(
        "NYC Taxi Demand Intelligence",
        "Từ 80 triệu chuyến taxi đến phân tích và dự đoán nhu cầu theo khu vực, ngày và giờ.",
    )
    values = kpi.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng số chuyến", f"{int(values['total_trips']):,}")
    c2.metric("Taxi zones", f"{int(values['total_zones']):,}")
    c3.metric("Số giờ phân tích", f"{int(values['total_hours']):,}")
    c4.metric("Demand grid rows", f"{int(values['grid_rows']):,}")

    st.markdown("### Bức tranh nhu cầu")
    left, right = st.columns([1.4, 1])
    with left:
        chart = hourly.copy().sort_values("pickup_hour")
        chart["pickup_hour"] = chart["pickup_hour"].astype(int)
        st.caption("Tổng số chuyến theo giờ trong ngày")
        st.line_chart(chart.set_index("pickup_hour")["total_trips"])
    with right:
        zones, _ = enrich_zone_data(top_zones.head(8))
        st.caption("Top 8 khu vực đón khách")
        st.bar_chart(zones.set_index("Zone")["total_trips"])

    metrics = load_optional_csv("model_metrics.csv")
    if metrics is not None and not metrics.empty:
        test_metric = metrics.loc[metrics["split"] == "test"].iloc[0]
        st.markdown("### Hiệu năng mô hình dự đoán")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Mô hình", str(test_metric["model"]))
        m2.metric("MAE", f"{test_metric['mae']:.2f}")
        m3.metric("RMSE", f"{test_metric['rmse']:.2f}")
        m4.metric("R²", f"{test_metric['r2']:.3f}")
        st.caption("Các metric trên tập test tháng 11–12/2024, tách hoàn toàn theo thời gian.")
    else:
        st.info("Phần phân tích đã sẵn sàng. Chạy task train_predict để hiển thị metric mô hình.")


elif page == "Analysis":
    render_header(
        "Demand Analysis",
        "Khám phá mùa vụ theo giờ, tháng, khu vực và loại ngày từ dữ liệu đã tổng hợp bằng Spark.",
    )

    st.markdown('<div class="eyebrow">Temporal patterns</div>', unsafe_allow_html=True)
    left, right = st.columns(2)
    with left:
        hourly_chart = hourly.copy().sort_values("pickup_hour")
        hourly_chart["pickup_hour"] = hourly_chart["pickup_hour"].astype(int)
        st.subheader("Nhu cầu theo 24 giờ")
        st.line_chart(hourly_chart.set_index("pickup_hour")["total_trips"])
    with right:
        month_chart = monthly.copy()
        month_chart["period"] = (
            month_chart["pickup_year"].astype(int).astype(str)
            + "-"
            + month_chart["pickup_month"].astype(int).astype(str).str.zfill(2)
        )
        month_chart = month_chart.sort_values(["pickup_year", "pickup_month"])
        st.subheader("Nhu cầu theo tháng")
        st.line_chart(month_chart.set_index("period")["total_trips"])

    st.markdown('<div class="eyebrow">Spatial & calendar patterns</div>', unsafe_allow_html=True)
    left, right = st.columns([1.5, 1])
    with left:
        zone_chart, zone_col = enrich_zone_data(top_zones)
        st.subheader("Top 15 pickup zones")
        st.bar_chart(zone_chart.set_index("Zone")["total_trips"])
    with right:
        st.subheader("Weekday vs Weekend")
        st.bar_chart(weekday.set_index("day_type")["total_trips"])

    with st.expander("Xem dữ liệu tổng hợp"):
        tab1, tab2, tab3 = st.tabs(["Theo giờ", "Theo tháng", "Theo zone"])
        tab1.dataframe(hourly, width="stretch", hide_index=True)
        tab2.dataframe(monthly, width="stretch", hide_index=True)
        zone_table, zone_col = enrich_zone_data(top_zones)
        tab3.dataframe(
            zone_table[[zone_col, "Zone", "Borough", "total_trips"]].rename(
                columns={
                    zone_col: "Zone ID",
                    "Zone": "Tên khu vực",
                    "Borough": "Quận",
                    "total_trips": "Tổng số chuyến",
                }
            ),
            width="stretch",
            hide_index=True,
        )


elif page == "Prediction":
    render_header(
        "Taxi Demand Prediction",
        "Random Forest của Spark ML dự đoán số chuyến trong một giờ dựa trên zone và đặc trưng lịch.",
    )

    metadata = load_optional_csv("model_metadata.csv")
    lookup_path = DATA_DIR / "prediction_lookup"
    if metadata is None or metadata.empty or not lookup_path.exists():
        st.warning(
            "Chưa có model output cho website. Hãy chạy src/06_train.py rồi src/08_export_dashboard.py."
        )
    else:
        meta = metadata.iloc[0]
        form_column, directory_column = st.columns([2.05, 1], gap="large")
        with form_column:
            with st.form("prediction_form"):
                st.markdown("### Chọn thời điểm cần dự đoán")
                c1, c2, c3 = st.columns(3)
                zone_id = c1.number_input(
                    "Zone ID", min_value=1, max_value=265, value=186
                )
                selected_date = c2.date_input("Ngày", value=date(2024, 12, 15))
                selected_hour = c3.selectbox(
                    "Giờ",
                    options=list(range(24)),
                    index=18,
                    format_func=lambda hour: f"{hour:02d}:00",
                )
                submitted = st.form_submit_button(
                    "Dự đoán nhu cầu", width="stretch"
                )

        with directory_column:
            st.markdown("### Danh mục Zone ID")
            st.caption("Tra tên khu vực và quận trước khi nhập Zone ID.")
            st.dataframe(
                zone_directory[["zone_id", "Zone", "Borough"]].rename(
                    columns={
                        "zone_id": "Zone ID",
                        "Zone": "Tên khu vực",
                        "Borough": "Quận",
                    }
                ),
                width="stretch",
                height=300,
                hide_index=True,
            )

        if submitted:
            with st.spinner("Đang tra cứu kết quả dự đoán..."):
                lookup = load_zone_lookup(int(zone_id))

            spark_day_of_week = ((selected_date.weekday() + 1) % 7) + 1
            match = lookup[
                (lookup["month"] == selected_date.month)
                & (lookup["day_of_week"] == spark_day_of_week)
                & (lookup["hour_of_day"] == selected_hour)
            ]

            if match.empty:
                st.error("Không tìm thấy tổ hợp dự đoán tương ứng.")
            else:
                predicted = max(0.0, float(match.iloc[0]["prediction"]))
                selected_zone = zone_directory.loc[
                    zone_directory["zone_id"] == int(zone_id)
                ].iloc[0]
                zone_name = str(selected_zone["Zone"])
                borough = str(selected_zone["Borough"])
                low_max = float(meta["low_max"])
                medium_max = float(meta["medium_max"])
                if predicted < low_max:
                    level, css_class = "LOW", "level-low"
                elif predicted < medium_max:
                    level, css_class = "MEDIUM", "level-medium"
                else:
                    level, css_class = "HIGH", "level-high"

                st.markdown("### Kết quả")
                r1, r2, r3, r4 = st.columns([1.15, .7, 1.15, 1.15])
                r1.metric("Dự đoán nhu cầu", f"{predicted:.0f} chuyến / giờ")
                r2.metric("Zone ID", f"{int(zone_id)}")
                r3.metric("Khu vực", zone_name)
                r4.metric(
                    "Thời điểm",
                    f"{selected_date:%d/%m/%Y} • {selected_hour:02d}:00",
                )
                st.caption(f"Địa chỉ zone: {zone_name}, {borough}.")
                st.markdown(
                    f'<div class="{css_class}">Mức nhu cầu: {level}</div>',
                    unsafe_allow_html=True,
                )

                daily_profile = lookup[
                    (lookup["month"] == selected_date.month)
                    & (lookup["day_of_week"] == spark_day_of_week)
                ].sort_values("hour_of_day")
                st.markdown("### Hồ sơ nhu cầu 24 giờ cùng ngày")
                st.line_chart(daily_profile.set_index("hour_of_day")["prediction"])
                st.caption(
                    f"Ngưỡng theo phân phối dự đoán: LOW < {low_max:.1f}; "
                    f"HIGH ≥ {medium_max:.1f} chuyến/giờ."
                )

        st.divider()
        m1, m2, m3 = st.columns(3)
        m1.metric("Model", str(meta["model"]))
        m2.metric("Train", str(meta["train_period"]))
        m3.metric("Test", str(meta["test_period"]))
        st.caption(
            "Đây là dự báo theo mẫu lịch sử, dùng các đặc trưng zone, giờ, thứ, tháng và weekend. "
            "Model không sử dụng dữ liệu tương lai khi đánh giá."
        )


elif page == "Realtime Kafka":
    render_header(
        "Realtime Taxi Events",
        "Theo dõi bản chụp sự kiện gần nhất từ Kafka → Spark Structured Streaming → HDFS.",
    )

    if st.button("Làm mới dữ liệu"):
        load_realtime.clear()
        st.rerun()

    realtime = load_realtime()
    if realtime is None or realtime.empty:
        st.info("Chưa có snapshot realtime. Website vẫn hoạt động; hãy khởi động Kafka stream và producer.")
        st.code(
            "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
            "src/07_kafka_stream.py\n"
            "spark-submit src/06_kafka_producer.py\n"
            "spark-submit src/08_export_dashboard.py",
            language="bash",
        )
    else:
        realtime["pickup_datetime"] = pd.to_datetime(
            realtime["pickup_datetime"], errors="coerce"
        )
        c1, c2, c3 = st.columns(3)
        c1.metric("Sự kiện trong snapshot", f"{len(realtime):,}")
        c2.metric("Zones đang hoạt động", f"{realtime['pickup_zone_id'].nunique():,}")
        c3.metric("Fare trung bình", f"${realtime['fare_amount'].mean():.2f}")

        left, right = st.columns([1.35, 1])
        with left:
            st.subheader("Sự kiện mới nhất")
            st.dataframe(
                realtime.sort_values("pickup_datetime", ascending=False),
                width="stretch",
                hide_index=True,
            )
        with right:
            st.subheader("Top zones trong stream")
            counts = realtime["pickup_zone_id"].value_counts().head(10)
            counts.index = counts.index.astype(str)
            st.bar_chart(counts)


else:
    render_header(
        "Big Data System",
        "Kiến trúc end-to-end từ dữ liệu thô đến mô hình dự đoán và website demo.",
    )

    st.code(
        """NYC Taxi Parquet (2023–2024)
              │
              ▼
        Hadoop HDFS ──► Spark Cleaning ──► Spark SQL
                                             │
                                             ▼
                                       Demand Grid
                                       zone × hour
                                             │
                                             ▼
                                    Spark ML Random Forest
                                             │
                                             ▼
                           Predictions + Metrics + Lookup
                                             │
                                             ▼
                                     Streamlit Website

        Kafka ──► Spark Structured Streaming ──► HDFS snapshot
        Airflow ──► Điều phối và kiểm tra toàn bộ pipeline""",
        language="text",
    )

    model_ready = (DATA_DIR / "model_metadata.csv").exists()
    lookup_ready = (DATA_DIR / "prediction_lookup").exists()
    realtime_ready = (DATA_DIR / "realtime_events.csv").exists()
    dag_ready = (PROJECT_DIR / "dags" / "taxi_bigdata_pipeline.py").exists()
    status = pd.DataFrame(
        [
            ["HDFS / Demand Grid", "READY", "4.6M zone × hour rows"],
            ["Spark SQL Aggregates", "READY", "CSV tổng hợp cho dashboard"],
            ["Spark ML Prediction", "READY" if model_ready else "PENDING", "Random Forest"],
            ["Prediction Lookup", "READY" if lookup_ready else "PENDING", "Dữ liệu Parquet theo zone"],
            ["Kafka Streaming", "READY" if realtime_ready else "WAITING", "Snapshot 200 events gần nhất"],
            ["Airflow DAG", "READY" if dag_ready else "PENDING", "taxi_bigdata_pipeline"],
        ],
        columns=["Thành phần", "Trạng thái", "Output"],
    )
    st.subheader("Trạng thái demo")
    st.dataframe(status, width="stretch", hide_index=True)

    st.subheader("Airflow workflow")
    st.code(
        "start → check_hdfs → clean_data → spark_sql → demand_grid\n"
        "      → save_results → train_predict → export_dashboard\n"
        "      → check_output → end",
        language="text",
    )

    with st.expander("Đường dẫn output chính"):
        st.code(
            "hdfs://localhost:9000/taxi-demand/processed/demand_hourly\n"
            "hdfs://localhost:9000/taxi-demand/models/random_forest_demand\n"
            "hdfs://localhost:9000/taxi-demand/output/demand_predictions\n"
            "hdfs://localhost:9000/taxi-demand/output/model_metrics\n"
            "/home/khoa2/taxi-demand/dashboard/data",
            language="text",
        )

st.divider()
st.caption(
    "Big Data Taxi Demand Project • Hadoop HDFS • Apache Spark • Spark ML • Kafka • Airflow"
)
