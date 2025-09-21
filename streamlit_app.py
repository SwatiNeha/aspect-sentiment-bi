# streamlit_app.py
import streamlit as st
import pandas as pd
import sqlite3
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

# --- Dashboard Title ---
st.set_page_config(page_title="Feedback Dashboard", layout="wide")
st.title("📊 Feedback Dashboard")

# --- Auto Refresh ---
st_autorefresh(interval=60 * 1000, limit=None, key="refresh")  # refresh every 60 sec

# --- DB Connection ---
@st.cache_data(ttl=60)
def load_data():
    conn = sqlite3.connect("data/aspect_reviews.db")
    df_reviews = pd.read_sql("SELECT * FROM reviews_raw", conn)
    df_proc = pd.read_sql("SELECT * FROM reviews_processed", conn)
    conn.close()
    return df_reviews, df_proc

df_reviews, df_proc = load_data()
df_proc["datetime"] = pd.to_datetime(df_proc["processed_at"])
df_proc["date"] = df_proc["datetime"].dt.date

# --- Custom CSS ---
st.markdown("""
<style>
.kpi-box {
    background-color: #f0fff4;
    border-radius: 10px;
    padding: 15px;
    text-align: center;
    box-shadow: 0 1px 4px rgba(0,0,0,0.1);
    margin: 5px;
}
.kpi-value {
    font-size: 28px;
    font-weight: bold;
}
.kpi-label {
    font-size: 16px;
    color: #555;
}
div[data-baseweb="tag"] {
    background-color: #d3d3d3 !important;
    color: black !important;
    border-radius: 6px !important;
}
</style>
""", unsafe_allow_html=True)

# --- Sidebar Controls ---
st.sidebar.header("⚙️ Filters")

time_group = st.sidebar.radio(
    "View Sentiment Trend By:",
    ["Total", "Hourly", "Daily", "Weekly", "Monthly"],
    index=0
)

sentiment_options = ["Positive", "Negative", "Neutral"]
sentiments_selected = st.sidebar.multiselect(
    "Filter by Sentiment:",
    options=sentiment_options,
    default=sentiment_options
)

# --- Apply Sentiment Filter ---
df_filtered = df_proc.copy()
if sentiments_selected:
    df_filtered = df_filtered[df_filtered["sentiment_label"].isin([s.upper() for s in sentiments_selected])]

# --- Aggregate Data by Time ---
if time_group == "Hourly":
    df_filtered["hour"] = df_filtered["datetime"].dt.floor("H")
    trend_data = df_filtered.groupby("hour").agg(
        Average_Sentiment=("score_signed", "mean"),
        Review_Count=("review_id", "count")
    ).reset_index()
    latest_time = trend_data.iloc[-1]["hour"] if not trend_data.empty else None
    last_period = df_filtered[df_filtered["hour"] == latest_time] if latest_time is not None else pd.DataFrame()

elif time_group == "Daily":
    trend_data = df_filtered.groupby("date").agg(
        Average_Sentiment=("score_signed", "mean"),
        Review_Count=("review_id", "count")
    ).reset_index()
    latest_time = trend_data.iloc[-1]["date"] if not trend_data.empty else None
    last_period = df_filtered[df_filtered["date"] == latest_time] if latest_time is not None else pd.DataFrame()

elif time_group == "Weekly":
    # Floor to Monday of each week
    df_filtered["week_start"] = df_filtered["datetime"].dt.to_period("W-MON").dt.start_time

    # Aggregate by week_start
    trend_data = df_filtered.groupby("week_start").agg(
        Average_Sentiment=("score_signed", "mean"),
        Review_Count=("review_id", "count")
    ).reset_index()

    non_empty = trend_data[trend_data["Review_Count"] > 0]
    latest_time = non_empty.iloc[-1]["week_start"] if not non_empty.empty else None
    last_period = df_filtered[df_filtered["week_start"] == latest_time] if latest_time is not None else pd.DataFrame()

elif time_group == "Monthly":
    df_filtered["month_start"] = df_filtered["datetime"].dt.to_period("M").dt.start_time

    trend_data = df_filtered.groupby("month_start").agg(
        Average_Sentiment=("score_signed", "mean"),
        Review_Count=("review_id", "count")
    ).reset_index()

    non_empty = trend_data[trend_data["Review_Count"] > 0]
    latest_time = non_empty.iloc[-1]["month_start"] if not non_empty.empty else None
    last_period = df_filtered[df_filtered["month_start"] == latest_time] if latest_time is not None else pd.DataFrame()

else:  # Total → cumulative trend
    daily = df_filtered.groupby("date").agg(
        Daily_Sentiment=("score_signed", "mean"),
        Daily_Count=("review_id", "count")
    ).reset_index()
    daily["Cumulative_Count"] = daily["Daily_Count"].cumsum()
    daily["Cumulative_Sentiment"] = (
        (daily["Daily_Sentiment"] * daily["Daily_Count"]).cumsum()
        / daily["Cumulative_Count"]
    )
    trend_data = daily.rename(columns={"date": "datetime"})
    last_period = df_filtered  # total = all data



# --- KPI Values ---
n_pos = (last_period["sentiment_label"] == "POSITIVE").sum() if not last_period.empty else 0
n_neg = (last_period["sentiment_label"] == "NEGATIVE").sum() if not last_period.empty else 0
n_neu = (last_period["sentiment_label"] == "NEUTRAL").sum() if not last_period.empty else 0
avg_conf = last_period["score"].mean() if not last_period.empty else 0.00

total = n_pos + n_neg + n_neu
pos_pct = f"{(n_pos/total):.1%}" if total > 0 else "0%"
neg_pct = f"{(n_neg/total):.1%}" if total > 0 else "0%"
neu_pct = f"{(n_neu/total):.1%}" if total > 0 else "0%"

# --- KPI Display ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"<div class='kpi-box'><div class='kpi-label'>😀 Positive Feedback</div><div class='kpi-value'>{n_pos}</div><div>{pos_pct}</div></div>", unsafe_allow_html=True)
with col2:
    st.markdown(f"<div class='kpi-box'><div class='kpi-label'>😡 Negative Feedback</div><div class='kpi-value'>{n_neg}</div><div>{neg_pct}</div></div>", unsafe_allow_html=True)
with col3:
    st.markdown(f"<div class='kpi-box'><div class='kpi-label'>😐 Neutral Feedback</div><div class='kpi-value'>{n_neu}</div><div>{neu_pct}</div></div>", unsafe_allow_html=True)
with col4:
    st.markdown(f"<div class='kpi-box'><div class='kpi-label'>📏 Model Confidence</div><div class='kpi-value'>{avg_conf:.2f}</div></div>", unsafe_allow_html=True)

st.markdown("---")

# --- Sentiment Distribution ---
st.subheader("⚖️ Sentiment Distribution")
fig_donut = go.Figure(data=[go.Pie(
    labels=["Positive", "Negative", "Neutral"],
    values=[n_pos, n_neg, n_neu],
    hole=0.6,
    marker=dict(colors=["#90EE90", "#FFB6B6", "#ADD8E6"])
)])
fig_donut.update_traces(textinfo="label+percent")
st.plotly_chart(fig_donut, use_container_width=True)

# --- Sentiment Trend ---
st.subheader(f"📈 Sentiment Trend ({time_group})")
if not trend_data.empty:
    if time_group == "Total":
        st.line_chart(trend_data.set_index("datetime")[["Cumulative_Sentiment", "Cumulative_Count"]])
    else:
        index_col = trend_data.columns[0]  # dynamic: hour/date/year_week/year_month
        st.line_chart(trend_data.set_index(index_col)[["Average_Sentiment", "Review_Count"]])
else:
    st.info("No data available for selected filters.")

# --- Word Cloud ---
st.subheader("☁️ Word Cloud of Aspects / Techs")
if not df_filtered.empty:
    all_aspects = ",".join(df_filtered["aspect_csv"].dropna().astype(str))
    wc = WordCloud(width=800, height=400, background_color="white").generate(all_aspects)
    fig_wc, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig_wc)
else:
    st.info("No aspects available for selected filters.")


# --- Latest Feedback Table ---
st.subheader("📝 Latest Feedback")
df_display = df_reviews.rename(columns={
    "author": "User",
    "text": "Feedback",
    "created_at": "Date",
    "source": "Source" 
})[["User", "Feedback", "Date", "Source"]]
latest_feedback = df_display.sort_values("Date", ascending=False).head(10).reset_index(drop=True)
latest_feedback.index = latest_feedback.index + 1
latest_feedback.index.name = "S.No."
if not df_filtered.empty:
    st.dataframe(latest_feedback)
else:
    st.info("No feedback available for selected filters.")
