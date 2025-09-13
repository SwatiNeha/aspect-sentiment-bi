# streamlit_app.py
import streamlit as st
import pandas as pd
import sqlite3
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh   # 👈 add this

# --- Auto Refresh ---
# refresh every 60 seconds (60000 ms)
st_autorefresh(interval=60 * 1000, limit=None, key="refresh")

# --- DB Connection ---
@st.cache_data
def load_data():
    conn = sqlite3.connect("data/aspect_reviews.db")
    df_reviews = pd.read_sql("SELECT * FROM reviews_raw", conn)
    df_proc = pd.read_sql("SELECT * FROM reviews_processed", conn)
    conn.close()
    return df_reviews, df_proc

df_reviews, df_proc = load_data()
df_proc["date"] = pd.to_datetime(df_proc["processed_at"]).dt.date

# --- Custom CSS for Sidebar Filters ---
st.markdown("""
<style>
/* Multiselect selected item background (tags) */
div[data-baseweb="tag"] {
    background-color: #d3d3d3 !important; /* gray */
    color: black !important;
    border: 1px solid #a9a9a9 !important;
    border-radius: 6px !important;
    padding: 2px 6px !important;
}

/* Hover effect inside dropdown */
div[data-baseweb="select"] div[role="option"]:hover {
    background-color: #e0e0e0 !important;
    color: black !important;
}

/* Radio button selected option styling */
div[role="radiogroup"] > div[aria-checked="true"] {
    background-color: #d3d3d3 !important;
    border-radius: 6px;
    padding: 2px 6px;
    color: black !important;
}
</style>
""", unsafe_allow_html=True)

# --- Sidebar Controls ---
st.sidebar.header("⚙️ Filters")

time_group = st.sidebar.radio(
    "View Sentiment Trend By:",
    ["Daily", "Weekly", "Monthly"],
    index=0
)

sentiment_options = ["Positive", "Negative", "Neutral"]
sentiments_selected = st.sidebar.multiselect(
    "Filter by Sentiment:",
    options=sentiment_options,
    default=sentiment_options
)

# --- Apply Sentiment Filter ---
if sentiments_selected:
    df_proc = df_proc[df_proc["sentiment_label"].isin([s.upper() for s in sentiments_selected])]

# --- Aggregate Data by Time ---
if time_group == "Weekly":
    trend_data = (
        df_proc.groupby(pd.Grouper(key="date", freq="W"))
        .agg(Average_Sentiment=("score_signed", "mean"),
             Review_Count=("review_id", "count"))
        .reset_index()
    )
elif time_group == "Monthly":
    trend_data = (
        df_proc.groupby(pd.Grouper(key="date", freq="M"))
        .agg(Average_Sentiment=("score_signed", "mean"),
             Review_Count=("review_id", "count"))
        .reset_index()
    )
else:  # Daily
    trend_data = (
        df_proc.groupby("date")
        .agg(Average_Sentiment=("score_signed", "mean"),
             Review_Count=("review_id", "count"))
        .reset_index()
    )

# --- Dashboard Title ---
st.set_page_config(page_title="Feedback Dashboard", layout="wide")
st.title("📊 Feedback Dashboard")

# --- KPI Values ---
n_pos = (df_proc["sentiment_label"] == "POSITIVE").sum()
n_neg = (df_proc["sentiment_label"] == "NEGATIVE").sum()
n_neu = (df_proc["sentiment_label"] == "NEUTRAL").sum()
avg_conf = df_proc["score"].mean() if "score" in df_proc.columns else 0.70

# --- Custom KPI Styling ---
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
</style>
""", unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"<div class='kpi-box'><div class='kpi-label'>😀 Positive Feedback</div><div class='kpi-value'>{n_pos}</div><div>{n_pos/len(df_proc):.1%}</div></div>", unsafe_allow_html=True)
with col2:
    st.markdown(f"<div class='kpi-box'><div class='kpi-label'>😡 Negative Feedback</div><div class='kpi-value'>{n_neg}</div><div>{n_neg/len(df_proc):.1%}</div></div>", unsafe_allow_html=True)
with col3:
    st.markdown(f"<div class='kpi-box'><div class='kpi-label'>😐 Neutral Feedback</div><div class='kpi-value'>{n_neu}</div><div>{n_neu/len(df_proc):.1%}</div></div>", unsafe_allow_html=True)
with col4:
    st.markdown(f"<div class='kpi-box'><div class='kpi-label'>📏 Model Confidence</div><div class='kpi-value'>{avg_conf:.2f}</div></div>", unsafe_allow_html=True)

st.markdown("---")

# --- Sentiment Distribution ---
st.subheader("⚖️ Sentiment Distribution")
fig_donut = go.Figure(data=[go.Pie(
    labels=["Positive", "Negative", "Neutral"],
    values=[n_pos, n_neg, n_neu],
    hole=0.6,
    marker=dict(colors=["#90EE90", "#FFB6B6", "#ADD8E6"]),
)])
fig_donut.update_traces(textinfo="label+percent", insidetextorientation="radial")
st.plotly_chart(fig_donut, use_container_width=True)

# --- Sentiment Trend ---
st.subheader(f"📈 Sentiment Trend ({time_group})")
st.line_chart(trend_data.set_index("date")[["Average_Sentiment", "Review_Count"]])

# --- Word Cloud ---
st.subheader("☁️ Word Cloud of Aspects / Techs")
all_aspects = ",".join(df_proc["aspect_csv"].dropna().astype(str))
wc = WordCloud(width=800, height=400, background_color="white").generate(all_aspects)
fig_wc, ax = plt.subplots(figsize=(10, 5))
ax.imshow(wc, interpolation="bilinear")
ax.axis("off")
st.pyplot(fig_wc)

# --- Latest Feedback Table ---
st.subheader("📝 Latest Feedback")
df_display = df_reviews.rename(columns={
    "author": "User",
    "text": "Feedback",
    "created_at": "Date"
})
st.dataframe(df_display.sort_values("Date", ascending=False).head(10))
