import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime
import json

st.set_page_config(
    page_title="PRISM Grandfathered Enrollment Dashboard",
    layout="wide",
    page_icon="📊"
)

BASE_URL = "https://api.iterable.com/api"

def get_headers(project: str) -> dict:
    key_map = {
        "preenrollment": st.secrets.get("ITERABLE_KEY_PREENROLLMENT", ""),
        "digbi_health":  st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH",  ""),
    }
    return {"Api-Key": key_map[project]}

# ─── DATA FETCHING ────────────────────────────────────────────────────────────

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_users_bulk(project: str, fields: list) -> list[dict]:
    """Bulk export all users from Iterable as NDJSON, return list of dicts."""
    params = {
        "dataTypeName": "user",
        "onlyFields": ",".join(fields),
    }
    resp = requests.get(
        f"{BASE_URL}/export/data/json",
        headers=get_headers(project),
        params=params,
        stream=True,
        timeout=300,
    )
    resp.raise_for_status()
    users = []
    for line in resp.iter_lines():
        if line:
            try:
                users.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return users

def parse_dates(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    df = df.copy()
    df["enrollmentDate"] = pd.to_datetime(df[date_col], errors="coerce")
    # Handle both unix ms timestamps and string dates like "2026-01-07"
    # If parsing failed but values look like integers, try unix ms
    mask_failed = df["enrollmentDate"].isna() & df[date_col].notna()
    if mask_failed.any():
        try:
            df.loc[mask_failed, "enrollmentDate"] = pd.to_datetime(
                df.loc[mask_failed, date_col].astype(float), unit="ms", errors="coerce"
            )
        except Exception:
            pass
    df["date"]  = df["enrollmentDate"].dt.normalize()
    df["month"] = df["enrollmentDate"].dt.to_period("M").astype(str)
    return df

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ Settings")
    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption("Data cached for 30 min.")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

st.title("📊 PRISM Grandfathered Enrollment Dashboard")
st.caption("Tracking total vs. enrolled PRISM Grandfathered users · Real-time from Iterable")

# Check secrets
missing_keys = []
if not st.secrets.get("ITERABLE_KEY_PREENROLLMENT"):
    missing_keys.append("`ITERABLE_KEY_PREENROLLMENT`")
if not st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH"):
    missing_keys.append("`ITERABLE_KEY_DIGBI_HEALTH`")

if missing_keys:
    st.error(f"Missing API key(s): {', '.join(missing_keys)} — add to Streamlit secrets.")
    st.stop()

# ─── LOAD DATA ────────────────────────────────────────────────────────────────

with st.spinner("Fetching total PRISM Grandfathered users from Digbi_preEnrollment..."):
    try:
        fields_pre = ["email", "prismGrandfathered", "enrollmentStatus"]
        raw_pre = fetch_users_bulk("preenrollment", fields_pre)
    except requests.HTTPError as e:
        st.error(f"Iterable API error (preEnrollment): {e.response.status_code} — {e.response.text[:300]}")
        st.stop()
    except Exception as e:
        st.error(f"Error fetching preEnrollment data: {e}")
        st.stop()

with st.spinner("Fetching enrolled PRISM Grandfathered users from Digbi Health..."):
    try:
        fields_dh = ["email", "prismGrandfathered", "enrollmentStatus", "enrollmentDateFormatted"]
        raw_dh = fetch_users_bulk("digbi_health", fields_dh)
    except requests.HTTPError as e:
        st.error(f"Iterable API error (Digbi Health): {e.response.status_code} — {e.response.text[:300]}")
        st.stop()
    except Exception as e:
        st.error(f"Error fetching Digbi Health data: {e}")
        st.stop()

# ─── FILTER ───────────────────────────────────────────────────────────────────

# Total GF: prismGrandfathered=true AND enrollmentStatus != TERMINATED
total_gf = []
for u in raw_pre:
    if u.get("prismGrandfathered") is True and u.get("enrollmentStatus") != "TERMINATED":
        total_gf.append({"email": u.get("email", "")})
df_total = pd.DataFrame(total_gf)

# Enrolled GF: prismGrandfathered=true AND enrollmentStatus=ENROLLED
enrolled_gf = []
for u in raw_dh:
    if u.get("prismGrandfathered") is True and u.get("enrollmentStatus") == "ENROLLED":
        enrolled_gf.append({
            "email":                    u.get("email", ""),
            "enrollmentStatus":         u.get("enrollmentStatus", ""),
            "enrollmentDateFormatted":  u.get("enrollmentDateFormatted"),
        })
df_enrolled = pd.DataFrame(enrolled_gf)

# Parse dates
if not df_enrolled.empty and "enrollmentDateFormatted" in df_enrolled.columns:
    df_enrolled = parse_dates(df_enrolled, "enrollmentDateFormatted")

# ─── KPI TILES ────────────────────────────────────────────────────────────────

total_count    = len(df_total)
enrolled_count = len(df_enrolled)
pct            = (enrolled_count / total_count * 100) if total_count > 0 else 0

col1, col2, col3 = st.columns(3)
col1.metric("Total PRISM Grandfathered", f"{total_count:,}",    help="From Digbi_preEnrollment · prismGrandfathered=true, not TERMINATED")
col2.metric("Enrolled in Digbi Health",  f"{enrolled_count:,}", help="From Digbi Health · enrollmentStatus=ENROLLED")
col3.metric("Enrollment Rate",           f"{pct:.1f}%")

st.markdown("---")

# ─── CHARTS ───────────────────────────────────────────────────────────────────

has_dates = (
    not df_enrolled.empty
    and "date" in df_enrolled.columns
    and df_enrolled["date"].notna().any()
)

if has_dates:
    tab1, tab2 = st.tabs(["📅 By Day", "📆 By Month"])

    with tab1:
        daily = (
            df_enrolled.dropna(subset=["date"])
            .groupby("date").size()
            .reset_index(name="New Enrollments")
            .sort_values("date")
        )
        daily["Cumulative"] = daily["New Enrollments"].cumsum()

        col_a, col_b = st.columns(2)
        with col_a:
            fig = px.bar(daily, x="date", y="New Enrollments",
                         title="New Enrollments by Day",
                         color_discrete_sequence=["#4F86C6"])
            fig.update_layout(hovermode="x unified", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        with col_b:
            fig2 = px.area(daily, x="date", y="Cumulative",
                           title="Cumulative Enrollments",
                           color_discrete_sequence=["#5CB85C"])
            fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig2, use_container_width=True)

        with st.expander("View daily data"):
            st.dataframe(daily, use_container_width=True, hide_index=True)

    with tab2:
        monthly = (
            df_enrolled.dropna(subset=["month"])
            .groupby("month").size()
            .reset_index(name="New Enrollments")
            .sort_values("month")
        )
        monthly["Cumulative"] = monthly["New Enrollments"].cumsum()

        col_a, col_b = st.columns(2)
        with col_a:
            fig3 = px.bar(monthly, x="month", y="New Enrollments",
                          title="New Enrollments by Month",
                          color_discrete_sequence=["#4F86C6"])
            fig3.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig3, use_container_width=True)
        with col_b:
            fig4 = px.area(monthly, x="month", y="Cumulative",
                           title="Cumulative Enrollments by Month",
                           color_discrete_sequence=["#5CB85C"])
            fig4.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig4, use_container_width=True)

        with st.expander("View monthly data"):
            st.dataframe(monthly, use_container_width=True, hide_index=True)

else:
    st.warning(
        "No enrollment dates found in the data yet. "
        "Charts will appear once `enrollmentDateFormatted` is populated in Iterable."
    )
    if not df_enrolled.empty:
        st.dataframe(df_enrolled[["email", "enrollmentStatus"]].head(50), use_container_width=True, hide_index=True)

st.markdown("---")
st.caption(f"Last loaded: {datetime.now().strftime('%b %d, %Y %I:%M %p')} · Source: Iterable API (Bulk Export)")
