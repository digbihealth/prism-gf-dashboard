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
def fetch_list_users(project: str, list_id: int) -> list[dict]:
    """
    Fetch users from a list using getUsers endpoint (works with server-side keys).
    Returns list of user dicts.
    """
    users = []
    resp = requests.get(
        f"{BASE_URL}/lists/getUsers",
        headers=get_headers(project),
        params={"listId": list_id},
        stream=True,
        timeout=300,
    )
    resp.raise_for_status()
    for line in resp.iter_lines():
        if line:
            try:
                obj = json.loads(line)
                # getUsers returns {email, dataFields} or flat user object
                if "dataFields" in obj:
                    flat = {"email": obj.get("email", "")}
                    flat.update(obj["dataFields"])
                    users.append(flat)
                else:
                    users.append(obj)
            except json.JSONDecodeError:
                continue
    return users

@st.cache_data(ttl=1800, show_spinner=False)  
def fetch_list_count(project: str, list_id: int) -> int:
    """Get user count for a list."""
    resp = requests.get(
        f"{BASE_URL}/lists",
        headers=get_headers(project),
        timeout=30,
    )
    resp.raise_for_status()
    lists = resp.json().get("lists", [])
    for l in lists:
        if l.get("id") == list_id:
            return l.get("userCount", 0) or l.get("size", 0)
    return 0

def parse_dates(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    df = df.copy()
    df["enrollmentDate"] = pd.to_datetime(df[date_col], errors="coerce")
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

missing_keys = []
if not st.secrets.get("ITERABLE_KEY_PREENROLLMENT"):
    missing_keys.append("`ITERABLE_KEY_PREENROLLMENT`")
if not st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH"):
    missing_keys.append("`ITERABLE_KEY_DIGBI_HEALTH`")

if missing_keys:
    st.error(f"Missing API key(s): {', '.join(missing_keys)} — add to Streamlit secrets.")
    st.stop()

# List IDs
TOTAL_GF_LIST_ID    = 7948771  # Digbi_preEnrollment project
ENROLLED_GF_LIST_ID = 9021040  # Digbi Health project

# ─── LOAD DATA ────────────────────────────────────────────────────────────────

col_load1, col_load2 = st.columns(2)

with col_load1:
    with st.spinner("Fetching total PRISM GF users..."):
        try:
            # First try to get count from list metadata (fast)
            total_count = fetch_list_count("preenrollment", TOTAL_GF_LIST_ID)
            if total_count == 0:
                # Fallback: fetch actual users and count
                raw_total = fetch_list_users("preenrollment", TOTAL_GF_LIST_ID)
                total_count = len(raw_total)
        except requests.HTTPError as e:
            st.error(f"preEnrollment API error: {e.response.status_code}")
            st.stop()
        except Exception as e:
            st.error(f"Error: {e}")
            st.stop()

with col_load2:
    with st.spinner("Fetching enrolled PRISM GF users..."):
        try:
            raw_enrolled = fetch_list_users("digbi_health", ENROLLED_GF_LIST_ID)
            df_enrolled = pd.DataFrame(raw_enrolled)
        except requests.HTTPError as e:
            st.error(f"Digbi Health API error: {e.response.status_code}")
            st.stop()
        except Exception as e:
            st.error(f"Error: {e}")
            st.stop()

# Parse dates
if not df_enrolled.empty:
    date_col = next(
        (c for c in ["enrollmentDateFormatted", "enrollmentDate", "signUpDate", "createdAt"]
         if c in df_enrolled.columns and df_enrolled[c].notna().any()),
        None
    )
    if date_col:
        df_enrolled = parse_dates(df_enrolled, date_col)

enrolled_count = len(df_enrolled)
pct = (enrolled_count / total_count * 100) if total_count > 0 else 0

# ─── KPI TILES ────────────────────────────────────────────────────────────────

c1, c2, c3 = st.columns(3)
c1.metric("Total PRISM Grandfathered", f"{total_count:,}",    help="From Digbi_preEnrollment · List 7948771")
c2.metric("Enrolled in Digbi Health",  f"{enrolled_count:,}", help="From Digbi Health · List 9021040")
c3.metric("Enrollment Rate",           f"{pct:.1f}%")

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

        ca, cb = st.columns(2)
        with ca:
            fig = px.bar(daily, x="date", y="New Enrollments",
                         title="New Enrollments by Day",
                         color_discrete_sequence=["#4F86C6"])
            fig.update_layout(hovermode="x unified",
                              plot_bgcolor="rgba(0,0,0,0)",
                              paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        with cb:
            fig2 = px.area(daily, x="date", y="Cumulative",
                           title="Cumulative Enrollments",
                           color_discrete_sequence=["#5CB85C"])
            fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)",
                               paper_bgcolor="rgba(0,0,0,0)")
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

        ca, cb = st.columns(2)
        with ca:
            fig3 = px.bar(monthly, x="month", y="New Enrollments",
                          title="New Enrollments by Month",
                          color_discrete_sequence=["#4F86C6"])
            fig3.update_layout(plot_bgcolor="rgba(0,0,0,0)",
                               paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig3, use_container_width=True)
        with cb:
            fig4 = px.area(monthly, x="month", y="Cumulative",
                           title="Cumulative Enrollments by Month",
                           color_discrete_sequence=["#5CB85C"])
            fig4.update_layout(plot_bgcolor="rgba(0,0,0,0)",
                               paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig4, use_container_width=True)

        with st.expander("View monthly data"):
            st.dataframe(monthly, use_container_width=True, hide_index=True)

else:
    st.info("Charts will appear once enrollment date data is available.")
    if not df_enrolled.empty:
        st.dataframe(df_enrolled.head(50), use_container_width=True, hide_index=True)

st.markdown("---")
st.caption(f"Last loaded: {datetime.now().strftime('%b %d, %Y %I:%M %p')} · Source: Iterable API")
