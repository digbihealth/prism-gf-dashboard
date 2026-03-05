import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import io

st.set_page_config(
    page_title="PRISM Grandfathered Enrollment Dashboard",
    layout="wide",
    page_icon="📊"
)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
# Store API keys in .streamlit/secrets.toml:
#   ITERABLE_KEY_PREENROLLMENT = "..."
#   ITERABLE_KEY_DIGBI_HEALTH  = "..."

BASE_URL = "https://api.iterable.com/api"

def get_headers(project: str) -> dict:
    key_map = {
        "preenrollment": st.secrets.get("ITERABLE_KEY_PREENROLLMENT", ""),
        "digbi_health":  st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH",  ""),
    }
    return {"Api-Key": key_map[project]}

# ─── DATA FETCHING ────────────────────────────────────────────────────────────

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_users_ndjson(project: str, only_fields: list[str]) -> list[dict]:
    """Stream bulk user export from Iterable (NDJSON)."""
    params = {
        "dataTypeName": "user",
        "onlyFields": ",".join(only_fields),
    }
    resp = requests.get(
        f"{BASE_URL}/export/data/json",
        headers=get_headers(project),
        params=params,
        stream=True,
        timeout=120,
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


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_list_users(project: str, list_id: int) -> list[dict]:
    """Export users from a static Iterable list."""
    resp = requests.get(
        f"{BASE_URL}/lists/getUsers",
        headers=get_headers(project),
        params={"listId": list_id},
        stream=True,
        timeout=120,
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


def build_grandfathered_df(users: list[dict]) -> pd.DataFrame:
    """Filter PRISM grandfathered users: prismGrandfathered=True AND enrollmentStatus != TERMINATED."""
    rows = []
    for u in users:
        fields = u.get("dataFields", u)  # handle both ndjson formats
        if fields.get("prismGrandfathered") is True:
            status = fields.get("enrollmentStatus", "")
            if status != "TERMINATED":
                rows.append({
                    "email":            u.get("email", fields.get("email", "")),
                    "enrollmentStatus": status,
                    "enrollmentDate":   fields.get("enrollmentDateFormatted")
                                        or fields.get("enrollmentDate")
                                        or fields.get("signUpDate")
                                        or fields.get("createdAt"),
                })
    return pd.DataFrame(rows)


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Robustly parse enrollment dates in multiple formats."""
    if df.empty or "enrollmentDate" not in df.columns:
        return df
    df = df.copy()
    df["enrollmentDate"] = pd.to_datetime(df["enrollmentDate"], errors="coerce", utc=True)
    df["enrollmentDate"] = df["enrollmentDate"].dt.tz_localize(None)
    df["date"] = df["enrollmentDate"].dt.normalize()
    df["month"] = df["enrollmentDate"].dt.to_period("M").astype(str)
    return df

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://digbi.health/wp-content/uploads/2023/09/digbi-health-logo.png",
             use_container_width=True, output_format="auto")
    st.markdown("---")
    st.header("⚙️ Settings")

    data_mode = st.radio(
        "Data source",
        ["Pre-created List IDs", "API — Bulk Export"],
        help="List IDs are faster. Bulk Export re-fetches all users from Iterable."
    )

    if data_mode == "Pre-created List IDs":
        list_id_total    = st.number_input("List ID — Total GF (preEnrollment project)", min_value=1, step=1, value=7948771)
        list_id_enrolled = st.number_input("List ID — Enrolled GF (Digbi Health project)", min_value=1, step=1, value=9021040)

    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption("Data cached for 30 min. Hit Refresh for latest.")

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
    st.error(
        f"⚠️ Missing API key(s): {', '.join(missing_keys)}\n\n"
        "Add them to `.streamlit/secrets.toml`:\n"
        "```toml\nITERABLE_KEY_PREENROLLMENT = \"your_key\"\n"
        "ITERABLE_KEY_DIGBI_HEALTH  = \"your_key\"\n```"
    )
    st.stop()

# ─── LOAD DATA ────────────────────────────────────────────────────────────────

with st.spinner("Fetching PRISM Grandfathered users from Iterable..."):
    try:
        if data_mode == "Pre-created List IDs" and list_id_total > 0 and list_id_enrolled > 0:
            raw_total    = fetch_list_users("preenrollment", int(list_id_total))
            raw_enrolled = fetch_list_users("digbi_health",  int(list_id_enrolled))
            df_total    = pd.DataFrame(raw_total)
            df_enrolled = pd.DataFrame(raw_enrolled)
            # When using pre-built lists, trust the list membership — just parse dates
            if not df_enrolled.empty:
                # Try to find a date column
                date_col = next((c for c in ["enrollmentDateFormatted", "enrollmentDate","signUpDate","createdAt"] if c in df_enrolled.columns), None)
                if date_col:
                    df_enrolled["enrollmentDate"] = df_enrolled[date_col]
            df_enrolled = parse_dates(df_enrolled) if not df_enrolled.empty else df_enrolled

        else:
            # Bulk export + filter
            fields_needed = ["email", "prismGrandfathered", "enrollmentStatus", "enrollmentDateFormatted", "enrollmentDate", "signUpDate", "createdAt"]

            raw_total    = fetch_users_ndjson("preenrollment", fields_needed)
            raw_enrolled = fetch_users_ndjson("digbi_health",  fields_needed)

            df_total    = build_grandfathered_df(raw_total)
            df_enrolled_raw = build_grandfathered_df(raw_enrolled)
            # Additional filter: must be ENROLLED
            df_enrolled = df_enrolled_raw[df_enrolled_raw["enrollmentStatus"] == "ENROLLED"].copy()
            df_enrolled = parse_dates(df_enrolled)

    except requests.HTTPError as e:
        st.error(f"Iterable API error: {e.response.status_code} — {e.response.text[:300]}")
        st.stop()
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        st.stop()

total_grandfathered = len(df_total)
total_enrolled      = len(df_enrolled)
pct_enrolled        = (total_enrolled / total_grandfathered * 100) if total_grandfathered > 0 else 0

# ─── KPI TILES ────────────────────────────────────────────────────────────────

col1, col2, col3 = st.columns(3)
col1.metric("Total PRISM Grandfathered", f"{total_grandfathered:,}", help="From Digbi_preEnrollment project")
col2.metric("Enrolled in Digbi Health", f"{total_enrolled:,}",      help="enrollmentStatus = ENROLLED")
col3.metric("Enrollment Rate",          f"{pct_enrolled:.1f}%")

st.markdown("---")

# ─── CHARTS ───────────────────────────────────────────────────────────────────

has_dates = not df_enrolled.empty and "date" in df_enrolled.columns and df_enrolled["date"].notna().any()

if has_dates:
    tab1, tab2 = st.tabs(["📅 By Day", "📆 By Month"])

    # ── BY DAY ──
    with tab1:
        daily = (
            df_enrolled.dropna(subset=["date"])
            .groupby("date")
            .size()
            .reset_index(name="Enrolled")
            .sort_values("date")
        )
        daily["Cumulative"] = daily["Enrolled"].cumsum()

        col_a, col_b = st.columns(2)

        with col_a:
            fig_daily = px.bar(
                daily, x="date", y="Enrolled",
                title="New Enrollments by Day",
                labels={"date": "Date", "Enrolled": "New Enrollments"},
                color_discrete_sequence=["#4F86C6"]
            )
            fig_daily.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                hovermode="x unified"
            )
            st.plotly_chart(fig_daily, use_container_width=True)

        with col_b:
            fig_cum = px.area(
                daily, x="date", y="Cumulative",
                title="Cumulative Enrollments",
                labels={"date": "Date", "Cumulative": "Total Enrolled"},
                color_discrete_sequence=["#5CB85C"]
            )
            fig_cum.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_cum, use_container_width=True)

        with st.expander("View daily data table"):
            st.dataframe(daily.rename(columns={"date": "Date"}), use_container_width=True, hide_index=True)

    # ── BY MONTH ──
    with tab2:
        monthly = (
            df_enrolled.dropna(subset=["month"])
            .groupby("month")
            .size()
            .reset_index(name="Enrolled")
            .sort_values("month")
        )
        monthly["Cumulative"] = monthly["Enrolled"].cumsum()

        col_a, col_b = st.columns(2)

        with col_a:
            fig_month = px.bar(
                monthly, x="month", y="Enrolled",
                title="New Enrollments by Month",
                labels={"month": "Month", "Enrolled": "New Enrollments"},
                color_discrete_sequence=["#4F86C6"]
            )
            fig_month.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_month, use_container_width=True)

        with col_b:
            fig_cum_m = px.area(
                monthly, x="month", y="Cumulative",
                title="Cumulative Enrollments by Month",
                labels={"month": "Month", "Cumulative": "Total Enrolled"},
                color_discrete_sequence=["#5CB85C"]
            )
            fig_cum_m.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_cum_m, use_container_width=True)

        with st.expander("View monthly data table"):
            st.dataframe(monthly.rename(columns={"month": "Month"}), use_container_width=True, hide_index=True)

else:
    st.info(
        "📅 No enrollment date field found in the data — showing summary only.\n\n"
        "To enable time-series charts, ensure `enrollmentDate`, `signUpDate`, or `createdAt` "
        "is populated in the Iterable user profile."
    )
    st.dataframe(
        df_enrolled[["email", "enrollmentStatus"]].head(100) if not df_enrolled.empty else pd.DataFrame(),
        use_container_width=True, hide_index=True
    )

# ─── FOOTER ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(f"Last loaded: {datetime.now().strftime('%b %d, %Y %I:%M %p')} · Source: Iterable API")
