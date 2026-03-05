import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime
import json

st.set_page_config(
    page_title="PRISM Grandfathered Enrollment Dashboard",
    layout="wide",
    page_icon="👴"
)

BASE_URL    = "https://api.iterable.com/api"
CUTOFF_DATE    = pd.Timestamp("2025-12-01")
CAMPAIGN_START = pd.Timestamp("2026-03-03")

def get_headers(project: str) -> dict:
    key_map = {
        "preenrollment": st.secrets.get("ITERABLE_KEY_PREENROLLMENT", ""),
        "digbi_health":  st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH",  ""),
    }
    return {"Api-Key": key_map[project]}

# ─── DATA FETCHING ────────────────────────────────────────────────────────────

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_list_emails(project: str, list_id: int) -> list[str]:
    resp = requests.get(
        f"{BASE_URL}/lists/getUsers",
        headers=get_headers(project),
        params={"listId": list_id},
        stream=True,
        timeout=300,
    )
    resp.raise_for_status()
    emails = []
    for line in resp.iter_lines():
        if line:
            decoded = line.decode("utf-8") if isinstance(line, bytes) else line
            decoded = decoded.strip()
            if decoded:
                try:
                    obj = json.loads(decoded)
                    email = obj.get("email", "")
                    if email:
                        emails.append(email)
                except json.JSONDecodeError:
                    emails.append(decoded)
    return emails

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_user_fields(project: str, emails: tuple, fields: tuple) -> list[dict]:
    headers   = get_headers(project)
    results   = []
    batch_size = 100
    email_list = list(emails)
    progress   = st.progress(0, text="Loading user profiles...")
    for i in range(0, len(email_list), batch_size):
        batch = email_list[i:i+batch_size]
        try:
            resp = requests.post(
                f"{BASE_URL}/users/bulkGet",
                headers={**headers, "Content-Type": "application/json"},
                json={"emails": batch, "fields": list(fields)},
                timeout=30,
            )
            if resp.status_code == 200:
                for u in resp.json().get("users", []):
                    row = {"email": u.get("email", "")}
                    row.update(u.get("dataFields", {}))
                    results.append(row)
            else:
                for email in batch:
                    try:
                        r = requests.get(f"{BASE_URL}/users/{email}", headers=headers, timeout=10)
                        if r.status_code == 200:
                            u = r.json().get("user", {})
                            row = {"email": email}
                            row.update(u.get("dataFields", {}))
                            results.append(row)
                    except Exception:
                        results.append({"email": email})
        except Exception:
            for email in batch:
                results.append({"email": email})
        progress.progress(
            min(1.0, (i + batch_size) / max(len(email_list), 1)),
            text=f"Loading user profiles... {min(i+batch_size, len(email_list))}/{len(email_list)}"
        )
    progress.empty()
    return results

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

st.title("👴 PRISM Grandfathered Enrollment Dashboard")
st.caption("Tracking total vs. enrolled PRISM Grandfathered users · Real-time from Iterable · From Dec 1, 2025")

missing_keys = []
if not st.secrets.get("ITERABLE_KEY_PREENROLLMENT"):
    missing_keys.append("`ITERABLE_KEY_PREENROLLMENT`")
if not st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH"):
    missing_keys.append("`ITERABLE_KEY_DIGBI_HEALTH`")
if missing_keys:
    st.error(f"Missing API key(s): {', '.join(missing_keys)} — add to Streamlit secrets.")
    st.stop()

TOTAL_GF_LIST_ID    = 7948771
ENROLLED_GF_LIST_ID = 9021040

# ─── LOAD EMAILS ──────────────────────────────────────────────────────────────

with st.spinner("Fetching total PRISM GF users from preEnrollment..."):
    try:
        total_emails = fetch_list_emails("preenrollment", TOTAL_GF_LIST_ID)
        total_count  = len(total_emails)
    except requests.HTTPError as e:
        st.error(f"preEnrollment API error: {e.response.status_code} — {e.response.text[:200]}")
        st.stop()
    except Exception as e:
        st.error(f"Error: {e}")
        st.stop()

with st.spinner("Fetching enrolled PRISM GF users from Digbi Health..."):
    try:
        enrolled_emails = fetch_list_emails("digbi_health", ENROLLED_GF_LIST_ID)
        enrolled_count  = len(enrolled_emails)
    except requests.HTTPError as e:
        st.error(f"Digbi Health API error: {e.response.status_code} — {e.response.text[:200]}")
        st.stop()
    except Exception as e:
        st.error(f"Error: {e}")
        st.stop()

pct = (enrolled_count / total_count * 100) if total_count > 0 else 0.0

# ─── FETCH PROFILES ───────────────────────────────────────────────────────────

if enrolled_count > 0:
    user_data   = fetch_user_fields("digbi_health", tuple(enrolled_emails), ("enrollmentDateFormatted", "appDownload"))
    df_enrolled = pd.DataFrame(user_data)

    # Parse & filter dates
    has_date_col = (
        "enrollmentDateFormatted" in df_enrolled.columns
        and df_enrolled["enrollmentDateFormatted"].notna().any()
    )
    if has_date_col:
        df_enrolled = parse_dates(df_enrolled, "enrollmentDateFormatted")
        df_enrolled = df_enrolled[df_enrolled["date"].isna() | (df_enrolled["date"] >= CUTOFF_DATE)]
        has_dates   = df_enrolled["date"].notna().any()
    else:
        has_dates = False

    # App download rate
    if "appDownload" in df_enrolled.columns:
        downloaded     = df_enrolled["appDownload"].notna() & (df_enrolled["appDownload"].astype(str).str.strip() != "")
        app_dl_count   = int(downloaded.sum())
        app_dl_rate    = (app_dl_count / len(df_enrolled) * 100) if len(df_enrolled) > 0 else 0.0
    else:
        app_dl_count, app_dl_rate = 0, 0.0
else:
    has_dates = False
    app_dl_count, app_dl_rate = 0, 0.0

# Campaign enrollments — since Mar 3, 2026
if has_dates:
    campaign_enrolled = int((df_enrolled["date"] >= CAMPAIGN_START).sum())
else:
    campaign_enrolled = 0

# ─── KPI TILES ────────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total PRISM Grandfathered", f"{total_count:,}",        help="Digbi_preEnrollment · List 7948771")
c2.metric("Enrolled in Digbi Health",  f"{enrolled_count:,}",     help="Digbi Health · List 9021040")
c3.metric("Enrolled Since Mar 3",      f"{campaign_enrolled:,}",  help="Enrollments since campaign launch on Mar 3, 2026")
c4.metric("Enrollment Rate",           f"{pct:.1f}%",             help="Enrolled / Total Grandfathered")
c5.metric("App Download Rate",         f"{app_dl_rate:.1f}%",     help=f"{app_dl_count:,} enrolled members have downloaded the app")

st.markdown("---")

# ─── CHARTS & TABLES ──────────────────────────────────────────────────────────

if has_dates:
    tab1, tab2 = st.tabs(["📅 By Day", "📆 By Month"])

    # ── BY DAY ──
    with tab1:
        daily = (
            df_enrolled.dropna(subset=["date"])
            .groupby("date").size()
            .reset_index(name="New Enrollments")
            .sort_values("date")
        )
        daily["Cumulative"] = daily["New Enrollments"].cumsum()

        # Current week table — above charts
        today      = pd.Timestamp.today().normalize()
        week_start = today - pd.Timedelta(days=today.dayofweek)
        week_data  = daily[daily["date"] >= week_start].copy()
        week_data["date"] = week_data["date"].dt.strftime("%A, %b %d")

        st.markdown("#### 📋 This Week's Enrollments by Day")
        if not week_data.empty:
            st.dataframe(
                week_data.rename(columns={"date": "Day", "New Enrollments": "Enrollments", "Cumulative": "Cumulative Total"}),
                use_container_width=True, hide_index=True
            )
        else:
            st.info("No enrollments yet this week.")

        ca, cb = st.columns(2)
        with ca:
            fig = px.bar(daily, x="date", y="New Enrollments",
                         title="New Enrollments by Day",
                         color_discrete_sequence=["#4F86C6"])
            fig.update_layout(hovermode="x unified",
                              plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        with cb:
            fig2 = px.area(daily, x="date", y="Cumulative",
                           title="Cumulative Enrollments",
                           color_discrete_sequence=["#5CB85C"])
            fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig2, use_container_width=True)

    # ── BY MONTH ──
    with tab2:
        monthly = (
            df_enrolled.dropna(subset=["month"])
            .groupby("month").size()
            .reset_index(name="New Enrollments")
            .sort_values("month")
        )
        monthly["Cumulative"] = monthly["New Enrollments"].cumsum()

        # Monthly table — above charts
        st.markdown("#### 📋 Enrollments by Month")
        monthly_display = monthly.copy()
        monthly_display["month"] = pd.to_datetime(monthly_display["month"]).dt.strftime("%B %Y")
        st.dataframe(
            monthly_display.rename(columns={"month": "Month", "New Enrollments": "Enrollments", "Cumulative": "Cumulative Total"}),
            use_container_width=True, hide_index=True
        )

        ca, cb = st.columns(2)
        with ca:
            fig3 = px.bar(monthly, x="month", y="New Enrollments",
                          title="New Enrollments by Month",
                          color_discrete_sequence=["#4F86C6"])
            fig3.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig3, use_container_width=True)
        with cb:
            fig4 = px.area(monthly, x="month", y="Cumulative",
                           title="Cumulative Enrollments by Month",
                           color_discrete_sequence=["#5CB85C"])
            fig4.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig4, use_container_width=True)

else:
    st.info("No enrollment date data found — charts unavailable.")

st.markdown("---")
st.caption(f"Last loaded: {datetime.now().strftime('%b %d, %Y %I:%M %p')} · Source: Iterable API · Enrollments from Dec 1, 2025 onwards")
