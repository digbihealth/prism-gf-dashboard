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
def fetch_list_emails(project: str, list_id: int) -> list[str]:
    """Fetch plain email list from Iterable getUsers (returns one email per line)."""
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
                # Handle JSON or plain email
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
    """
    Fetch specific profile fields for a list of users.
    Uses /users/bulkGet — batches 100 at a time.
    Falls back to individual /users/{email} calls if bulk not available.
    """
    headers = get_headers(project)
    results = []
    batch_size = 100
    email_list = list(emails)
    
    progress = st.progress(0, text="Loading user profiles...")
    total_batches = max(1, len(email_list) // batch_size + 1)
    
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
                data = resp.json()
                for u in data.get("users", []):
                    row = {"email": u.get("email", "")}
                    row.update(u.get("dataFields", {}))
                    results.append(row)
            else:
                # Fallback: individual calls
                for email in batch:
                    try:
                        r = requests.get(
                            f"{BASE_URL}/users/{email}",
                            headers=headers,
                            timeout=10,
                        )
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

TOTAL_GF_LIST_ID    = 7948771  # Digbi_preEnrollment
ENROLLED_GF_LIST_ID = 9021040  # Digbi Health

# ─── LOAD DATA ────────────────────────────────────────────────────────────────

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
    except requests.HTTPError as e:
        st.error(f"Digbi Health API error: {e.response.status_code} — {e.response.text[:200]}")
        st.stop()
    except Exception as e:
        st.error(f"Error: {e}")
        st.stop()

# Fetch enrollment dates for enrolled users
enrolled_count = len(enrolled_emails)
pct = (enrolled_count / total_count * 100) if total_count > 0 else 0

# ─── KPI TILES ────────────────────────────────────────────────────────────────

c1, c2, c3 = st.columns(3)
c1.metric("Total PRISM Grandfathered", f"{total_count:,}",    help="From Digbi_preEnrollment · List 7948771")
c2.metric("Enrolled in Digbi Health",  f"{enrolled_count:,}", help="From Digbi Health · List 9021040")
c3.metric("Enrollment Rate",           f"{pct:.1f}%")

st.markdown("---")

# ─── FETCH DATES & CHARTS ─────────────────────────────────────────────────────

if enrolled_count > 0:
    user_data = fetch_user_fields(
        "digbi_health",
        tuple(enrolled_emails),
        ("enrollmentDateFormatted",)
    )
    df_enrolled = pd.DataFrame(user_data)

    has_date_col = (
        "enrollmentDateFormatted" in df_enrolled.columns
        and df_enrolled["enrollmentDateFormatted"].notna().any()
    )

    if has_date_col:
        df_enrolled = parse_dates(df_enrolled, "enrollmentDateFormatted")
        has_dates = df_enrolled["date"].notna().any()
    else:
        has_dates = False

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
        st.info("No `enrollmentDateFormatted` found in user profiles — charts unavailable.")
else:
    st.info("No enrolled users found in list 9021040.")

st.markdown("---")
st.caption(f"Last loaded: {datetime.now().strftime('%b %d, %Y %I:%M %p')} · Source: Iterable API")
