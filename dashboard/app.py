"""wikiflow dashboard.

Run:  uv run --group dashboard streamlit run dashboard/app.py

A thin presentation layer. Every number comes from `wikiflow.metrics`, which is tested
independently; this file only decides how to show it.

Chart rules followed throughout, because each one prevents a specific misreading:

* **Colour follows the company, never its rank.** The mapping is fixed by the seed file
  order, so deselecting a company never repaints the others.
* **One measure per axis.** Views and share-of-traffic are different scales, so they are
  a toggle on one chart, never two y-axes on the same plot.
* **Gaps are shown as gaps.** A line drawn straight across missing hours implies data
  that does not exist, so lines break wherever an hour is absent.
* **Every chart has a table view**, and status is always an icon plus a word, never
  colour alone.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from wikiflow import metrics
from wikiflow.config import get_settings
from wikiflow.storage import configure_duckdb_s3

REPO_ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = Path(
    os.environ.get("WIKIFLOW_DUCKDB_PATH", REPO_ROOT / "dbt/warehouse/wikiflow.duckdb")
)
DBT_TARGET = Path(os.environ.get("WIKIFLOW_DBT_TARGET_DIR", REPO_ROOT / "dbt/target"))

# --- palette -----------------------------------------------------------------------
# Validated with the dataviz palette checker in this order (light surface): adjacent CVD
# separation >= 9.1, normal-vision >= 19.6. Three slots sit below 3:1 contrast, which is
# why every chart carries direct labels and a table view.
COMPANY_COLOURS: dict[str, str] = {
    "Amazon": "#2a78d6",
    "Apple Inc.": "#eb6834",
    "Microsoft": "#1baf7a",
    "Google": "#eda100",
    "Alphabet Inc.": "#e87ba4",
    "Meta Platforms": "#008300",
    "Facebook": "#4a3aa7",
}
DEFAULT_SELECTION = ["Amazon", "Apple Inc.", "Facebook", "Google", "Microsoft"]

INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SURFACE = "#fcfcfb"

AUDIT_SOURCE = f"s3://{get_settings().lake_bucket}/ops/ingestion_audit/*/*/*.parquet"


# --- data access -------------------------------------------------------------------


def _warehouse() -> duckdb.DuckDBPyConnection:
    # Read-only and short-lived: the transform DAG takes a write lock while dbt builds,
    # and a long-held reader would block it.
    return duckdb.connect(str(WAREHOUSE), read_only=True)


@st.cache_data(ttl=60, show_spinner=False)
def load_warehouse(selected: tuple[str, ...], language_qid: str | None) -> dict[str, object]:
    connection = _warehouse()
    try:
        company_rows = metrics.companies(connection)
        qid_by_name = {row["company_name"]: row["company_qid"] for row in company_rows}
        qids = [qid_by_name[name] for name in selected if name in qid_by_name]
        return {
            "summary": metrics.summary(connection),
            "companies": company_rows,
            "hourly": metrics.company_hourly(connection, qids),
            "languages": (
                metrics.language_breakdown(connection, language_qid) if language_qid else []
            ),
            "access": metrics.access_split(connection, qids),
        }
    finally:
        connection.close()


@st.cache_data(ttl=60, show_spinner=False)
def load_audit() -> list[dict[str, object]]:
    connection = duckdb.connect()
    try:
        configure_duckdb_s3(connection)
        return metrics.audit_attempts(connection, AUDIT_SOURCE, limit=200)
    finally:
        connection.close()


# --- chart helpers -----------------------------------------------------------------


def colour_scale() -> alt.Scale:
    # The full domain every time, so a company keeps its colour whatever is selected.
    return alt.Scale(domain=list(COMPANY_COLOURS), range=list(COMPANY_COLOURS.values()))


def styled(chart: alt.Chart | alt.LayerChart) -> alt.Chart | alt.LayerChart:
    return (
        chart.configure_view(strokeWidth=0, fill=SURFACE)
        .configure(background=SURFACE, font="system-ui, -apple-system, Segoe UI, sans-serif")
        .configure_axis(
            gridColor=GRID,
            gridWidth=1,
            domainColor=AXIS,
            tickColor=AXIS,
            labelColor=INK_MUTED,
            titleColor=INK_SECONDARY,
            labelFontSize=11,
            titleFontSize=11,
            titleFontWeight="normal",
        )
        .configure_legend(
            labelColor=INK_SECONDARY, titleColor=INK_SECONDARY, orient="bottom", symbolStrokeWidth=2
        )
    )


def with_gaps_as_breaks(frame: pd.DataFrame) -> pd.DataFrame:
    """Assign a segment id that changes wherever an hour is missing.

    Encoding `detail` on the segment draws each contiguous run as its own line, so the
    chart breaks at a gap instead of drawing a confident straight line across hours
    that were never loaded. This avoids depending on a particular Vega-Lite version's
    handling of null values.
    """
    frame = frame.sort_values(["company_name", "interval_start_utc"]).copy()
    step = frame.groupby("company_name")["interval_start_utc"].diff()
    new_segment = step.isna() | (step > pd.Timedelta(hours=1))
    frame["segment"] = (
        frame["company_name"]
        + "-"
        + new_segment.groupby(frame["company_name"]).cumsum().astype(str)
    )
    return frame


def attention_chart(frame: pd.DataFrame, measure: str, *, compress: bool) -> alt.LayerChart:
    field, title, fmt = (
        ("company_views", "Views per hour", ",.0f")
        if measure == "views"
        else ("share_of_traffic", "Share of all Wikimedia traffic", ".4%")
    )
    base = alt.Chart(frame).encode(
        x=alt.X("interval_start_utc:T", title="Hour (UTC)", axis=alt.Axis(format="%a %H:%M")),
        # symlog rather than log: it compresses a spike the same way but tolerates zero,
        # which a true log scale cannot draw.
        y=alt.Y(
            f"{field}:Q",
            title=f"{title} (compressed scale)" if compress else title,
            axis=alt.Axis(format=fmt),
            scale=alt.Scale(type="symlog") if compress else alt.Scale(),
        ),
        # The scale domain is every company (so colours never shift), but the legend
        # lists only what is drawn - advertising unselected series would be a lie.
        color=alt.Color(
            "company_name:N",
            scale=colour_scale(),
            title=None,
            legend=alt.Legend(values=sorted(frame["company_name"].unique().tolist())),
        ),
    )

    lines = base.mark_line(strokeWidth=2, interpolate="monotone").encode(detail="segment:N")

    # A segment with one point - an hour isolated between gaps - draws nothing as a line.
    # Without a dot it would be real data rendered invisible.
    sizes = frame.groupby("segment")["segment"].transform("size")
    isolated = (
        alt.Chart(frame[sizes == 1])
        .mark_point(filled=True, size=36, opacity=1)
        .encode(
            x="interval_start_utc:T",
            y=alt.Y(f"{field}:Q"),
            color=alt.Color("company_name:N", scale=colour_scale(), legend=None),
        )
    )

    # Crosshair: nearest hour, not nearest pixel - a 24px-plus hit area by construction.
    hover = alt.selection_point(
        fields=["interval_start_utc"],
        nearest=True,
        on="pointerover",
        clear="pointerout",
        empty=False,
    )
    catcher = (
        alt.Chart(frame)
        .mark_rule(strokeWidth=24, opacity=0)
        .encode(x="interval_start_utc:T")
        .add_params(hover)
    )
    rule = (
        alt.Chart(frame)
        .mark_rule(color=AXIS, strokeWidth=1)
        .encode(x="interval_start_utc:T")
        .transform_filter(hover)
    )
    points = (
        base.mark_point(filled=True, size=64, stroke=SURFACE, strokeWidth=2, opacity=1)
        .encode(
            tooltip=[
                alt.Tooltip("interval_start_utc:T", title="Hour (UTC)", format="%Y-%m-%d %H:00"),
                alt.Tooltip("company_name:N", title="Company"),
                alt.Tooltip("company_views:Q", title="Views", format=","),
                alt.Tooltip("share_of_traffic:Q", title="Share of traffic", format=".4%"),
            ]
        )
        .transform_filter(hover)
    )

    layers: list[alt.Chart] = [lines, isolated, catcher, rule, points]

    # Direct end labels only for a few series, and only where they would not collide.
    # With more series the legend carries identity; stacked, overlapping labels are
    # worse than none.
    if frame["company_name"].nunique() <= 4:
        last = frame.loc[frame.groupby("company_name")["interval_start_utc"].idxmax()]
        last = last.sort_values(field, ascending=False)
        spread = float(frame[field].max() - frame[field].min()) or 1.0
        kept, previous = [], None
        for _, row in last.iterrows():
            if previous is None or (previous - row[field]) / spread >= 0.06:
                kept.append(row)
                previous = row[field]
        if kept:
            layers.append(
                alt.Chart(pd.DataFrame(kept))
                .mark_text(align="left", dx=6, fontSize=11, color=INK_SECONDARY)
                .encode(x="interval_start_utc:T", y=f"{field}:Q", text="company_name:N")
            )

    # Label the single extreme point: it is the thing a reader will ask about, and the
    # label states what it was without guessing why it happened.
    peak = frame.loc[[frame[field].idxmax()]].copy()
    peak["note"] = peak.apply(
        lambda r: (
            f"{r['company_name']}: {r['company_views']:,} views, "
            f"{r['interval_start_utc']:%a %H:00} UTC"
        ),
        axis=1,
    )
    layers.append(
        alt.Chart(peak)
        .mark_text(align="left", dx=8, dy=-2, fontSize=11, color=INK)
        .encode(x="interval_start_utc:T", y=f"{field}:Q", text="note:N")
    )

    return alt.layer(*layers).properties(height=340)


def bar_chart(
    frame: pd.DataFrame,
    *,
    category: str,
    value: str,
    value_title: str,
    fmt: str,
    colour: alt.Color | str,
) -> alt.LayerChart:
    height = max(160, 26 * len(frame))
    # Ranked by value, except an "Other" bucket, which always goes last: sorted in among
    # real categories it reads as one of them.
    ranked = frame.sort_values(value, ascending=False)[category].tolist()
    order = [c for c in ranked if not str(c).startswith("Other")] + [
        c for c in ranked if str(c).startswith("Other")
    ]
    base = alt.Chart(frame).encode(
        y=alt.Y(f"{category}:N", title=None, sort=order, axis=alt.Axis(labelLimit=200)),
        x=alt.X(f"{value}:Q", title=value_title, axis=alt.Axis(format=fmt)),
    )
    # Thin bars with rounded data-ends; band padding leaves a surface gap between them.
    if isinstance(colour, alt.Color):
        bars = base.mark_bar(size=14, cornerRadiusEnd=4).encode(color=colour)
    else:
        bars = base.mark_bar(size=14, cornerRadiusEnd=4, color=colour)
    bars = bars.encode(
        tooltip=[alt.Tooltip(f"{category}:N"), alt.Tooltip(f"{value}:Q", format=fmt)]
    )
    # Value labels sit outside the bar end, so they never clip inside a short bar.
    labels = base.mark_text(align="left", dx=4, fontSize=11, color=INK_SECONDARY).encode(
        text=alt.Text(f"{value}:Q", format=fmt)
    )
    return alt.layer(bars, labels).properties(height=height)


def table_view(frame: pd.DataFrame, label: str = "Table view") -> None:
    with st.expander(label):
        st.dataframe(frame, hide_index=True, width="stretch")


def status_label(status: str) -> str:
    return {"success": "✅ success", "failed": "⛔ failed"}.get(status, f"❔ {status}")


def human_age(then: datetime | None) -> str:
    if then is None:
        return "never"
    reference = then if then.tzinfo else then.replace(tzinfo=UTC)
    minutes = int((datetime.now(UTC) - reference).total_seconds() // 60)
    if minutes < 90:
        return f"{minutes} min ago"
    if minutes < 48 * 60:
        return f"{minutes // 60} h ago"
    return f"{minutes // (60 * 24)} days ago"


# --- page --------------------------------------------------------------------------

st.set_page_config(page_title="wikiflow", page_icon="📈", layout="wide")

st.title("Attention on Wikipedia")
st.caption(
    "How public attention to five big tech companies moves hour by hour across every "
    "language edition of Wikipedia - and what share of all Wikimedia traffic it is."
)

if not WAREHOUSE.exists():
    st.error(
        f"No warehouse at `{WAREHOUSE}`. Build it first: "
        "`cd dbt && dbt build` (or let the pageviews_transform DAG run)."
    )
    st.stop()

# One filter row, above everything it scopes.
filters = st.columns([3, 2, 2, 2])
with filters[0]:
    selected = st.multiselect("Companies", options=list(COMPANY_COLOURS), default=DEFAULT_SELECTION)
with filters[1]:
    measure = st.radio(
        "Measure",
        options=["views", "share"],
        format_func=lambda m: "Views per hour" if m == "views" else "Share of all traffic",
        horizontal=True,
    )
with filters[2]:
    compress = st.toggle(
        "Compress spikes",
        help="A single-hour spike can flatten every other line. This switches the y-axis "
        "to a compressed (symlog) scale so smaller series stay readable.",
    )
with filters[3]:
    focus = st.selectbox(
        "Language breakdown for", options=selected or list(COMPANY_COLOURS), index=0
    )

try:
    probe = _warehouse()
    qid_lookup = {row["company_name"]: row["company_qid"] for row in metrics.companies(probe)}
    probe.close()
    data = load_warehouse(tuple(selected), qid_lookup.get(focus))
except duckdb.IOException:
    st.info(
        "The warehouse is being rebuilt by dbt right now (it holds a write lock while "
        "building). Refresh in a few seconds."
    )
    st.stop()

summary: metrics.WarehouseSummary = data["summary"]  # type: ignore[assignment]
reconciliation = metrics.reconciliation_status(DBT_TARGET / "run_results.json")

# --- headline tiles ----------------------------------------------------------------
tiles = st.columns(4)
tiles[0].metric(
    "Hours loaded",
    f"{summary.hours_loaded:,}",
    help=(
        f"{summary.first_hour:%Y-%m-%d %H:00} to {summary.last_hour:%Y-%m-%d %H:00} UTC"
        if summary.first_hour and summary.last_hour
        else None
    ),
    border=True,
)
tiles[1].metric(
    "Views analysed",
    f"{summary.total_views / 1e6:,.0f}M",
    help="Every view in every hour loaded, across all Wikimedia projects.",
    border=True,
)
tiles[2].metric(
    "Tracked companies' share",
    f"{summary.company_share:.4%}",
    help=f"{summary.company_views:,} views about the seven tracked entities.",
    border=True,
)
# Short values: a metric tile truncates long text at narrow widths, and a truncated
# verdict ("exact ma...") is worse than a terse one. The detail lives in the help text.
reconciliation_text = {
    "pass": "✅ pass",
    "fail": f"⛔ {reconciliation.mismatches} off",
    "not run": "❔ not run",
}.get(reconciliation.status, f"⛔ {reconciliation.status}")
tiles[3].metric(
    "Exact match with Wikimedia",
    reconciliation_text,
    help=(
        f"{summary.domain_hours:,} domain-hours compared against Wikimedia's published "
        f"projectviews. Recorded by dbt at {reconciliation.generated_at}."
        if reconciliation.generated_at
        else "dbt has not recorded this test yet."
    ),
    border=True,
)

# --- attention over time ------------------------------------------------------------
st.subheader("Attention over time")
hourly = pd.DataFrame(data["hourly"])  # type: ignore[arg-type]
if hourly.empty:
    st.info("Select at least one company.")
else:
    hourly["interval_start_utc"] = pd.to_datetime(hourly["interval_start_utc"])
    hourly = with_gaps_as_breaks(hourly)
    gaps = int(hourly.groupby("company_name")["segment"].nunique().max()) - 1
    st.altair_chart(
        styled(attention_chart(hourly, measure, compress=compress)), theme=None, width="stretch"
    )
    if gaps:
        st.caption(
            f"Lines break where hours are missing ({gaps} gap{'s' if gaps != 1 else ''}). "
            "A line drawn across a gap would imply data that was never loaded."
        )
    table_view(hourly[["interval_start_utc", "company_name", "company_views", "share_of_traffic"]])

# --- breakdowns ---------------------------------------------------------------------
left, right = st.columns(2)

with left:
    st.subheader(f"{focus}: by language edition")
    languages = pd.DataFrame(data["languages"])  # type: ignore[arg-type]
    if languages.empty:
        st.info("No views recorded for this company.")
    else:
        st.altair_chart(
            styled(
                bar_chart(
                    languages,
                    category="language",
                    value="views",
                    value_title="Views",
                    fmt=",.0f",
                    colour=COMPANY_COLOURS[focus],
                )
            ),
            theme=None,
            width="stretch",
        )
        if focus == "Apple Inc.":
            st.caption(
                "German Wikipedia titles the company article plain “Apple”; English "
                "disambiguates it as “Apple Inc.”. The German figure likely includes "
                "readers who were not looking for the company."
            )
        table_view(languages)

with right:
    st.subheader("Mobile share of views")
    access = pd.DataFrame(data["access"])  # type: ignore[arg-type]
    if access.empty:
        st.info("Select at least one company.")
    else:
        st.altair_chart(
            styled(
                bar_chart(
                    access,
                    category="company_name",
                    value="mobile_share",
                    value_title="Share of views from the mobile web",
                    fmt=".0%",
                    colour=alt.Color("company_name:N", scale=colour_scale(), legend=None),
                )
            ),
            theme=None,
            width="stretch",
        )
        table_view(access[["company_name", "views", "mobile_share"]])

# --- pipeline health -----------------------------------------------------------------
st.divider()
st.subheader("Pipeline health")
st.caption("Read from the ingestion audit trail: one record per attempt, failures included.")

try:
    audit = pd.DataFrame(load_audit())
except duckdb.Error as exc:
    st.warning(f"Could not read the audit trail from object storage: {exc}")
    audit = pd.DataFrame()

if audit.empty:
    st.info("No ingestion attempts recorded yet.")
else:
    successes = audit[audit["status"] == "success"]
    failures = audit[audit["status"] != "success"]
    last_success = (
        pd.to_datetime(successes["started_at"], utc=True).max() if not successes.empty else None
    )

    health = st.columns(4)
    health[0].metric("Attempts recorded", f"{len(audit):,}", border=True)
    health[1].metric(
        "Failed attempts",
        f"{len(failures):,}",
        help="Retried attempts that later succeeded still count here - that is the point.",
        border=True,
    )
    health[2].metric(
        "Median ingest time",
        f"{successes['duration_seconds'].median():,.0f}s" if not successes.empty else "-",
        border=True,
    )
    health[3].metric(
        "Last successful ingest",
        human_age(last_success.to_pydatetime() if last_success is not None else None),
        border=True,
    )

    shown = audit.copy()
    # Convert to UTC explicitly and drop the zone. Left tz-aware, both DuckDB and the
    # browser render these in the VIEWER's local zone - which put this table seven hours
    # out of step with the charts above, all of which are UTC.
    for column in ("started_at", "interval_start"):
        shown[column] = pd.to_datetime(shown[column], utc=True).dt.tz_convert(None)
    shown["status"] = shown["status"].map(status_label)
    shown["error"] = shown["error"].fillna("")
    st.dataframe(
        shown[
            [
                "started_at",
                "interval_start",
                "try_number",
                "status",
                "duration_seconds",
                "rows_scanned",
                "company_rows",
                "error",
            ]
        ],
        hide_index=True,
        width="stretch",
        column_config={
            "started_at": st.column_config.DatetimeColumn(
                "Attempted (UTC)", format="YYYY-MM-DD HH:mm"
            ),
            "interval_start": st.column_config.DatetimeColumn(
                "Hour (UTC)", format="YYYY-MM-DD HH:00"
            ),
            "try_number": st.column_config.NumberColumn("Try"),
            "duration_seconds": st.column_config.NumberColumn("Seconds", format="%.0f"),
            "rows_scanned": st.column_config.NumberColumn("Rows scanned", format="localized"),
            "company_rows": st.column_config.NumberColumn("Company rows"),
            "error": st.column_config.TextColumn("Error", width="large"),
        },
    )
