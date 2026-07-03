"""
generate_data.py
Reads attribution tables from BigQuery → writes JSON files to data/
Runs daily via GitHub Actions. Also supports the transition from old table schemas
(loaded by load_bq.bat / ads_script_v2) to new schemas (from ads_script_v3).

Output files:
  data/meta.json           — last_updated + date range available
  data/campaign_stats.json — daily spend/conv/revenue (all 365 days; dashboard filters)
  data/assisted_conv.json  — campaign role + assist ratios
  data/time_lag.json       — days-to-conversion histogram
  data/path_length.json    — interactions-to-conversion histogram
  data/conv_paths.json     — top conversion paths
  data/conv_breakdown.json — conversion action breakdown
"""

import json, os
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT = "flawlessfine-ga4"
DATASET = "attribution_results"
OUT     = "data"

client = bigquery.Client(project=PROJECT)
os.makedirs(OUT, exist_ok=True)


def q(sql: str) -> list[dict]:
    rows = client.query(sql).result()
    return [dict(row) for row in rows]


def save(fname: str, payload) -> None:
    path = os.path.join(OUT, fname)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, default=str, ensure_ascii=False)
    size = os.path.getsize(path)
    print(f"  {fname}  ({size} bytes)")


def f2(v) -> float:
    return round(float(v or 0), 2)


# ─────────────────────────────────────────────────────────────────────────────
# Check which column names exist in a table
# ─────────────────────────────────────────────────────────────────────────────
def cols(table: str) -> set:
    try:
        t = client.get_table(f"{PROJECT}.{DATASET}.{table}")
        return {s.name for s in t.schema}
    except Exception:
        return set()


# ─────────────────────────────────────────────────────────────────────────────
# 1. CAMPAIGN STATS
#    v3 schema: conversions, revenue, all_conversions_value
#    v2 schema: conversions_lastclick, revenue_lastclick, all_conv_value
# ─────────────────────────────────────────────────────────────────────────────
def gen_campaign_stats():
    c = cols("ads_campaign_stats")
    if not c:
        print("  ads_campaign_stats not found - skipping")
        return

    # Column name adapters (v3 schema vs old schema)
    conv_col = "conversions"           if "conversions"           in c else "conversions_lastclick"
    rev_col  = "revenue"               if "revenue"               in c else "revenue_lastclick"
    allv_col = "all_conversions_value" if "all_conversions_value" in c else "all_conv_value"

    live     = cols("ads_campaign_stats_live")
    history  = cols("ads_campaign_stats_history")

    live_conv = "conversions"           if live and "conversions"           in live else conv_col
    live_rev  = "revenue"               if live and "revenue"               in live else rev_col
    live_allv = "all_conversions_value" if live and "all_conversions_value" in live else allv_col

    # ── BUILD CTEs ───────────────────────────────────────────────────────────
    # live   = last 3 days (hourly script), highest priority
    # recent = ads_campaign_stats (90 days), excludes live dates
    # hist   = ads_campaign_stats_history (all time backfill), excludes recent dates

    live_cte = f"""
      live AS (
        SELECT
          CAST(date AS STRING) AS date, campaign,
          SUM(cost_gbp)                AS spend,
          SUM({live_conv})             AS conversions,
          SUM({live_rev})              AS revenue,
          SUM(all_conversions)         AS all_conversions,
          SUM({live_allv})             AS all_conv_value
        FROM `{PROJECT}.{DATASET}.ads_campaign_stats_live`
        GROUP BY date, campaign
      )""" if live else """
      live AS (SELECT CAST('1970-01-01' AS STRING) AS date, '' AS campaign,
        0.0 AS spend, 0.0 AS conversions, 0.0 AS revenue,
        0.0 AS all_conversions, 0.0 AS all_conv_value WHERE FALSE)"""

    recent_cte = f"""
      recent AS (
        SELECT
          CAST(date AS STRING) AS date, campaign,
          cost_gbp             AS spend,
          {conv_col}           AS conversions,
          {rev_col}            AS revenue,
          all_conversions,
          {allv_col}           AS all_conv_value
        FROM `{PROJECT}.{DATASET}.ads_campaign_stats`
        WHERE CAST(date AS STRING) NOT IN (SELECT DISTINCT date FROM live)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date, campaign ORDER BY pulled_at DESC) = 1
      )"""

    if history:
        hist_cte = f"""
      history AS (
        SELECT
          CAST(date AS STRING) AS date, campaign,
          SUM(cost_gbp)                AS spend,
          SUM(conversions)             AS conversions,
          SUM(revenue)                 AS revenue,
          SUM(all_conversions)         AS all_conversions,
          SUM(all_conversions_value)   AS all_conv_value
        FROM `{PROJECT}.{DATASET}.ads_campaign_stats_history`
        WHERE CAST(date AS STRING) NOT IN (
          SELECT DISTINCT date FROM recent
          UNION DISTINCT
          SELECT DISTINCT date FROM live
        )
        GROUP BY date, campaign
      )"""
        union_part = "SELECT * FROM live UNION ALL SELECT * FROM recent UNION ALL SELECT * FROM history"
        print("  Using ads_campaign_stats_history (backfill) + recent + live")
    else:
        hist_cte   = None
        union_part = "SELECT * FROM live UNION ALL SELECT * FROM recent"
        print("  No history table yet — using recent + live only")

    ctes = f"WITH {live_cte}, {recent_cte}"
    if hist_cte:
        ctes += f", {hist_cte}"

    sql = f"""
    {ctes}
    SELECT date, campaign, spend, conversions, revenue, all_conversions, all_conv_value
    FROM ({union_part})
    ORDER BY date DESC, spend DESC
    """

    rows = q(sql)
    for r in rows:
        for k in ["spend","conversions","revenue","all_conversions","all_conv_value"]:
            r[k] = f2(r.get(k))

    save("campaign_stats.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 2. ASSISTED CONVERSIONS
#    new table: ads_assisted_conv   (spend, last_click_conv, assists, lc_roas, mta_roas)
#    old table: ads_assisted_conversions (last_click_conversions, click_view_assists)
# ─────────────────────────────────────────────────────────────────────────────
def gen_assisted_conv():
    # Prefer new table if it exists
    new_cols = cols("ads_assisted_conv")
    if new_cols:
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.ads_assisted_conv`
        WHERE pulled_at = (SELECT MAX(pulled_at) FROM `{PROJECT}.{DATASET}.ads_assisted_conv`)
        ORDER BY last_click_conv DESC
        """
        rows = q(sql)
        for r in rows:
            for k in ["spend","last_click_conv","last_click_value","all_conversions",
                      "assists","assist_ratio","lc_roas","mta_roas"]:
                r[k] = f2(r.get(k))
    else:
        old_cols = cols("ads_assisted_conversions")
        if not old_cols:
            print("  no assisted conv table - skipping")
            return
        # Old schema: last_click_conversions, click_view_assists, assist_value, assist_ratio
        sql = f"""
        SELECT
            campaign,
            last_click_conversions   AS last_click_conv,
            last_click_value,
            click_view_assists       AS assists,
            assist_value,
            assist_ratio,
            campaign_role,
            date_range
        FROM `{PROJECT}.{DATASET}.ads_assisted_conversions`
        ORDER BY last_click_conversions DESC
        """
        rows = q(sql)
        for r in rows:
            for k in ["last_click_conv","last_click_value","assists","assist_value","assist_ratio"]:
                r[k] = f2(r.get(k))

    save("assisted_conv.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 3. TIME LAG
#    new table: ads_time_lag_raw   (days_to_conversion INTEGER)
#    old table: ads_time_lag       (days_bucket STRING, sort_order)
# ─────────────────────────────────────────────────────────────────────────────
def gen_time_lag():
    if cols("ads_time_lag_raw"):
        sql = f"""
        SELECT
            days_to_conversion                  AS bucket,
            CAST(days_to_conversion AS STRING)  AS label,
            SUM(conversions)                    AS conversions,
            SUM(conversion_value)               AS conversion_value
        FROM `{PROJECT}.{DATASET}.ads_time_lag_raw`
        WHERE DATE(pulled_at) = (
            SELECT DATE(MAX(pulled_at)) FROM `{PROJECT}.{DATASET}.ads_time_lag_raw`
        )
        GROUP BY days_to_conversion
        ORDER BY days_to_conversion
        """
    elif cols("ads_time_lag"):
        sql = f"""
        SELECT
            sort_order    AS bucket,
            days_bucket   AS label,
            conversions,
            conversion_value
        FROM `{PROJECT}.{DATASET}.ads_time_lag`
        ORDER BY sort_order
        """
    else:
        print("  no time_lag table - skipping"); return

    rows = q(sql)
    for r in rows:
        r["conversions"]      = f2(r.get("conversions"))
        r["conversion_value"] = f2(r.get("conversion_value"))
        r["bucket"]           = int(r["bucket"] or 0)

    save("time_lag.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 4. PATH LENGTH
#    new table: ads_path_length_raw   (interactions INTEGER)
#    old table: ads_path_length       (interactions_bucket STRING, sort_order)
# ─────────────────────────────────────────────────────────────────────────────
def gen_path_length():
    if cols("ads_path_length_raw"):
        sql = f"""
        SELECT
            interactions                       AS bucket,
            CAST(interactions AS STRING)       AS label,
            SUM(conversions)                   AS conversions,
            SUM(conversion_value)              AS conversion_value
        FROM `{PROJECT}.{DATASET}.ads_path_length_raw`
        WHERE DATE(pulled_at) = (
            SELECT DATE(MAX(pulled_at)) FROM `{PROJECT}.{DATASET}.ads_path_length_raw`
        )
        GROUP BY interactions
        ORDER BY interactions
        """
    elif cols("ads_path_length"):
        sql = f"""
        SELECT
            sort_order              AS bucket,
            interactions_bucket     AS label,
            conversions,
            conversion_value
        FROM `{PROJECT}.{DATASET}.ads_path_length`
        ORDER BY sort_order
        """
    else:
        print("  no path_length table - skipping"); return

    rows = q(sql)
    for r in rows:
        r["conversions"]      = f2(r.get("conversions"))
        r["conversion_value"] = f2(r.get("conversion_value"))
        r["bucket"]           = int(r["bucket"] or 0)

    save("path_length.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 5. CONVERSION PATHS — from GA4 BigQuery export (events_* tables)
#    Reconstructs actual campaign touch sequences per converting user.
#    Falls back to ads_conv_paths (Google Ads Scripts) if GA4 not available.
# ─────────────────────────────────────────────────────────────────────────────
def gen_conv_paths():
    from datetime import date, timedelta

    # Auto-discover the GA4 analytics dataset (analytics_XXXXXXXXX)
    ga4_ds = None
    try:
        for ds in client.list_datasets(project=PROJECT):
            if ds.dataset_id.startswith("analytics_"):
                ga4_ds = ds.dataset_id
                break
    except Exception as e:
        print(f"  GA4 dataset discovery failed: {e}")

    if ga4_ds:
        print(f"  Using GA4 BigQuery export: {ga4_ds}")
        end_dt   = date.today() - timedelta(days=1)
        start_dt = end_dt - timedelta(days=89)
        start_s  = start_dt.strftime("%Y%m%d")
        end_s    = end_dt.strftime("%Y%m%d")

        sql = f"""
        WITH sessions AS (
          SELECT
            user_pseudo_id,
            TIMESTAMP_MICROS(event_timestamp) AS ts,
            COALESCE(
              IF(NULLIF(collected_traffic_source.manual_campaign_name, '') IS NOT NULL
                 AND collected_traffic_source.manual_campaign_name NOT IN ('(not set)', '(direct)'),
                 collected_traffic_source.manual_campaign_name, NULL),
              IF(collected_traffic_source.gclid IS NOT NULL, 'Google Ads', NULL),
              IF(NULLIF(traffic_source.name, '') IS NOT NULL
                 AND traffic_source.name NOT IN ('(not set)', '(direct)'),
                 traffic_source.name, NULL)
            ) AS campaign
          FROM `{PROJECT}.{ga4_ds}.events_*`
          WHERE event_name = 'session_start'
            AND _TABLE_SUFFIX BETWEEN '{start_s}' AND '{end_s}'
        ),
        purchases AS (
          SELECT
            user_pseudo_id,
            TIMESTAMP_MICROS(event_timestamp) AS ts,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS txn_id,
            COALESCE(
              (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value'),
              (SELECT SAFE_CAST(value.string_value AS FLOAT64) FROM UNNEST(event_params) WHERE key = 'value'),
              0.0
            ) AS revenue
          FROM `{PROJECT}.{ga4_ds}.events_*`
          WHERE event_name = 'purchase'
            AND _TABLE_SUFFIX BETWEEN '{start_s}' AND '{end_s}'
        ),
        path_build AS (
          SELECT
            p.txn_id,
            p.revenue,
            ARRAY_TO_STRING(
              ARRAY_AGG(s.campaign IGNORE NULLS ORDER BY s.ts),
              ' > '
            ) AS path
          FROM purchases p
          JOIN sessions s ON s.user_pseudo_id = p.user_pseudo_id
            AND s.ts <= p.ts
            AND s.ts >= TIMESTAMP_SUB(p.ts, INTERVAL 90 DAY)
          GROUP BY p.txn_id, p.revenue
        )
        SELECT
          path,
          COUNT(*)             AS conversions,
          ROUND(SUM(revenue),2) AS conversion_value
        FROM path_build
        WHERE path IS NOT NULL AND path != ''
        GROUP BY path
        ORDER BY conversions DESC, conversion_value DESC
        LIMIT 50
        """
        try:
            rows = q(sql)
            for r in rows:
                r["conversions"]      = f2(r.get("conversions"))
                r["conversion_value"] = f2(r.get("conversion_value"))
                r["source"] = "ga4"
            print(f"  GA4 conv paths: {len(rows)} unique paths")
            save("conv_paths.json", {"rows": rows, "source": "ga4",
                                     "date_range": f"{start_dt} to {end_dt}"})
            return
        except Exception as e:
            print(f"  GA4 path query failed: {e}")

    # Fallback: Google Ads Scripts table
    if cols("ads_conv_paths"):
        sql = f"""
        SELECT path, conversion_action,
            SUM(conversions) AS conversions, SUM(conversion_value) AS conversion_value
        FROM `{PROJECT}.{DATASET}.ads_conv_paths`
        WHERE DATE(pulled_at) = (SELECT DATE(MAX(pulled_at)) FROM `{PROJECT}.{DATASET}.ads_conv_paths`)
        GROUP BY path, conversion_action
        ORDER BY conversions DESC LIMIT 30
        """
        rows = q(sql)
        for r in rows:
            r["conversions"]      = f2(r.get("conversions"))
            r["conversion_value"] = f2(r.get("conversion_value"))
            r["source"] = "ads_scripts"
        save("conv_paths.json", {"rows": rows, "source": "ads_scripts"})
    else:
        print("  no conv_paths data available - skipping")


# ─────────────────────────────────────────────────────────────────────────────
# 6. CONVERSION BREAKDOWN
# ─────────────────────────────────────────────────────────────────────────────
def gen_conv_breakdown():
    # Try new table first, then old
    table = "ads_conv_breakdown" if cols("ads_conv_breakdown") else \
            "ads_conversion_breakdown" if cols("ads_conversion_breakdown") else None
    if not table:
        print("  no conv_breakdown table - skipping"); return

    c = cols(table)
    conv_col = "conversions" if "conversions" in c else "conversions_lastclick"
    val_col  = "conversion_value" if "conversion_value" in c else "revenue_lastclick"

    has_pt  = "pulled_at" in c
    has_cat = "conversion_category" in c
    cat_sel = "conversion_category," if has_cat else "'' AS conversion_category,"
    cat_gb  = ", conversion_category" if has_cat else ""

    if has_pt:
        sql = f"""
    WITH deduped AS (
      SELECT CAST(date AS STRING) AS date, campaign, conversion_action,
        {cat_sel} {conv_col} AS conversions, {val_col} AS conversion_value
      FROM `{PROJECT}.{DATASET}.{table}`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY date, campaign, conversion_action{cat_gb} ORDER BY pulled_at DESC
      ) = 1
    )
    SELECT date, campaign, conversion_action, conversion_category,
      conversions, conversion_value
    FROM deduped
    ORDER BY date DESC, conversion_value DESC, conversions DESC
    """
    else:
        sql = f"""
    SELECT CAST(date AS STRING) AS date, campaign, conversion_action,
      {cat_sel} SUM({conv_col}) AS conversions, SUM({val_col}) AS conversion_value
    FROM `{PROJECT}.{DATASET}.{table}`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY date, campaign, conversion_action{cat_gb}
    ORDER BY date DESC, conversion_value DESC, conversions DESC
    """
    rows = q(sql)
    for r in rows:
        r["conversions"]      = f2(r.get("conversions"))
        r["conversion_value"] = f2(r.get("conversion_value"))

    save("conv_breakdown.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 7. META
# ─────────────────────────────────────────────────────────────────────────────
def gen_meta():
    # Include history table in date range if it exists
    history = cols("ads_campaign_stats_history")
    if history:
        sql = f"""
        SELECT
            CAST(MIN(date) AS STRING) AS data_from,
            CAST(MAX(date) AS STRING) AS data_to,
            COUNT(DISTINCT date)      AS days_available
        FROM (
          SELECT date FROM `{PROJECT}.{DATASET}.ads_campaign_stats`
          UNION DISTINCT
          SELECT date FROM `{PROJECT}.{DATASET}.ads_campaign_stats_history`
        )
        """
    else:
        sql = f"""
        SELECT
            CAST(MIN(date) AS STRING) AS data_from,
            CAST(MAX(date) AS STRING) AS data_to,
            COUNT(DISTINCT date)      AS days_available
        FROM `{PROJECT}.{DATASET}.ads_campaign_stats`
        """
    rows = q(sql)
    meta = rows[0] if rows else {"data_from": None, "data_to": None, "days_available": 0}
    meta["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    save("meta.json", meta)


# ─────────────────────────────────────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating dashboard data...")
    fns = [
        ("campaign_stats",  gen_campaign_stats),
        ("assisted_conv",   gen_assisted_conv),
        ("time_lag",        gen_time_lag),
        ("path_length",     gen_path_length),
        ("conv_paths",      gen_conv_paths),
        ("conv_breakdown",  gen_conv_breakdown),
        ("meta",            gen_meta),
    ]
    for name, fn in fns:
        try:
            fn()
        except Exception as e:
            print(f"  ERROR {name}: {e}")
    print("Done.")
