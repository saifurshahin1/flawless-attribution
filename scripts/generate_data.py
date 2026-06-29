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

    live = cols("ads_campaign_stats_live")

    if live:
        # Live table exists (hourly script is running).
        # Use live data for the last 3 days; historical table for everything older.
        # This avoids double-counting: live dates replace the same dates in hist.
        live_conv = "conversions"           if "conversions"           in live else conv_col
        live_rev  = "revenue"               if "revenue"               in live else rev_col
        live_allv = "all_conversions_value" if "all_conversions_value" in live else allv_col

        sql = f"""
        WITH live AS (
          SELECT
            CAST(date AS STRING) AS date,
            campaign,
            SUM(cost_gbp)                AS spend,
            SUM({live_conv})             AS conversions,
            SUM({live_rev})              AS revenue,
            SUM(all_conversions)         AS all_conversions,
            SUM({live_allv})             AS all_conv_value
          FROM `{PROJECT}.{DATASET}.ads_campaign_stats_live`
          GROUP BY date, campaign
        ),
        hist AS (
          SELECT
            CAST(date AS STRING) AS date,
            campaign,
            SUM(cost_gbp)                AS spend,
            SUM({conv_col})              AS conversions,
            SUM({rev_col})               AS revenue,
            SUM(all_conversions)         AS all_conversions,
            SUM({allv_col})              AS all_conv_value
          FROM `{PROJECT}.{DATASET}.ads_campaign_stats`
          WHERE CAST(date AS STRING) NOT IN (SELECT DISTINCT date FROM live)
            AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
          GROUP BY date, campaign
        )
        SELECT date, campaign, spend, conversions, revenue, all_conversions, all_conv_value
        FROM (SELECT * FROM live UNION ALL SELECT * FROM hist)
        ORDER BY date DESC, spend DESC
        """
    else:
        sql = f"""
        SELECT
            CAST(date AS STRING)      AS date,
            campaign,
            SUM(cost_gbp)             AS spend,
            SUM({conv_col})           AS conversions,
            SUM({rev_col})            AS revenue,
            SUM(all_conversions)      AS all_conversions,
            SUM({allv_col})           AS all_conv_value
        FROM `{PROJECT}.{DATASET}.ads_campaign_stats`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
        GROUP BY date, campaign
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
# 5. CONVERSION PATHS
#    new table: ads_conv_paths       (path, conversion_action, attribution_type)
#    old table: ads_conversion_paths (path_label)
# ─────────────────────────────────────────────────────────────────────────────
def gen_conv_paths():
    if cols("ads_conv_paths"):
        sql = f"""
        SELECT
            path, conversion_action,
            SUM(conversions)      AS conversions,
            SUM(conversion_value) AS conversion_value
        FROM `{PROJECT}.{DATASET}.ads_conv_paths`
        WHERE DATE(pulled_at) = (
            SELECT DATE(MAX(pulled_at)) FROM `{PROJECT}.{DATASET}.ads_conv_paths`
        )
        GROUP BY path, conversion_action
        ORDER BY conversions DESC
        LIMIT 20
        """
    elif cols("ads_conversion_paths"):
        sql = f"""
        SELECT
            path_label        AS path,
            '' AS conversion_action,
            conversions,
            conversion_value
        FROM `{PROJECT}.{DATASET}.ads_conversion_paths`
        WHERE is_other_paths = FALSE OR is_other_paths IS NULL
        ORDER BY conversions DESC
        LIMIT 20
        """
    else:
        print("  no conv_paths table - skipping"); return

    rows = q(sql)
    for r in rows:
        r["conversions"]      = f2(r.get("conversions"))
        r["conversion_value"] = f2(r.get("conversion_value"))

    save("conv_paths.json", {"rows": rows})


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

    sql = f"""
    SELECT
        campaign,
        conversion_action,
        {'conversion_category,' if 'conversion_category' in c else "'' AS conversion_category,"}
        SUM({conv_col})  AS conversions,
        SUM({val_col})   AS conversion_value
    FROM `{PROJECT}.{DATASET}.{table}`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY campaign, conversion_action {',' if 'conversion_category' in c else ''}
             {'conversion_category' if 'conversion_category' in c else ''}
    ORDER BY conversion_value DESC, conversions DESC
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
