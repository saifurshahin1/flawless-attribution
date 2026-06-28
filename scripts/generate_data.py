"""
generate_data.py
Reads attribution tables from BigQuery → writes JSON files to data/
GitHub Actions runs this daily after the Google Ads Script has updated BQ.

Output files (read directly by index.html via fetch):
  data/meta.json           — last_updated timestamp + available date range
  data/campaign_stats.json — daily rows for all periods (dashboard filters client-side)
  data/assisted_conv.json  — campaign role / assist ratio (latest pull)
  data/time_lag.json       — days-to-conversion histogram (latest pull)
  data/path_length.json    — interactions-to-conversion histogram (latest pull)
  data/conv_paths.json     — top conversion paths (latest pull)
  data/conv_breakdown.json — breakdown by conversion action (latest pull)
"""

import json
import os
from datetime import datetime, timezone
from google.cloud import bigquery

BQ_PROJECT = "flawlessfine-ga4"
BQ_DATASET = "attribution_results"
OUT_DIR    = "data"

client = bigquery.Client(project=BQ_PROJECT)

os.makedirs(OUT_DIR, exist_ok=True)


def q(sql: str) -> list[dict]:
    """Run a BigQuery query and return list of dicts."""
    rows = client.query(sql).result()
    return [dict(row) for row in rows]


def save(filename: str, payload) -> None:
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w") as f:
        json.dump(payload, f, default=str)
    print(f"  wrote {path}  ({len(json.dumps(payload, default=str))} bytes)")


# ─────────────────────────────────────────────────────────────────────────────
# 1. CAMPAIGN STATS — all daily rows (dashboard filters by date client-side)
# ─────────────────────────────────────────────────────────────────────────────
def gen_campaign_stats():
    sql = f"""
    SELECT
        CAST(date AS STRING)   AS date,
        campaign,
        SUM(cost_gbp)          AS spend,
        SUM(conversions)       AS conversions,
        SUM(revenue)           AS revenue,
        SUM(all_conversions)   AS all_conversions,
        SUM(all_conversions_value) AS all_conv_value
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_campaign_stats`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    GROUP BY date, campaign
    ORDER BY date DESC, spend DESC
    """
    rows = q(sql)

    # Convert Decimal → float for JSON serialisation
    for r in rows:
        for k in ["spend", "conversions", "revenue", "all_conversions", "all_conv_value"]:
            r[k] = round(float(r.get(k) or 0), 2)

    save("campaign_stats.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 2. ASSISTED CONVERSIONS — latest pull (period summary)
# ─────────────────────────────────────────────────────────────────────────────
def gen_assisted_conv():
    sql = f"""
    SELECT *
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_assisted_conv`
    WHERE pulled_at = (SELECT MAX(pulled_at) FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_assisted_conv`)
    ORDER BY last_click_conv DESC
    """
    rows = q(sql)
    for r in rows:
        for k in ["spend","last_click_conv","last_click_value","all_conversions",
                  "assists","assist_ratio","lc_roas","mta_roas"]:
            r[k] = round(float(r.get(k) or 0), 2)

    save("assisted_conv.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 3. TIME LAG — aggregate per bucket, latest pull
# ─────────────────────────────────────────────────────────────────────────────
def gen_time_lag():
    sql = f"""
    SELECT
        days_to_conversion,
        SUM(conversions)      AS conversions,
        SUM(conversion_value) AS conversion_value
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_time_lag_raw`
    WHERE DATE(pulled_at) = (
        SELECT DATE(MAX(pulled_at)) FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_time_lag_raw`
    )
    GROUP BY days_to_conversion
    ORDER BY days_to_conversion
    """
    rows = q(sql)
    for r in rows:
        r["conversions"]      = round(float(r.get("conversions") or 0), 2)
        r["conversion_value"] = round(float(r.get("conversion_value") or 0), 2)

    save("time_lag.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 4. PATH LENGTH — aggregate per bucket, latest pull
# ─────────────────────────────────────────────────────────────────────────────
def gen_path_length():
    sql = f"""
    SELECT
        interactions,
        SUM(conversions)      AS conversions,
        SUM(conversion_value) AS conversion_value
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_path_length_raw`
    WHERE DATE(pulled_at) = (
        SELECT DATE(MAX(pulled_at)) FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_path_length_raw`
    )
    GROUP BY interactions
    ORDER BY interactions
    """
    rows = q(sql)
    for r in rows:
        r["conversions"]      = round(float(r.get("conversions") or 0), 2)
        r["conversion_value"] = round(float(r.get("conversion_value") or 0), 2)

    save("path_length.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 5. CONVERSION PATHS — top 20, latest pull
# ─────────────────────────────────────────────────────────────────────────────
def gen_conv_paths():
    sql = f"""
    SELECT path, conversion_action, SUM(conversions) AS conversions,
           SUM(conversion_value) AS conversion_value
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_conv_paths`
    WHERE DATE(pulled_at) = (
        SELECT DATE(MAX(pulled_at)) FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_conv_paths`
    )
    GROUP BY path, conversion_action
    ORDER BY conversions DESC
    LIMIT 20
    """
    rows = q(sql)
    for r in rows:
        r["conversions"]      = round(float(r.get("conversions") or 0), 2)
        r["conversion_value"] = round(float(r.get("conversion_value") or 0), 2)

    save("conv_paths.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 6. CONVERSION BREAKDOWN — aggregate by action, latest 90 days
# ─────────────────────────────────────────────────────────────────────────────
def gen_conv_breakdown():
    sql = f"""
    SELECT
        campaign,
        conversion_action,
        conversion_category,
        SUM(conversions)      AS conversions,
        SUM(conversion_value) AS conversion_value
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_conv_breakdown`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY campaign, conversion_action, conversion_category
    ORDER BY conversion_value DESC, conversions DESC
    """
    rows = q(sql)
    for r in rows:
        r["conversions"]      = round(float(r.get("conversions") or 0), 2)
        r["conversion_value"] = round(float(r.get("conversion_value") or 0), 2)

    save("conv_breakdown.json", {"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# 7. META — date range available + last updated
# ─────────────────────────────────────────────────────────────────────────────
def gen_meta():
    sql = f"""
    SELECT
        CAST(MIN(date) AS STRING) AS data_from,
        CAST(MAX(date) AS STRING) AS data_to
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_campaign_stats`
    """
    rows = q(sql)
    meta = rows[0] if rows else {"data_from": None, "data_to": None}
    meta["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    save("meta.json", meta)


# ─────────────────────────────────────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating dashboard data files...")
    try: gen_campaign_stats();  print("✓ campaign_stats")
    except Exception as e:      print(f"✗ campaign_stats: {e}")

    try: gen_assisted_conv();   print("✓ assisted_conv")
    except Exception as e:      print(f"✗ assisted_conv: {e}")

    try: gen_time_lag();        print("✓ time_lag")
    except Exception as e:      print(f"✗ time_lag: {e}")

    try: gen_path_length();     print("✓ path_length")
    except Exception as e:      print(f"✗ path_length: {e}")

    try: gen_conv_paths();      print("✓ conv_paths")
    except Exception as e:      print(f"✗ conv_paths: {e}")

    try: gen_conv_breakdown();  print("✓ conv_breakdown")
    except Exception as e:      print(f"✗ conv_breakdown: {e}")

    try: gen_meta();            print("✓ meta")
    except Exception as e:      print(f"✗ meta: {e}")

    print("Done.")
