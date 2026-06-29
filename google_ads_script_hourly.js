/**
 * Flawless Fine Jewelry — Hourly Quick Export
 *
 * Runs INSIDE Google Ads Scripts. Pulls only the last 3 days of campaign stats
 * so today's spend / conversions are always fresh in the dashboard.
 *
 * Schedule: Every hour (set in Google Ads Scripts > Schedules)
 * BigQuery table updated: ads_campaign_stats_live
 *   (generate_data.py merges this with the full 90-day table automatically)
 *
 * The full v3 script (google_ads_script_v3.js) still runs DAILY at 02:00 UK
 * time to update attribution analysis, time lag, conversion paths, etc.
 */

var CONFIG = {
  BQ_PROJECT: 'flawlessfine-ga4',
  BQ_DATASET: 'attribution_results',
  DAYS_BACK:  3   // today + yesterday + day before — where live data matters
};

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
function main() {
  var dates = getDateRange();
  Logger.log('=== Flawless Quick Export (Hourly) ===');
  Logger.log('Period: ' + dates.start + ' to ' + dates.end);
  Logger.log('Run time: ' + new Date().toISOString());

  try {
    exportCampaignStatsLive(dates);
  } catch(e) {
    Logger.log('ERR campaignStats: ' + e);
  }

  Logger.log('=== Done ===');
}

// ─────────────────────────────────────────────────────────────────────────────
// CAMPAIGN STATS (last 3 days) → ads_campaign_stats_live
// ─────────────────────────────────────────────────────────────────────────────
function exportCampaignStatsLive(dates) {
  var rows = [];
  var iter = AdsApp.search(
    'SELECT segments.date, campaign.name, campaign.status, ' +
    '  metrics.impressions, metrics.clicks, metrics.cost_micros, ' +
    '  metrics.conversions, metrics.conversions_value, ' +
    '  metrics.all_conversions, metrics.all_conversions_value ' +
    'FROM campaign ' +
    'WHERE segments.date BETWEEN "' + dates.start + '" AND "' + dates.end + '" ' +
    '  AND campaign.status != "REMOVED" ' +
    'ORDER BY segments.date DESC, metrics.cost_micros DESC'
  );

  while (iter.hasNext()) {
    var r = iter.next();
    rows.push({
      insertId: r.segments.date + '_' + slug(r.campaign.name) + '_live',
      json: {
        date:                  r.segments.date,
        campaign:              r.campaign.name,
        status:                r.campaign.status,
        impressions:           r.metrics.impressions || 0,
        clicks:                r.metrics.clicks || 0,
        cost_gbp:              r2(r.metrics.costMicros / 1000000),
        conversions:           r2(r.metrics.conversions || 0),
        revenue:               r2(r.metrics.conversionsValue || 0),
        all_conversions:       r2(r.metrics.allConversions || 0),
        all_conversions_value: r2(r.metrics.allConversionsValue || 0),
        pulled_at:             new Date().toISOString()
      }
    });
  }

  Logger.log('Live campaign stats: ' + rows.length + ' rows');
  bqReplace('ads_campaign_stats_live', schema(), rows);
}

// ─────────────────────────────────────────────────────────────────────────────
// BIGQUERY — drop table, recreate, stream insert
// ─────────────────────────────────────────────────────────────────────────────
function bqReplace(table, schemaFields, rows) {
  if (!rows.length) { Logger.log(table + ': 0 rows — skipped'); return; }

  try { BigQuery.Tables.remove(CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET, table); } catch(_) {}
  Utilities.sleep(500);

  BigQuery.Tables.insert(
    {
      tableReference: { projectId: CONFIG.BQ_PROJECT, datasetId: CONFIG.BQ_DATASET, tableId: table },
      schema: { fields: schemaFields }
    },
    CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET
  );

  for (var i = 0; i < rows.length; i += 500) {
    var batch = rows.slice(i, i + 500);
    var resp  = BigQuery.Tabledata.insertAll(
      { rows: batch, skipInvalidRows: true, ignoreUnknownValues: true },
      CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET, table
    );
    if (resp.insertErrors && resp.insertErrors.length) {
      Logger.log(table + ' insert error: ' + JSON.stringify(resp.insertErrors[0]));
    }
  }
  Logger.log(table + ': ' + rows.length + ' rows written');
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────
function getDateRange() {
  var now   = new Date();
  var end   = new Date(now); end.setDate(end.getDate() - 1); // yesterday (Google Ads finalises yesterday)
  var start = new Date(now); start.setDate(start.getDate() - CONFIG.DAYS_BACK);
  return { start: fmtDate(start), end: fmtDate(end) };
}

function schema() {
  return [
    { name: 'date',                  type: 'DATE'      },
    { name: 'campaign',              type: 'STRING'    },
    { name: 'status',                type: 'STRING'    },
    { name: 'impressions',           type: 'INTEGER'   },
    { name: 'clicks',                type: 'INTEGER'   },
    { name: 'cost_gbp',              type: 'FLOAT'     },
    { name: 'conversions',           type: 'FLOAT'     },
    { name: 'revenue',               type: 'FLOAT'     },
    { name: 'all_conversions',       type: 'FLOAT'     },
    { name: 'all_conversions_value', type: 'FLOAT'     },
    { name: 'pulled_at',             type: 'TIMESTAMP' }
  ];
}

function fmtDate(d) {
  return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate());
}
function pad(n)   { return n < 10 ? '0' + n : '' + n; }
function slug(s)  { return String(s || '').replace(/[^a-zA-Z0-9]/g, '_'); }
function r2(n)    { return Math.round((n || 0) * 100) / 100; }
