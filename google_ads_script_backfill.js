/**
 * Flawless Fine Jewelry — Historical Data Backfill
 *
 * Runs INSIDE Google Ads Scripts (one-time or on-demand).
 * Exports campaign stats for a given date range to ads_campaign_stats_history.
 *
 * HOW TO USE:
 *   1. Paste this script into Google Ads Scripts
 *   2. Set DATE_FROM and DATE_TO below
 *   3. Click Preview first to see row count
 *   4. Then Run — it may take 10-20 minutes for 5 years of data
 *   5. Do NOT set a schedule — this is a one-time run
 *
 * SAFE TO RE-RUN: uses insertId deduplication per date+campaign.
 * If you split into multiple runs (e.g., year by year), use non-overlapping date ranges.
 *
 * After this runs, generate_data.py (GitHub Actions) will automatically
 * include historical data in the next refresh.
 */

var CONFIG = {
  BQ_PROJECT: 'flawlessfine-ga4',
  BQ_DATASET: 'attribution_results',
  TABLE:      'ads_campaign_stats_history',

  // ── Set the range you want to backfill ───────────────────────────────────
  DATE_FROM:  '2021-02-17',   // earliest date in your Google Ads account
  DATE_TO:    '2026-03-30',   // day before current BigQuery data starts (2026-03-31)
  // ─────────────────────────────────────────────────────────────────────────

  BATCH_SIZE: 500             // BigQuery streaming insert batch size
};

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
function main() {
  Logger.log('=== Flawless Historical Backfill ===');
  Logger.log('Range: ' + CONFIG.DATE_FROM + ' → ' + CONFIG.DATE_TO);
  Logger.log('Target table: ' + CONFIG.TABLE);

  var rows = fetchCampaignStats();
  Logger.log('Rows fetched: ' + rows.length);

  if (rows.length === 0) {
    Logger.log('No data found — check your date range and campaign filters.');
    return;
  }

  ensureTable();
  insertRows(rows);

  Logger.log('=== Done — ' + rows.length + ' rows written to ' + CONFIG.TABLE + ' ===');
}

// ─────────────────────────────────────────────────────────────────────────────
// FETCH
// ─────────────────────────────────────────────────────────────────────────────
function fetchCampaignStats() {
  var rows = [];
  var query =
    'SELECT segments.date, campaign.name, campaign.status, ' +
    '  metrics.impressions, metrics.clicks, metrics.cost_micros, ' +
    '  metrics.conversions, metrics.conversions_value, ' +
    '  metrics.all_conversions, metrics.all_conversions_value ' +
    'FROM campaign ' +
    'WHERE segments.date BETWEEN "' + CONFIG.DATE_FROM + '" AND "' + CONFIG.DATE_TO + '" ' +
    '  AND campaign.status != "REMOVED" ' +
    'ORDER BY segments.date ASC, metrics.cost_micros DESC';

  Logger.log('Running GAQL query...');
  var iter = AdsApp.search(query);
  var count = 0;

  while (iter.hasNext()) {
    var r = iter.next();
    var costMicros = r.metrics.costMicros || 0;
    // Skip rows with zero spend AND zero conversions (keeps data clean)
    if (costMicros === 0 && (r.metrics.conversions || 0) === 0 &&
        (r.metrics.allConversions || 0) === 0) {
      continue;
    }

    rows.push({
      // insertId ensures BigQuery deduplicates if script is re-run for same range
      insertId: r.segments.date + '_' + slug(r.campaign.name),
      json: {
        date:                  r.segments.date,
        campaign:              r.campaign.name,
        status:                r.campaign.status,
        impressions:           parseInt(r.metrics.impressions || 0),
        clicks:                parseInt(r.metrics.clicks || 0),
        cost_gbp:              r2(costMicros / 1000000),
        conversions:           r2(r.metrics.conversions || 0),
        revenue:               r2(r.metrics.conversionsValue || 0),
        all_conversions:       r2(r.metrics.allConversions || 0),
        all_conversions_value: r2(r.metrics.allConversionsValue || 0),
        pulled_at:             new Date().toISOString()
      }
    });

    count++;
    if (count % 1000 === 0) Logger.log('  Fetched ' + count + ' rows so far...');
  }

  return rows;
}

// ─────────────────────────────────────────────────────────────────────────────
// BIGQUERY — Create table if not exists (APPEND mode, not replace)
// ─────────────────────────────────────────────────────────────────────────────
function ensureTable() {
  try {
    BigQuery.Tables.get(CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET, CONFIG.TABLE);
    Logger.log('Table ' + CONFIG.TABLE + ' already exists — will append.');
  } catch(e) {
    Logger.log('Creating table ' + CONFIG.TABLE + '...');
    BigQuery.Tables.insert(
      {
        tableReference: {
          projectId: CONFIG.BQ_PROJECT,
          datasetId: CONFIG.BQ_DATASET,
          tableId:   CONFIG.TABLE
        },
        schema: { fields: schema() }
      },
      CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET
    );
    Utilities.sleep(1000);
    Logger.log('Table created.');
  }
}

function insertRows(rows) {
  var total = rows.length;
  var errors = 0;

  for (var i = 0; i < total; i += CONFIG.BATCH_SIZE) {
    var batch = rows.slice(i, i + CONFIG.BATCH_SIZE);
    var resp  = BigQuery.Tabledata.insertAll(
      { rows: batch, skipInvalidRows: true, ignoreUnknownValues: true },
      CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET, CONFIG.TABLE
    );
    if (resp.insertErrors && resp.insertErrors.length) {
      errors += resp.insertErrors.length;
      Logger.log('Insert errors in batch ' + (i/CONFIG.BATCH_SIZE+1) + ': ' +
                 JSON.stringify(resp.insertErrors[0]));
    }
    if ((i + CONFIG.BATCH_SIZE) % 5000 === 0) {
      Logger.log('  Inserted ' + Math.min(i + CONFIG.BATCH_SIZE, total) + '/' + total + ' rows...');
    }
  }

  Logger.log('Insert complete. Errors: ' + errors);
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────
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

function slug(s)  { return String(s || '').replace(/[^a-zA-Z0-9]/g, '_').slice(0, 60); }
function r2(n)    { return Math.round((n || 0) * 100) / 100; }
