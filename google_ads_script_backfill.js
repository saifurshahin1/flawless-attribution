/**
 * Flawless Fine Jewelry — Historical Data Backfill (v2)
 *
 * Runs INSIDE Google Ads Scripts (one-time or on-demand).
 * Exports ALL campaign stats (including removed campaigns) for a given date range.
 *
 * FIXES vs v1:
 *   - Includes REMOVED campaigns (they had real spend/conversions historically)
 *   - Includes zero-spend rows (captures view-through conversions)
 *   - REPLACES table (drop + recreate) to avoid duplicates on re-run
 *
 * HOW TO USE:
 *   1. Paste this script into Google Ads Scripts (replace old backfill script)
 *   2. Click Preview first to confirm row count in logs
 *   3. Then Run — takes 10-20 minutes for 5 years of data
 *   4. Do NOT set a schedule — one-time only
 */

var CONFIG = {
  BQ_PROJECT: 'flawlessfine-ga4',
  BQ_DATASET: 'attribution_results',
  TABLE:      'ads_campaign_stats_history',

  DATE_FROM:  '2021-02-17',   // earliest date in your Google Ads account
  DATE_TO:    '2026-03-30',   // day before current BigQuery data starts

  BATCH_SIZE: 500
};

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
function main() {
  Logger.log('=== Flawless Historical Backfill v2 ===');
  Logger.log('Range: ' + CONFIG.DATE_FROM + ' -> ' + CONFIG.DATE_TO);
  Logger.log('Target table: ' + CONFIG.TABLE);
  Logger.log('Note: includes ALL campaigns (enabled, paused, removed)');

  var rows = fetchCampaignStats();
  Logger.log('Rows fetched: ' + rows.length);

  if (rows.length === 0) {
    Logger.log('No data found — check your date range.');
    return;
  }

  replaceTable(rows);

  Logger.log('=== Done — ' + rows.length + ' rows written to ' + CONFIG.TABLE + ' ===');
}

// ─────────────────────────────────────────────────────────────────────────────
// FETCH — NO status filter, NO zero-row filter
// ─────────────────────────────────────────────────────────────────────────────
function fetchCampaignStats() {
  var rows = [];

  // No campaign.status filter — includes REMOVED campaigns with historical data
  var query =
    'SELECT segments.date, campaign.name, campaign.status, ' +
    '  metrics.impressions, metrics.clicks, metrics.cost_micros, ' +
    '  metrics.conversions, metrics.conversions_value, ' +
    '  metrics.all_conversions, metrics.all_conversions_value ' +
    'FROM campaign ' +
    'WHERE segments.date BETWEEN "' + CONFIG.DATE_FROM + '" AND "' + CONFIG.DATE_TO + '" ' +
    'ORDER BY segments.date ASC, metrics.cost_micros DESC';

  Logger.log('Running GAQL query (all campaigns including removed)...');
  var iter = AdsApp.search(query);
  var count = 0;

  while (iter.hasNext()) {
    var r = iter.next();
    var costMicros = r.metrics.costMicros || 0;
    var conv       = r.metrics.conversions || 0;
    var allConv    = r.metrics.allConversions || 0;
    var convVal    = r.metrics.conversionsValue || 0;

    // Only skip truly empty rows (no spend AND no conversions AND no impressions)
    if (costMicros === 0 && conv === 0 && allConv === 0 &&
        (r.metrics.impressions || 0) === 0) {
      continue;
    }

    rows.push({
      insertId: r.segments.date + '_' + slug(r.campaign.name),
      json: {
        date:                  r.segments.date,
        campaign:              r.campaign.name,
        status:                r.campaign.status,
        impressions:           parseInt(r.metrics.impressions || 0),
        clicks:                parseInt(r.metrics.clicks || 0),
        cost_gbp:              r2(costMicros / 1000000),
        conversions:           r2(conv),
        revenue:               r2(convVal),
        all_conversions:       r2(allConv),
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
// BIGQUERY — TRUNCATE existing table (or create if new), then insert all rows
// Using TRUNCATE instead of drop+recreate avoids the "table not found" race
// ─────────────────────────────────────────────────────────────────────────────
function replaceTable(rows) {
  // Check if table exists
  var tableExists = false;
  try {
    BigQuery.Tables.get(CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET, CONFIG.TABLE);
    tableExists = true;
  } catch(e) {
    tableExists = false;
  }

  if (tableExists) {
    // TRUNCATE: wipes data but keeps table structure — no timing issues
    Logger.log('Truncating existing ' + CONFIG.TABLE + ' table...');
    try {
      BigQuery.Jobs.query(
        {
          query: 'TRUNCATE TABLE `' + CONFIG.BQ_PROJECT + '.' + CONFIG.BQ_DATASET + '.' + CONFIG.TABLE + '`',
          useLegacySql: false,
          timeoutMs: 60000
        },
        CONFIG.BQ_PROJECT
      );
      Logger.log('Table truncated — ready for fresh insert.');
    } catch(e) {
      Logger.log('TRUNCATE failed: ' + e + ' — will try to proceed anyway.');
    }
    Utilities.sleep(3000);
  } else {
    // First run — create the table
    Logger.log('Creating ' + CONFIG.TABLE + ' table for first time...');
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
    Logger.log('Table created. Waiting 25s for BigQuery to be ready...');
    Utilities.sleep(25000);
  }

  // Insert in batches
  var total  = rows.length;
  var errors = 0;

  for (var i = 0; i < total; i += CONFIG.BATCH_SIZE) {
    var batch = rows.slice(i, i + CONFIG.BATCH_SIZE);
    var resp  = BigQuery.Tabledata.insertAll(
      { rows: batch, skipInvalidRows: true, ignoreUnknownValues: true },
      CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET, CONFIG.TABLE
    );
    if (resp.insertErrors && resp.insertErrors.length) {
      errors += resp.insertErrors.length;
      Logger.log('Insert error batch ' + Math.ceil(i/CONFIG.BATCH_SIZE+1) + ': ' +
                 JSON.stringify(resp.insertErrors[0]));
    }
    if ((i + CONFIG.BATCH_SIZE) % 5000 === 0) {
      Logger.log('  Inserted ' + Math.min(i + CONFIG.BATCH_SIZE, total) + '/' + total + '...');
    }
  }

  Logger.log('All rows inserted. Errors: ' + errors);
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
