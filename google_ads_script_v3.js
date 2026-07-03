/**
 * Flawless Fine Jewelry — Daily Attribution Export v3
 *
 * Runs INSIDE Google Ads Scripts. Pulls all attribution data → BigQuery daily.
 * Schedule: Daily (set in Google Ads Scripts > Schedules, run at 02:00 UK time)
 *
 * Tables updated in flawlessfine-ga4.attribution_results:
 *   ads_campaign_stats   — daily spend / conv / revenue per campaign
 *   ads_conv_breakdown   — daily breakdown by conversion action type
 *   ads_assisted_conv    — period-total assist ratios & campaign roles
 *   ads_time_lag_raw     — days-to-conversion histogram (period total)
 *   ads_path_length_raw  — interactions-to-conversion histogram (period total)
 *   ads_conv_paths       — top multi-touch conversion paths (period total)
 */

var CONFIG = {
  BQ_PROJECT: 'flawlessfine-ga4',
  BQ_DATASET: 'attribution_results',
  DAYS_BACK:  90   // covers Google Ads' full attribution window
};

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
function main() {
  var dates = getDateRange();
  Logger.log('=== Flawless Attribution Export v3 ===');
  Logger.log('Period: ' + dates.start + ' → ' + dates.end);

  try { exportCampaignStats(dates);     } catch(e) { Logger.log('ERR campaignStats: '   + e); }
  try { exportConvBreakdown(dates);     } catch(e) { Logger.log('ERR convBreakdown: '   + e); }
  try { exportAssistedConv(dates);      } catch(e) { Logger.log('ERR assistedConv: '    + e); }
  try { exportTimeLag(dates);           } catch(e) { Logger.log('ERR timeLag: '         + e); }
  try { exportPathLength(dates);        } catch(e) { Logger.log('ERR pathLength: '      + e); }
  try { exportConvPaths(dates);         } catch(e) { Logger.log('ERR convPaths: '       + e); }

  Logger.log('=== Export complete ===');
}

// ─────────────────────────────────────────────────────────────────────────────
// DATE HELPERS
// ─────────────────────────────────────────────────────────────────────────────
function getDateRange() {
  var now   = new Date();
  var end   = new Date(now); end.setDate(end.getDate() - 1);
  var start = new Date(now); start.setDate(start.getDate() - CONFIG.DAYS_BACK);
  return {
    start: fmtDate(start),
    end:   fmtDate(end),
    label: fmtDate(start) + ' to ' + fmtDate(end)
  };
}

function fmtDate(d) {
  return d.getFullYear() + '-' +
    pad(d.getMonth() + 1) + '-' + pad(d.getDate());
}

function pad(n) { return n < 10 ? '0' + n : '' + n; }

// ─────────────────────────────────────────────────────────────────────────────
// 1. CAMPAIGN STATS — one row per date × campaign
// ─────────────────────────────────────────────────────────────────────────────
function exportCampaignStats(dates) {
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
      insertId: r.segments.date + '_' + slug(r.campaign.name),
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
  Logger.log('Campaign stats: ' + rows.length + ' rows');
  bqReplace('ads_campaign_stats', schemaCampaignStats(), rows);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. CONVERSION BREAKDOWN — one row per date × campaign × conversion action
// ─────────────────────────────────────────────────────────────────────────────
function exportConvBreakdown(dates) {
  var rows = [];
  var iter = AdsApp.search(
    'SELECT segments.date, campaign.name, ' +
    '  segments.conversion_action_name, segments.conversion_action_category, ' +
    '  metrics.all_conversions, metrics.all_conversions_value ' +
    'FROM campaign ' +
    'WHERE segments.date BETWEEN "' + dates.start + '" AND "' + dates.end + '" ' +
    '  AND metrics.all_conversions > 0'
  );
  while (iter.hasNext()) {
    var r  = iter.next();
    var ca = r.segments.conversionActionName || '';
    rows.push({
      insertId: r.segments.date + '_' + slug(r.campaign.name) + '_' + slug(ca),
      json: {
        date:                r.segments.date,
        campaign:            r.campaign.name,
        conversion_action:   ca,
        conversion_category: r.segments.conversionActionCategory || '',
        conversions:         r2(r.metrics.allConversions || 0),
        conversion_value:    r2(r.metrics.allConversionsValue || 0),
        pulled_at:           new Date().toISOString()
      }
    });
  }
  Logger.log('Conv breakdown: ' + rows.length + ' rows');
  bqReplace('ads_conv_breakdown', schemaConvBreakdown(), rows);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. ASSISTED CONVERSIONS — campaign role analysis for the full period
//
//    last_click_conv  = metrics.conversions        (primary attribution model)
//    all_conversions  = metrics.all_conversions    (all touches incl. assists)
//    assists          ≈ all_conversions - last_click_conv
//
//    NOTE: Google Ads' Attribution UI shows the exact same figures. The small
//    delta you may see is view-through conversions included in all_conversions.
//    To exclude view-throughs, the user can filter by conversion_category in
//    the next phase.
// ─────────────────────────────────────────────────────────────────────────────
function exportAssistedConv(dates) {
  var acc = {};

  var iter = AdsApp.search(
    'SELECT campaign.name, ' +
    '  metrics.cost_micros, metrics.conversions, metrics.conversions_value, ' +
    '  metrics.all_conversions, metrics.all_conversions_value ' +
    'FROM campaign ' +
    'WHERE segments.date BETWEEN "' + dates.start + '" AND "' + dates.end + '" ' +
    '  AND campaign.status != "REMOVED"'
  );
  while (iter.hasNext()) {
    var r = iter.next();
    var n = r.campaign.name;
    if (!acc[n]) acc[n] = { spend:0, lc:0, lv:0, ac:0, av:0 };
    acc[n].spend += r.metrics.costMicros / 1000000;
    acc[n].lc    += r.metrics.conversions || 0;
    acc[n].lv    += r.metrics.conversionsValue || 0;
    acc[n].ac    += r.metrics.allConversions || 0;
    acc[n].av    += r.metrics.allConversionsValue || 0;
  }

  var rows = [];
  for (var name in acc) {
    var d       = acc[name];
    var assists = r2(Math.max(0, d.ac - d.lc));
    var ratio   = d.lc > 0 ? r2(assists / d.lc)
                           : (d.ac > 0 ? 99.0 : 0.0);
    var role    = d.lc === 0 && assists > 0 ? 'Pure Introducer'
                : ratio >= 1.5              ? 'Introducer'
                : ratio >= 0.5              ? 'Assist'
                :                             'Closer';
    rows.push({
      insertId: slug(name) + '_' + dates.start,
      json: {
        campaign:         name,
        spend:            r2(d.spend),
        last_click_conv:  r2(d.lc),
        last_click_value: r2(d.lv),
        all_conversions:  r2(d.ac),
        assists:          assists,
        assist_ratio:     ratio,
        campaign_role:    role,
        lc_roas:          d.spend > 0 ? r2(d.lv / d.spend) : 0,
        mta_roas:         d.spend > 0 ? r2(d.av / d.spend) : 0,
        date_range:       dates.label,
        pulled_at:        new Date().toISOString()
      }
    });
  }
  Logger.log('Assisted conv: ' + rows.length + ' campaigns');
  bqReplace('ads_assisted_conv', schemaAssistedConv(), rows);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. TIME LAG — days-to-conversion histogram
// ─────────────────────────────────────────────────────────────────────────────
function exportTimeLag(dates) {
  var rows = [];
  var iter = AdsApp.search(
    'SELECT campaign.name, segments.days_to_conversion, ' +
    '  metrics.all_conversions, metrics.all_conversions_value ' +
    'FROM campaign ' +
    'WHERE segments.date BETWEEN "' + dates.start + '" AND "' + dates.end + '" ' +
    '  AND metrics.all_conversions > 0'
  );
  while (iter.hasNext()) {
    var r    = iter.next();
    var days = r.segments.daysToConversion;
    rows.push({
      insertId: slug(r.campaign.name) + '_' + days + '_' + dates.start,
      json: {
        days_to_conversion: days,
        campaign:           r.campaign.name,
        conversions:        r2(r.metrics.allConversions || 0),
        conversion_value:   r2(r.metrics.allConversionsValue || 0),
        date_range:         dates.label,
        pulled_at:          new Date().toISOString()
      }
    });
  }
  Logger.log('Time lag: ' + rows.length + ' rows');
  bqReplace('ads_time_lag_raw', schemaTimeLag(), rows);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. PATH LENGTH — interactions-to-conversion histogram
// ─────────────────────────────────────────────────────────────────────────────
function exportPathLength(dates) {
  var rows = [];
  var iter = AdsApp.search(
    'SELECT campaign.name, segments.interactions_to_conversion, ' +
    '  metrics.all_conversions, metrics.all_conversions_value ' +
    'FROM campaign ' +
    'WHERE segments.date BETWEEN "' + dates.start + '" AND "' + dates.end + '" ' +
    '  AND metrics.all_conversions > 0'
  );
  while (iter.hasNext()) {
    var r    = iter.next();
    var ints = r.segments.interactionsToConversion;
    rows.push({
      insertId: slug(r.campaign.name) + '_' + ints + '_' + dates.start,
      json: {
        interactions:     ints,
        campaign:         r.campaign.name,
        conversions:      r2(r.metrics.allConversions || 0),
        conversion_value: r2(r.metrics.allConversionsValue || 0),
        date_range:       dates.label,
        pulled_at:        new Date().toISOString()
      }
    });
  }
  Logger.log('Path length: ' + rows.length + ' rows');
  bqReplace('ads_path_length_raw', schemaPathLength(), rows);
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. CONVERSION PATHS — top multi-touch paths (may be limited by privacy)
// ─────────────────────────────────────────────────────────────────────────────
function exportConvPaths(dates) {
  // Aggregate by path+action because segments.date returns one row per day per path.
  // Without aggregation, duplicate insertIds cause BigQuery to keep only 1 row per path,
  // making totals ~90x too low.
  var pathMap = {};
  try {
    var iter = AdsApp.search(
      'SELECT segments.conversion_action_name, ' +
      '  metrics.conversions, metrics.conversions_value ' +
      'FROM top_combinations_view'
    );
    while (iter.hasNext()) {
      var r      = iter.next();
      var pArr   = [];
      try { pArr = r.topCombinationsView.path || []; } catch(_) {}
      var pathStr = pArr.map(function(p) {
        return p.campaignName || p.campaignGroup || p.channelType || '?';
      }).join(' > ');
      var action = r.segments.conversionActionName || '';
      var key    = pathStr + '||' + action;

      if (!pathMap[key]) {
        pathMap[key] = {
          path:              pathStr,
          conversion_action: action,
          attribution_type:  '',
          conversions:       0,
          conversion_value:  0
        };
      }
      pathMap[key].conversions      += r2(r.metrics.conversions || 0);
      pathMap[key].conversion_value += r2(r.metrics.conversionsValue || 0);
    }
  } catch(e) {
    Logger.log('Conv paths note (privacy threshold may apply): ' + e);
  }

  var rows = Object.keys(pathMap).map(function(key) {
    var item = pathMap[key];
    return {
      insertId: slug(item.path).slice(0, 60) + '_' + dates.start,
      json: {
        path:              item.path,
        conversion_action: item.conversion_action,
        attribution_type:  item.attribution_type,
        conversions:       r2(item.conversions),
        conversion_value:  r2(item.conversion_value),
        date_range:        dates.label,
        pulled_at:         new Date().toISOString()
      }
    };
  });

  Logger.log('Conv paths: ' + rows.length + ' unique paths');
  if (rows.length > 0) {
    bqReplace('ads_conv_paths', schemaConvPaths(), rows);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// BIGQUERY — drop table, recreate, stream insert
// ─────────────────────────────────────────────────────────────────────────────
function bqReplace(table, schema, rows) {
  if (!rows.length) { Logger.log(table + ': 0 rows — skipped'); return; }

  // Drop existing table
  try { BigQuery.Tables.remove(CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET, table); } catch(_) {}
  Utilities.sleep(1000);

  // Create table
  try {
    BigQuery.Tables.insert(
      { tableReference: { projectId: CONFIG.BQ_PROJECT, datasetId: CONFIG.BQ_DATASET, tableId: table },
        schema: { fields: schema } },
      CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET
    );
  } catch(e) { Logger.log(table + ' create err: ' + e); return; }

  // Poll until BigQuery confirms table is ready (max 30s)
  var ready = false;
  for (var w = 0; w < 15; w++) {
    Utilities.sleep(2000);
    try { BigQuery.Tables.get(CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET, table); ready = true; break; } catch(_) {}
  }
  if (!ready) { Logger.log(table + ': not ready after 30s, skipping insert'); return; }

  // Stream insert in batches of 500
  var n = 0;
  for (var i = 0; i < rows.length; i += 500) {
    var batch = rows.slice(i, i + 500);
    var resp  = BigQuery.Tabledata.insertAll(
      { rows: batch, skipInvalidRows: true, ignoreUnknownValues: true },
      CONFIG.BQ_PROJECT, CONFIG.BQ_DATASET, table
    );
    if (resp.insertErrors && resp.insertErrors.length) {
      Logger.log(table + ' row err: ' + JSON.stringify(resp.insertErrors[0]));
    }
    n += batch.length;
  }
  Logger.log(table + ': ' + n + ' rows ✓');
}

// ─────────────────────────────────────────────────────────────────────────────
// SCHEMAS
// ─────────────────────────────────────────────────────────────────────────────
function schemaCampaignStats() {
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

function schemaConvBreakdown() {
  return [
    { name: 'date',                type: 'DATE'      },
    { name: 'campaign',            type: 'STRING'    },
    { name: 'conversion_action',   type: 'STRING'    },
    { name: 'conversion_category', type: 'STRING'    },
    { name: 'conversions',         type: 'FLOAT'     },
    { name: 'conversion_value',    type: 'FLOAT'     },
    { name: 'pulled_at',           type: 'TIMESTAMP' }
  ];
}

function schemaAssistedConv() {
  return [
    { name: 'campaign',         type: 'STRING'    },
    { name: 'spend',            type: 'FLOAT'     },
    { name: 'last_click_conv',  type: 'FLOAT'     },
    { name: 'last_click_value', type: 'FLOAT'     },
    { name: 'all_conversions',  type: 'FLOAT'     },
    { name: 'assists',          type: 'FLOAT'     },
    { name: 'assist_ratio',     type: 'FLOAT'     },
    { name: 'campaign_role',    type: 'STRING'    },
    { name: 'lc_roas',          type: 'FLOAT'     },
    { name: 'mta_roas',         type: 'FLOAT'     },
    { name: 'date_range',       type: 'STRING'    },
    { name: 'pulled_at',        type: 'TIMESTAMP' }
  ];
}

function schemaTimeLag() {
  return [
    { name: 'days_to_conversion', type: 'INTEGER'   },
    { name: 'campaign',           type: 'STRING'    },
    { name: 'conversions',        type: 'FLOAT'     },
    { name: 'conversion_value',   type: 'FLOAT'     },
    { name: 'date_range',         type: 'STRING'    },
    { name: 'pulled_at',          type: 'TIMESTAMP' }
  ];
}

function schemaPathLength() {
  return [
    { name: 'interactions',     type: 'INTEGER'   },
    { name: 'campaign',         type: 'STRING'    },
    { name: 'conversions',      type: 'FLOAT'     },
    { name: 'conversion_value', type: 'FLOAT'     },
    { name: 'date_range',       type: 'STRING'    },
    { name: 'pulled_at',        type: 'TIMESTAMP' }
  ];
}

function schemaConvPaths() {
  return [
    { name: 'path',             type: 'STRING'    },
    { name: 'conversion_action',type: 'STRING'    },
    { name: 'attribution_type', type: 'STRING'    },
    { name: 'conversions',      type: 'FLOAT'     },
    { name: 'conversion_value', type: 'FLOAT'     },
    { name: 'date_range',       type: 'STRING'    },
    { name: 'pulled_at',        type: 'TIMESTAMP' }
  ];
}

// ─────────────────────────────────────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────────────────────────────────────
function slug(s) { return String(s || '').replace(/[^a-zA-Z0-9]/g, '_'); }
function r2(n)   { return Math.round((n || 0) * 100) / 100; }
