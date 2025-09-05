/** ===== STOCKS (FBO+FBS) ===== **/

/**
 * Этот модуль собирает остатки по FBS-складам продавца.
 * Алгоритм:
 *  1) читаем список nmId (Map_NMID -> Ads_API как резерв);
 *  2) через Content API получаем штрих-коды (skus) для этих nmId;
 *  3) получаем список складов /api/v3/warehouses;
 *  4) для каждого склада отправляем POST /api/v3/stocks/{warehouseId} со списком skus;
 *  5) из ответа берём amount и маппим sku -> nmId; агрегируем по nmId+склад;
 *  6) пишем в лист Stocks_API: [nmId, Склад, Кол-во].
 *
 * ВАЖНО:
 *  - WB /api/v3/stocks/{warehouseId} ожидает ТОЛЬКО { "skus": [...] }.
 *  - В ответе остаток в поле "amount" (не quantity/qty/stock).
 */

const SHEET_STOCKS_API = 'Stocks_API';

// Обёртка под старое меню
function WB_collectStocksPlusFBS() { return WB_collectStocks(); }

/** Точка входа */
function WB_collectStocks() {
  const S = getSettings_();
  const auth = (S.stockToken && String(S.stockToken).trim()) || S.token;
  if (!auth) throw new Error('Нет токена Authorization (Settings → API токен).');

  const headersMP = { 'Authorization': auth, 'Content-Type': 'application/json', 'Accept':'application/json' };
  const headersCT = headersMP; // тот же токен для Content API

  log_('INFO','Stocks','start','Сбор остатков (FBO+FBS)', null);

  // 1) nmId
  const nmSet = _stocks_readNmIds_();
  if (nmSet.size === 0) {
    log_('WARN','Stocks','empty_nm','Нет nmId (проверь Map_NMID / Ads_API).', null);
    _stocks_writeTable_([]);
    log_('INFO','Stocks','done','Stocks_API: 0 строк', { rows: 0 });
    return;
  }

  // 2) индексы nm <-> skus через Content API
  const { mapNmToSkus, mapSkuToNm, nmWithSkuCount } = _buildSkuIndexFromContent_(headersCT, nmSet);
  log_('INFO','Stocks','index_stats', `nmTotal:${nmSet.size}, nmWithSku:${nmWithSkuCount}`, null);

  if (nmWithSkuCount === 0) {
    log_('WARN','Stocks','no_sku_for_nm','Не нашли ни одного SKU для переданных nmId', null);
    _stocks_writeTable_([]);
    log_('INFO','Stocks','done','Stocks_API: 0 строк', { rows: 0 });
    return;
  }

  // 3) склады
  const warehouses = _stocks_getWarehouses_(headersMP);
  if (!warehouses.length) {
    log_('WARN','Stocks','endpoint_fail','Склады не найдены (эндпоинт недоступен).', null);
    _stocks_writeTable_([]);
    log_('INFO','Stocks','done','Stocks_API: 0 строк', { rows: 0 });
    return;
  }

  // 4) отправляем SKU по складам, агрегируем nmId+склад
  const agg = new Map(); // key = nmId|whName -> sum(amount)
  const CH_SKU = 100;    // безопасная пачка

  for (const wh of warehouses) {
    const whId = Number(wh.id);
    const whName = String(wh.name || '');

    // Соберём единый массив всех SKU, которые есть у наших nmId
    const skuAll = [];
    mapNmToSkus.forEach(set => set.forEach(s => skuAll.push(String(s))));
    if (!skuAll.length) continue;

    for (let i = 0; i < skuAll.length; i += CH_SKU) {
      const part = skuAll.slice(i, i + CH_SKU);
      const rows = _stocks_postOneBatch_(headersMP, whId, whName, part, mapSkuToNm);
      if (rows && rows.length) {
        for (const [nm, whN, qty] of rows) {
          const key = `${nm}|${whN}`;
          const prev = agg.get(key) || 0;
          agg.set(key, prev + qty);
        }
      }
    }
  }

  // 5) в таблицу
  const outRows = [];
  agg.forEach((qty, key) => {
    const [nm, whName] = key.split('|');
    outRows.push([Number(nm), whName, qty]);
  });

  _stocks_writeTable_(outRows);
  log_('INFO','Stocks','done', `Stocks_API: ${outRows.length} строк`, { rows: outRows.length });
}

/* -------------------- helpers -------------------- */

/** Читает nmId: приоритет Map_NMID, резерв Ads_API */
function _stocks_readNmIds_() {
  const ss = SpreadsheetApp.getActive();
  const set = new Set();

  // Map_NMID: предполагаем, что в колонке B лежит nmId (как в твоём проекте)
  const shMap = ss.getSheetByName('Map_NMID');
  if (shMap && shMap.getLastRow() > 1) {
    const values = shMap.getRange(2, 2, shMap.getLastRow() - 1, 1).getValues();
    for (const [nm] of values) {
      const v = Number(nm);
      if (Number.isFinite(v) && v > 0) set.add(v);
    }
  }
  if (set.size) return set;

  // Резерв — Ads_API: ищем колонку с nm
  const shAds = ss.getSheetByName('Ads_API');
  if (shAds && shAds.getLastRow() > 1) {
    const H = shAds.getRange(1,1,1, shAds.getLastColumn()).getDisplayValues()[0].map(s => String(s).toLowerCase());
    const cNm = H.findIndex(h => h.includes('nm'));
    if (cNm >= 0) {
      const v = shAds.getRange(2,1, shAds.getLastRow()-1, shAds.getLastColumn()).getValues();
      v.forEach(r => { const nm = Number(r[cNm]); if (Number.isFinite(nm) && nm > 0) set.add(nm); });
    }
  }
  return set;
}

/** Получаем список FBS-складов продавца */
function _stocks_getWarehouses_(headers) {
  const url = 'https://marketplace-api.wildberries.ru/api/v3/warehouses';
  const t0 = Date.now();
  const res = UrlFetchApp.fetch(url, { method: 'get', headers, muteHttpExceptions: true });
  const code = res.getResponseCode(), body = res.getContentText() || '';
  log_('INFO','HTTP','response', `${code} ${url} (${Date.now()-t0} ms)`, { respBytes: body.length });

  if (code !== 200) return [];
  try {
    const j = JSON.parse(body || '[]');
    const arr = Array.isArray(j) ? j : (j?.warehouses || j?.data || []);
    return arr
      .map(x => ({
        id: Number(x?.id ?? x?.warehouseId ?? x?.warehouse_id),
        name: String(x?.name ?? x?.warehouseName ?? x?.office ?? '')
      }))
      .filter(x => x.id && x.name);
  } catch(e) {
    return [];
  }
}

/**
 * Строим индексы nmId <-> skus по Content API v2/get/cards/list
 * Возвращает:
 *  - mapNmToSkus: Map<nmId, Set<sku>>
 *  - mapSkuToNm : Map<sku, nmId>
 */
function _buildSkuIndexFromContent_(headersContent, nmSet) {
  const url = 'https://content-api.wildberries.ru/content/v2/get/cards/list';
  const nmAll = Array.from(nmSet);
  const CH = 100; // пачка nmID

  const mapNmToSkus = new Map();
  const mapSkuToNm  = new Map();
  let nmWithSku = 0;

  for (let i = 0; i < nmAll.length; i += CH) {
    const part = nmAll.slice(i, i + CH);
    // WB v2: filter.nmID + cursor.limit
    const payload = JSON.stringify({
      sort:   { cursor: { limit: 1000 } },
      filter: { nmID: part }
    });

    const t0 = Date.now();
    const res = UrlFetchApp.fetch(url, {
      method: 'post',
      headers: headersContent,
      payload,
      muteHttpExceptions: true
    });
    const code = res.getResponseCode();
    const text = res.getContentText() || '';

    log_('INFO','HTTP','response', `${code} ${url} (${Date.now()-t0} ms)`,
      { variant: 'v2.cursor.filter.nmID', respBytes: String(text).length });

    if (code !== 200) { Utilities.sleep(300); continue; }

    try {
      const j = JSON.parse(text || '{}');
      const cards = j?.data?.cards || j?.cards || [];
      for (const c of cards) {
        const nm = Number(c?.nmID ?? c?.nmId ?? c?.nmid ?? c?.nm);
        if (!nm) continue;

        const sizes = Array.isArray(c?.sizes) ? c.sizes : [];
        let hasSku = false;

        for (const s of sizes) {
          const skus = Array.isArray(s?.skus) ? s.skus : [];
          for (const skuRaw of skus) {
            const sku = String(typeof skuRaw === 'object' && skuRaw ? skuRaw?.sku ?? '' : skuRaw || '');
            if (!sku) continue;

            hasSku = true;
            if (!mapNmToSkus.has(nm)) mapNmToSkus.set(nm, new Set());
            mapNmToSkus.get(nm).add(sku);
            if (!mapSkuToNm.has(sku)) mapSkuToNm.set(sku, nm);
          }
        }
        if (hasSku) nmWithSku++;
      }
    } catch(_) { /* пропускаем неудачный чанок */ }

    Utilities.sleep(150);
  }

  // Логируем небольшую выборку отсутствующих SKU
  const missing = nmAll.filter(nm => !(mapNmToSkus.get(nm)?.size));
  if (missing.length) {
    log_('WARN','Stocks','no_sku_for_nm','Не нашли SKU для части nmId', { missing: missing.slice(0,50) });
  }

  return { mapNmToSkus, mapSkuToNm, nmWithSkuCount: nmWithSku };
}

/**
 * Отправляет на склад список SKU и возвращает строки [nmId, whName, qty].
 * ВАЖНО: qty берём из поля "amount".
 */
function _stocks_postOneBatch_(headers, whId, whName, skuBatch, mapSkuToNm) {
  const url = `https://marketplace-api.wildberries.ru/api/v3/stocks/${whId}`;
  const payload = JSON.stringify({ skus: skuBatch });

  let attempts = 0;
  while (attempts < 4) {
    attempts++;
    const t0 = Date.now();
    const res = UrlFetchApp.fetch(url, {
      method: 'post',
      headers,
      payload,
      muteHttpExceptions: true
    });
    const code = res.getResponseCode();
    const text = res.getContentText() || '';

    log_('INFO','HTTP','response', `${code} ${url} (${Date.now()-t0} ms)`,
      { whId, whName, batch: skuBatch.length, bodyKey: 'skus', respBytes: String(text).length });

    if (code === 200) {
      const out = [];
      try {
        const j   = JSON.parse(text || '{}');
        const arr = Array.isArray(j) ? j : (j.stocks || j.data?.stocks || []);
        for (const it of arr) {
          const sku = String(it?.sku || '');
          const nm  = mapSkuToNm.get(sku);
          const qty = Number(it?.amount ?? 0);   // ← КЛЮЧЕВОЕ: amount
          if (nm && Number.isFinite(qty)) out.push([nm, whName, qty]);
        }
      } catch(_) {}
      return out;
    }

    if (code === 429) { Utilities.sleep(1500); continue; } // лимитер
    if (code === 400) {                                    // тело не приняли — значит проблема со SKU
      log_('WARN','Stocks','400','WB отклонил тело (skus). Проверь skuBatch.', { sample: skuBatch.slice(0,10) });
      return [];
    }
    if (code === 401 || code === 403) {
      throw new Error('Stocks API: 401/403 — проверь токен и права «Маркетплейс/Поставки».');
    }

    Utilities.sleep(700);
  }
  return [];
}

/** Пишем сводную таблицу */
function _stocks_writeTable_(rows) {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(SHEET_STOCKS_API) || ss.insertSheet(SHEET_STOCKS_API);
  sh.clearContents();
  sh.getRange(1,1,1,3).setValues([['nmId','Склад','Кол-во']]);
  if (rows && rows.length) sh.getRange(2,1,rows.length,3).setValues(rows);
  sh.setFrozenRows(1);
}
