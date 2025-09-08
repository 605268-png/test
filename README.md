/** ===== DIAG: STOCKS (FBO+FBS) ===== **/

const SHEET_STOCKS_API = 'Stocks_API';
const SHEET_RAW_STOCKS = 'Raw_Stocks';

function WB_collectStocks(){
  const S = getSettings_();
  const auth = (S.stockToken && S.stockToken.trim()) || S.token;
  if (!auth) throw new Error('Нет токена Authorization (Settings → API токен).');

  const H_MP = { 'Authorization': auth, 'Content-Type':'application/json', 'Accept':'application/json' };
  const H_CT = H_MP;

  log_('INFO','Stocks','start','Сбор остатков (FBO+FBS)', null);

  // 1) nmID из Map_NMID + Ads_Nm_Cache + Ads_API (объединяем без дублей)
  const nmSet = _nm_collectAll_();
  const nmAll = Array.from(nmSet);
  log_('INFO','Stocks','nm_source', `Взято из Map_NMID/Ads: ${nmAll.length} nmId`, null);
  if (nmAll.length === 0){ _stocks_writeTable_([]); log_('INFO','Stocks','done','Stocks_API: 0 строк', {rows:0}); return; }

  // 2) склады продавца
  const warehouses = _stocks_getWarehouses_(H_MP);
  if (!warehouses.length){ log_('WARN','Stocks','wh_empty','Склады не найдены (GET /api/v3/warehouses)', null); _stocks_writeTable_([]); return; }
  log_('INFO','Stocks','wh_summary', `Найдено складов: ${warehouses.length}`, {wh: warehouses.map(x=>({id:x.id,name:x.name})).slice(0,20)});

  // 3) nm -> sku индексация
  const idx = _buildSkuIndex_(H_CT, nmAll);
  const totalSkus = Array.from(idx.mapNmToSkus.values()).reduce((a,s)=>a+s.size,0);
  log_('INFO','Stocks','index_stats', `nmTotal:${nmAll.length}, nmWithSku:${idx.mapNmToSkus.size}, skuTotal:${totalSkus}`, null);

  // 4) запросы по складам: шлём SKU, разворачиваем ответ в RAW и агрегируем по nmId
  _raw_clear_();
  const out = [];
  let sentSkus = 0, gotItems = 0, nonZero = 0;

  const CHUNK = 300; // безопасный батч
  for (const wh of warehouses){
    const skuAll = [];
    idx.mapNmToSkus.forEach(set => set.forEach(s => skuAll.push(s)));
    for (let i=0;i<skuAll.length;i+=CHUNK){
      const part = skuAll.slice(i,i+CHUNK);
      sentSkus += part.length;
      const items = _stocks_fetchBySkus_(H_MP, wh.id, part); // [{sku, amount}]
      gotItems += items.length;

      // развернём в RAW и агрегируем по nm
      for (const it of items){
        const sku = String(it.sku||'');
        const qty = Number(it.amount||0);
        const nm  = idx.mapSkuToNm.get(sku) || 0;
        _raw_append_([new Date(), wh.id, wh.name, nm, sku, qty]);
        if (qty>0) nonZero++;

        if (nm && wh.name){
          out.push([nm, wh.name, qty]);
        }
      }
    }
  }

  // запись сводной таблицы (nmId / Склад / Кол-во)
  _stocks_writeTable_(out);

  log_('INFO','Stocks','done', `Stocks_API: ${out.length} строк`, { rows: out.length, sentSkus, gotItems, nonZero });
}

/* === helpers === */

// объединяем источники nmId
function _nm_collectAll_(){
  const ss = SpreadsheetApp.getActive();
  const set = new Set();

  const shMap = ss.getSheetByName('Map_NMID');
  if (shMap && shMap.getLastRow()>1){
    const v = shMap.getRange(2,1,shMap.getLastRow()-1,2).getValues();
    v.forEach(r => { const nm = Number(r[1]); if (nm) set.add(nm); });
  }
  const shAdsCache = ss.getSheetByName('Ads_Nm_Cache');
  if (shAdsCache && shAdsCache.getLastRow()>1){
    const v = shAdsCache.getRange(2,1,shAdsCache.getLastRow()-1,1).getValues();
    v.forEach(r => { const nm = Number(r[0]); if (nm) set.add(nm); });
  }
  const shAds = ss.getSheetByName('Ads_API');
  if (shAds && shAds.getLastRow()>1){
    const H = shAds.getRange(1,1,1,shAds.getLastColumn()).getDisplayValues()[0].map(s=>String(s).toLowerCase());
    const cNm = H.findIndex(h => h.includes('nm'));
    if (cNm>=0){
      const v = shAds.getRange(2,1,shAds.getLastRow()-1,shAds.getLastColumn()).getValues();
      v.forEach(r => { const nm = Number(r[cNm]); if (nm) set.add(nm); });
    }
  }
  return set;
}

// склады продавца
function _stocks_getWarehouses_(headers){
  const url = 'https://marketplace-api.wildberries.ru/api/v3/warehouses';
  const t0 = Date.now();
  const res = UrlFetchApp.fetch(url, {method:'get', headers, muteHttpExceptions:true});
  const code = res.getResponseCode(), body = res.getContentText()||'';
  log_('INFO','HTTP','response', `${code} ${url} (${Date.now()-t0} ms)`, {respBytes: body.length});

  if (code!==200) return [];
  try{
    const j = JSON.parse(body||'[]');
    const arr = Array.isArray(j) ? j : (j?.data || j?.warehouses || []);
    return arr.map(x => ({
      id: Number(x?.id ?? x?.warehouseId ?? x?.warehouse_id),
      name: String(x?.name ?? x?.warehouseName ?? x?.office ?? '')
    })).filter(x => x.id && x.name);
  }catch(_){ return []; }
}

// индекс nm <-> sku (через Content API V2)
function _buildSkuIndex_(headers, nmAll){
  const mapNmToSkus = new Map();
  const mapSkuToNm  = new Map();
  const url = 'https://content-api.wildberries.ru/content/v2/get/cards/list';

  const CH = 100;
  for (let i=0;i<nmAll.length;i+=CH){
    const part = nmAll.slice(i,i+CH);
    const body = JSON.stringify({ sort:{cursor:{limit:1000}}, filter:{ nmID: part } });
    const t0 = Date.now();
    const res = UrlFetchApp.fetch(url, {method:'post', headers, payload: body, muteHttpExceptions:true});
    const code = res.getResponseCode(), text = res.getContentText()||'';
    log_('INFO','HTTP','response', `${code} ${url} (${Date.now()-t0} ms)`, {variant:'v2.cursor.filter.nmID', respBytes: String(text).length});
    if (code!==200) continue;

    try{
      const j = JSON.parse(text||'{}');
      const cards = j?.data?.cards || j?.cards || [];
      for (const c of cards){
        const nm = Number(c?.nmID ?? c?.nmId ?? c?.nmid ?? c?.nm);
        if (!nm) continue;
        const sizes = Array.isArray(c?.sizes) ? c.sizes : [];
        for (const s of sizes){
          const arr = Array.isArray(s?.skus) ? s.skus : [];
          for (const raw of arr){
            const sku = String((typeof raw==='object' && raw ? raw.sku : raw)||'').trim();
            if (!sku) continue;
            if (!mapNmToSkus.has(nm)) mapNmToSkus.set(nm, new Set());
            mapNmToSkus.get(nm).add(sku);
            mapSkuToNm.set(sku, nm);
          }
        }
      }
    }catch(_){}
  }
  return { mapNmToSkus, mapSkuToNm };
}

// запрос остатков по SKU для одного склада
function _stocks_fetchBySkus_(headers, whId, skus){
  const url = `https://marketplace-api.wildberries.ru/api/v3/stocks/${whId}`;
  const body = JSON.stringify({ skus });
  const t0 = Date.now();
  const res = UrlFetchApp.fetch(url, {method:'post', headers, payload: body, muteHttpExceptions:true});
  const code = res.getResponseCode(), text = res.getContentText()||'';
  log_('INFO','HTTP','response', `${code} ${url} (${Date.now()-t0} ms)`, {whId, bodyKey:'skus', batch: skus.length, respBytes: String(text).length});
  if (code!==200) return [];
  try{
    const j = JSON.parse(text||'[]');
    return Array.isArray(j) ? j : (j?.stocks || j?.data || []);
  }catch(_){ return []; }
}

function WB_debugSkuIndex(){
  const S = getSettings_();
  const auth = (S.stockToken && S.stockToken.trim()) || S.token;
  if (!auth) throw new Error('Нет токена Authorization (Settings → API токен).');

  const H_CT = { 'Authorization': auth, 'Content-Type':'application/json', 'Accept':'application/json' };

  // 1) Соберём все nmID (Map_NMID + Ads_Nm_Cache + Ads_API)
  const nmSet = _nm_collectAll_();
  const nmAll = Array.from(nmSet);

  // 2) Построим индекс nm <-> sku (через Content API)
  const idx = _buildSkuIndex_(H_CT, nmAll);

  // 3) Выгрузим в лист Sku_Index: nmId, sku (по одной строке на связку)
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName('Sku_Index') || ss.insertSheet('Sku_Index');
  sh.clearContents();
  sh.getRange(1,1,1,2).setValues([['nmId','sku']]);

  const rows = [];
  idx.mapNmToSkus.forEach((set, nm) => {
    set.forEach(sku => rows.push([nm, String(sku)]));
  });

  if (rows.length) sh.getRange(2,1,rows.length,2).setValues(rows);
  sh.setFrozenRows(1);

  // Итоговая строка с цифрами — чтобы было видно масштаб
  sh.getRange(1,4,1,2).setValues([['nmTotal', nmAll.length]]);
  sh.getRange(2,4,1,2).setValues([['nmWithSku', idx.mapNmToSkus.size]]);
  const totalSkus = rows.length;
  sh.getRange(3,4,1,2).setValues([['skuTotal', totalSkus]]);
}

function WB_debugSkuFor(nm){
  const S = getSettings_();
  const auth = (S.stockToken && S.stockToken.trim()) || S.token;
  if (!auth) throw new Error('Нет токена Authorization (Settings → API токен).');
  const H_CT = { 'Authorization': auth, 'Content-Type':'application/json', 'Accept':'application/json' };

  const idx = _buildSkuIndex_(H_CT, [Number(nm)]);
  const set = idx.mapNmToSkus.get(Number(nm)) || new Set();

  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName('Sku_Index') || ss.insertSheet('Sku_Index');
  sh.clearContents();
  sh.getRange(1,1,1,2).setValues([['nmId','sku']]);
  const rows = Array.from(set).map(s => [Number(nm), String(s)]);
  if (rows.length) sh.getRange(2,1,rows.length,2).setValues(rows);
  sh.setFrozenRows(1);
}


// RAW
function _raw_clear_(){
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(SHEET_RAW_STOCKS) || ss.insertSheet(SHEET_RAW_STOCKS);
  sh.clearContents();
  sh.getRange(1,1,1,6).setValues([['ts','whId','whName','nmId','sku','amount']]);
  sh.setFrozenRows(1);
}
function _raw_append_(row){
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(SHEET_RAW_STOCKS);
  const lr = sh.getLastRow();
  sh.getRange(lr+1,1,1,row.length).setValues([row]);
}

// сводная таблица nm / склад / qty
function _stocks_writeTable_(rows){
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(SHEET_STOCKS_API) || ss.insertSheet(SHEET_STOCKS_API);
  sh.clearContents();
  sh.getRange(1,1,1,3).setValues([['nmId','Склад','Кол-во']]);
  if (rows && rows.length) sh.getRange(2,1,rows.length,3).setValues(rows);
  sh.setFrozenRows(1);
}
