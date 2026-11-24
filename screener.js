// 一阶段分歧低开竞价买入策略 - 自动筛选脚本
// 数据源：东方财富 push2delay（板块/成分/实时行情/竞价分时） + 腾讯（日K/15分钟K）
//         东方财富不可用时自动切新浪（成分/行情）+ 腾讯（竞价分时）兜底
//         网络被沙箱拦截时自动切换 PowerShell 取数通道（IWR 稳定可用）
// 运行：node screener.js
// 可选：node screener.js --relax   （放宽一阶段分歧阈值到 -1.5%，当严格条件无候选时使用）

'use strict';

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { calcSpace, spaceAlerts } = require('./lib_space');

// ============ PowerShell 取数通道（沙箱内 Node 直连被限时用它兜底） ============
const PS_HELPER = path.join(__dirname, 'fetch_helper.ps1');
class PsFetcher {
  constructor() {
    this.buf = '';
    this.queue = [];
    this.proc = spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', PS_HELPER], { stdio: ['pipe', 'pipe', 'ignore'] });
    this.proc.stdout.setEncoding('utf8');
    this.proc.stdout.on('data', d => this._onData(d));
    this.proc.on('exit', () => {
      this.alive = false;
      this.queue.forEach(q => q.reject(new Error('PS 通道退出')));
      this.queue = [];
    });
  }
  _onData(d) {
    this.buf += d;
    let idx;
    while ((idx = this.buf.indexOf('\n')) >= 0) {
      const line = this.buf.slice(0, idx).replace(/\r$/, '');
      this.buf = this.buf.slice(idx + 1);
      const q = this.queue.shift();
      if (!q) continue;
      if (line.startsWith('OK ')) q.resolve(Buffer.from(line.slice(3), 'base64'));
      else q.reject(new Error('PS: ' + line.slice(0, 120)));
    }
  }
  fetch(url) {
    return new Promise((resolve, reject) => {
      this.queue.push({ resolve, reject });
      this.proc.stdin.write(url + '\n');
    });
  }
}

const PS_POOL = Array.from({ length: 6 }, () => new PsFetcher());
let psIdx = 0;
function psFetch(url) {
  psIdx = (psIdx + 1) % PS_POOL.length;
  return PS_POOL[psIdx].fetch(url);
}
// node 退出时回收 PowerShell 子进程，避免管道悬挂
process.on('exit', () => {
  for (const p of PS_POOL) { try { p.proc.kill(); } catch (e) { /* ignore */ } }
});
process.on('SIGINT', () => process.exit(130));

// 统一取数：先 node 直连，连续失败后切 PowerShell 通道
let nodeFails = 0;
async function fetchBuf(url, charset = 'utf8') {
  try {
    const r = await fetch(url, { headers: UA, signal: AbortSignal.timeout(12000) });
    if (r.ok) {
      const b = Buffer.from(await r.arrayBuffer());
      return new TextDecoder(charset).decode(b);
    }
    throw new Error('HTTP ' + r.status);
  } catch (e) {
    nodeFails++;
  }
  const b = await psFetch(url);
  return new TextDecoder(charset).decode(b);
}

// ============ 可调参数 ============
const CFG = {
  CAP_LARGE: 800e8,          // 总市值 >= 800亿 => 大容量（手册口径）
  AUCTION_LARGE: 3.0,        // 大容量竞价占比 >= 3%（手册）
  AUCTION_MID: 5.0,          // 中盘竞价占比 >= 5%（手册）
  OPEN_MAX: 2.0,             // 2026-08-18 口径：开盘涨幅 <= +2%（低开/平开/小高开）都算低开
  DIV: -0.02,                // 一阶段分歧：单日跌幅 <= -2%
  DIV_DEEP: -0.05,           // 深度分歧：<= -5%
  DIV_LOOKBACK: 2,           // 2026-08-25 口径：分歧日须出现在前两日（交易日）
  RS_WINDOW: 20,             // RS 主周期（交易日）
  RS_WINDOW2: 10,            // RS 辅助周期
  SAMPLE: 15,                // 每板块用于估算板块指数的成分股数（按成交额取前 N）
  MAX_PER_BOARD: 5,          // 每板块最多进入候选的股票数
  MAX_BOARDS: 30,            // 最多保留的候选板块数（按分歧深度排序）
  MAX_CANDIDATES: 100,       // 进入 15分钟/竞价 计算的个股总数上限
  CONCURRENCY: 6,            // 并发请求数（调低避免触发网关限流）
  REQ_DELAY_MS: 20,          // 请求间隔
  RETRY_ROUNDS: 3,           // 整批失败重试轮数
  MIN_BOARD_SERIES: 40,      // 板块序列最短长度
  CORR_MIN: 0.50,            // 个股与板块日收益率相关性下限（非庄股）
};

// ===== 冲板趋势判定（2026-08-18 口径） =====
// 脚本用可得数据近似评分（0~6 分，>=3 视为"有冲板趋势"）：
//   竞价抢筹达标 +2｜明显放量(>=1.5倍阈值) +1｜开盘不深跌(> -3% 且 <= +2%) +1｜
//   板块翻红 +1｜板块内有接近涨停的票 +1｜盘中距涨停 <=3% +1｜已触涨停 +2
// 撤单/加单、封单厚度等只有逐笔/盘口能看到的细节，脚本标记"冲板待核"，需人工用同花顺竞价明细核对。
function limitPctOf(code) { return /^(30|68)/.test(code) ? 1.2 : 1.1; } // 创业板/科创板 20%，其余 10%
function downPctOf(code) { return limitPctOf(code) > 1.1 ? -19.5 : -9.8; } // 跌停阈值（2026-08-25 口径：跌停当日不追）
function chongbanScore({ gap, ratio, thr, boardPct, boardHasLimit, distLimitPct, limitHit }) {
  let s = 0;
  // 量价一致：放量 + 开盘不深跌才算"抢筹"；放量深低开（<= -3%）= 卖方主导，不算抢筹
  const liangjiaOK = gap != null && gap > -3 && gap <= 2;
  if (ratio != null && thr) {
    if (liangjiaOK && ratio >= thr) s += 2;          // 竞价抢筹达标（量价一致才有效）
    if (liangjiaOK && ratio >= thr * 1.5) s += 1;    // 明显放量
  }
  if (liangjiaOK) s += 1;                            // 开盘不深跌
  if (boardPct != null && boardPct > 0) s += 1;    // 板块翻红
  if (boardHasLimit) s += 1;                       // 板块内有冲板氛围
  if (distLimitPct != null && distLimitPct <= 3) s += 1; // 盘中距涨停近
  if (limitHit) s += 2;                            // 已触/封涨停
  return s;
}
function chongbanLabel(s) { return s >= 3 ? '冲板✓' : (s === 2 ? '冲板待核' : '冲板✗'); }

const RELAX = process.argv.includes('--relax');
if (RELAX) CFG.DIV = -0.015;
const OFFLINE = process.argv.includes('--offline');
const FETCHDAY = process.argv.includes('--fetchday');
const PRE_MARKET = process.argv.includes('--premarket');
const CHECK = process.argv.includes('--check');
const AFTER = process.argv.includes('--after');

const UA = { 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://quote.eastmoney.com/' };

// ============ 磁盘缓存 ============
const CACHE_DIR = path.join(__dirname, '.cache');
function cacheGet(key, ttlMs) {
  try {
    const p = path.join(CACHE_DIR, key.replace(/[^a-z0-9_]/gi, '_') + '.json');
    if (fs.existsSync(p)) {
      const d = JSON.parse(fs.readFileSync(p, 'utf8'));
      if (OFFLINE || Date.now() - d._t < ttlMs) return d.data;
    }
  } catch (e) { /* ignore */ }
  return null;
}
function cacheSet(key, data) {
  try {
    const p = path.join(CACHE_DIR, key.replace(/[^a-z0-9_]/gi, '_') + '.json');
    fs.mkdirSync(path.dirname(p), { recursive: true });
    fs.writeFileSync(p, JSON.stringify({ _t: Date.now(), data }));
  } catch (e) { /* ignore */ }
}

// ============ 基础工具 ============
async function emJson(url, tries = 3) {
  // 域名轮换：push2delay 的 IP 会漂移，逐个备用域名重试
  const hosts = ['push2delay', '1.push2delay', '8.push2delay', '17.push2delay', '82.push2delay', '92.push2delay'];
  const schemes = ['https', 'http'];
  let lastErr = '';
  for (let attempt = 0; attempt < tries * hosts.length * schemes.length; attempt++) {
    const h = hosts[attempt % hosts.length];
    const scheme = schemes[Math.floor(attempt / hosts.length) % schemes.length];
    const u = url.replace('https://push2delay', `${scheme}://${h}`);
    try {
      const t = await fetchBuf(u);
      return JSON.parse(t);
    } catch (e) {
      lastErr = (e.cause && e.cause.code) || String(e).slice(0, 60);
      await new Promise(r => setTimeout(r, 600));
    }
  }
  throw new Error('东方财富接口全部失败 [' + lastErr + ']: ' + url.slice(0, 80));
}

async function txJson(url, tries = 3) {
  for (let i = 0; i < tries; i++) {
    try {
      const t = await fetchBuf(url);
      return JSON.parse(t);
    } catch (e) {
      if (i === tries - 1) throw e;
      await new Promise(r => setTimeout(r, 400 * (i + 1)));
    }
  }
}

function symOf(mkt, code) { return (String(mkt) === '1' ? 'sh' : 'sz') + code; }

function pct(v) { return v === '-' || v == null ? null : parseFloat(v); }

// 手册口径：总市值 >= 800亿 = 大容量(阈值3%)，其余中盘(阈值5%)
function capInfo(s) {
  const cap = s.totalCap || 0;
  return cap >= CFG.CAP_LARGE ? { cls: '大容量', thr: CFG.AUCTION_LARGE } : { cls: '中盘', thr: CFG.AUCTION_MID };
}

// 并发池
async function pool(items, worker, conc) {
  const results = new Array(items.length);
  let idx = 0;
  async function run() {
    while (idx < items.length) {
      const i = idx++;
      try { results[i] = await worker(items[i], i); }
      catch (e) { results[i] = { error: String(e).slice(0, 120) }; }
    }
  }
  const workers = [];
  for (let i = 0; i < Math.min(conc, items.length); i++) workers.push(run());
  await Promise.all(workers);
  return results;
}

// 带整批重试的并发池（网关限流冷却后自动恢复）
async function poolRetry(items, worker, conc, rounds = CFG.RETRY_ROUNDS) {
  let cur = items, idxMap = items.map((_, i) => i);
  const res = new Array(items.length);
  for (let r = 0; r < rounds; r++) {
    const out = await pool(cur, worker, conc);
    const fails = [];
    out.forEach((v, i) => { if (v && v.error) fails.push(i); else res[idxMap[i]] = v; });
    if (!fails.length) break;
    if (r === rounds - 1) { fails.forEach(i => res[idxMap[i]] = out[i]); break; }
    const wait = fails.length > items.length / 2 ? 30000 : 15000;
    console.log(`  [重试] ${fails.length} 个请求失败，${wait / 1000} 秒后重试...`);
    await new Promise(r2 => setTimeout(r2, wait));
    cur = fails.map(i => cur[i]);
    idxMap = fails.map(i => idxMap[i]);
  }
  return res;
}

// ============ 数据获取 ============
const delay = ms => new Promise(r => setTimeout(r, ms));

// 全市场股票（沪深主板/创业板/科创板），含行业归属
async function fetchUniverse() {
  const base = 'https://push2delay.eastmoney.com/api/qt/clist/get';
  const fsParam = 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23';
  const fields = 'f12,f13,f14,f2,f3,f6,f8,f15,f16,f17,f18,f20,f21,f24,f25,f100';
  const first = await emJson(`${base}?pn=1&pz=100&po=1&np=1&fltt=2&invt=2&fid=f12&fs=${fsParam}&fields=${fields}`);
  const total = first.data.total;
  let diff = first.data.diff || [];
  const pages = Math.ceil(total / 100);
  for (let p = 2; p <= pages; p++) {
    const d = await emJson(`${base}?pn=${p}&pz=100&po=1&np=1&fltt=2&invt=2&fid=f12&fs=${fsParam}&fields=${fields}`);
    diff = diff.concat(d.data.diff || []);
  }
  const stocks = [];
  for (const x of diff) {
    const code = String(x.f12 || '');
    const name = String(x.f14 || '');
    // 只保留主板 + 创业板，排除科创板/北交所/ST/退市
    if (!/^(60|000|001|002|003|300|301|302)/.test(code)) continue;
    if (/ST|退/.test(name)) continue;
    if (!x.f100 || x.f100 === '-') continue;
    stocks.push({
      code, mkt: String(x.f13 || '0'), name,
      industry: String(x.f100),
      price: pct(x.f2), chgPct: pct(x.f3), amount: pct(x.f6),
      turnover: pct(x.f8), high: pct(x.f15), low: pct(x.f16),
      open: pct(x.f17), prevClose: pct(x.f18),
      totalCap: pct(x.f20), floatCap: pct(x.f21),
      pct60: pct(x.f24), pctYtd: pct(x.f25),
    });
  }
  return stocks;
}

// 腾讯全市场（二级行业 hy2 + 成分股分页）：2026-10-28 起作为首选数据源
async function fetchTencentUniverse() {
  const RANK = 'https://proxy.finance.qq.com/cgi/cgi-bin/rank';
  const H = { 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://gu.qq.com/', 'Accept': 'application/json, text/plain, */*' };
  const jget = async (url, tries = 3) => {
    let lastErr;
    for (let i = 0; i < tries; i++) {
      try {
        const r = await fetch(url, { headers: H, signal: AbortSignal.timeout(15000) });
        return await r.json();
      } catch (e) {
        lastErr = e;
        if (i < tries - 1) await delay(400 * (i + 1));
      }
    }
    throw lastErr;
  };
  const boards = [];
  for (let off = 0; ; off += 100) {
    const d = await jget(`${RANK}/pt/getRank?board_type=hy2&sort_type=priceChange&direct=down&offset=${off}&count=100`);
    const list = (d.data && d.data.rank_list) || [];
    boards.push(...list);
    if (list.length < 100) break;
  }
  if (!boards.length) throw new Error('腾讯板块列表为空');
  const pages = await poolRetry(boards, async b => {
    const rows = [];
    for (let off = 0; ; off += 100) {
      const d = await jget(`${RANK}/hs/getBoardRankList?board_type=hy2&board_code=${b.code}&sort_type=priceChange&direct=down&offset=${off}&count=100`);
      const list = (d.data && d.data.rank_list) || [];
      rows.push(...list);
      if (list.length < 100) break;
    }
    return { rows };
  }, 8);
  const seen = new Set();
  const stocks = [];
  pages.forEach((p, idx) => {
    if (!p || p.error) return;
    const board = boards[idx];
    for (const x of p.rows) {
      const code = String(x.code || '').replace(/^(sh|sz)/i, '');
      const name = String(x.name || '');
      if (!/^(60|000|001|002|003|300|301|302)/.test(code)) continue;
      if (/ST|退/.test(name)) continue;
      if (seen.has(code)) continue;
      seen.add(code);
      stocks.push({
        code, mkt: code.startsWith('6') ? '1' : '0', name,
        industry: board.name,
        price: pct(x.zxj), chgPct: pct(x.zdf),
        amount: (pct(x.turnover) || 0) * 1e4,        // 腾讯 rank 单位万元 → 元
        turnover: pct(x.hsl),
        high: null, low: null, open: null, prevClose: null,
        totalCap: (pct(x.zsz) || 0) * 1e8,           // 亿元 → 元
        floatCap: (pct(x.ltsz) || 0) * 1e8,
        pct60: null, pctYtd: null,
      });
    }
  });
  return stocks;
}

// 腾讯日K（前复权）
async function txDayKline(sym, limit = 120) {
  const url = `https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=${sym},day,,,${limit},qfq`;
  const d = await txJson(url);
  const node = d.data && d.data[sym];
  const rows = (node && (node.qfqday || node.day)) || [];
  return rows.map(r => {
    const a = Array.isArray(r) ? r : String(r).split(',');
    return { date: a[0], open: +a[1], close: +a[2], high: +a[3], low: +a[4], volume: +a[5] };
  }).filter(b => b.date && isFinite(b.close) && b.close > 0);
}

async function dayKlineCached(sym, limit = 140) {
  const key = 'day/' + sym;
  const c = cacheGet(key, 4 * 3600 * 1000);
  if (c) return c;
  await delay(CFG.REQ_DELAY_MS);
  const b = await txDayKline(sym, limit);
  if (b.length) cacheSet(key, b);
  return b;
}

// 腾讯15分钟K
async function txM15Kline(sym, limit = 320) {
  const url = `https://ifzq.gtimg.cn/appstock/app/kline/mkline?param=${sym},m15,,${limit}`;
  const d = await txJson(url);
  const node = d.data && d.data[sym];
  const rows = (node && node.m15) || [];
  return rows.map(r => {
    const a = Array.isArray(r) ? r : String(r).split(',');
    return { date: a[0], open: +a[1], close: +a[2], high: +a[3], low: +a[4], volume: +a[5] };
  }).filter(b => b.date && isFinite(b.close) && b.close > 0);
}

async function m15KlineCached(sym, limit = 320) {
  const key = 'm15/' + sym;
  const c = cacheGet(key, 25 * 60 * 1000);
  if (c) return c;
  await delay(CFG.REQ_DELAY_MS);
  const b = await txM15Kline(sym, limit);
  if (b.length) cacheSet(key, b);
  return b;
}

// 东方财富分时（当天第一根 = 竞价；ndays=2 可拿到昨日分时作参考）
async function emMinute(sym, ndays = 1) {
  const url = `https://push2delay.eastmoney.com/api/qt/stock/trends2/get?secid=${sym === 'sh' ? 1 : 0}.${sym.slice(2)}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57&ndays=${ndays}&iscr=0`;
  const parse = d => {
    const trends = (d && d.data && d.data.trends) || [];
    if (!trends.length) return null;
    return trends.map(t => {
      const a = String(t).split(',');
      return { time: a[0], open: +a[1], close: +a[2], high: +a[3], low: +a[4], volume: +a[5], amount: +a[6] };
    });
  };
  const rowsOf = d => (d && (d.rc === 0 || d.rc == null)) ? parse(d) : null;
  try {
    const d = await emJson(url);
    const rows = rowsOf(d);
    if (rows) return rows;
    // 东财 node 直连对沪市常返回 rc=100/trends 为空；改走 PowerShell 通道重试同一 URL（实测可返回当天分时）
    const t2 = await psFetch(url);
    const d2 = JSON.parse(new TextDecoder('utf8').decode(t2));
    const rows2 = rowsOf(d2);
    if (rows2) return rows2;
  } catch (e) {
    /* 继续走腾讯 m1 兜底 */
  }
  // 东财 trends2 空/失败（沪市负载高时常 rc=100）：腾讯 m1 1分钟K线首根含竞价量，折算竞价额
  try {
    const m1 = await tencentM1Minute(sym);
    if (m1.length) return m1;
  } catch (e) {
    /* 落到腾讯分时兜底 */
  }
  return tencentMinute(sym); // 腾讯兜底
}

// 腾讯 1分钟K线兜底（东财沪市分时不可用时）：首根 09:30 含竞价量，amount ≈ volume(手)*100*open
// 返回与东财分时相同结构：首根 amount = 竞价额，volume/amount 按“分时”累计口径逐根累加
async function tencentM1Minute(sym) {
  const url = `https://ifzq.gtimg.cn/appstock/app/kline/mkline?param=${sym},m1,,320`;
  const d = await txJson(url);
  const rows = (d.data && d.data[sym] && d.data[sym].m1) || [];
  const today8 = todayStr().replace(/-/g, '');
  const bars = rows.map(t => {
    const a = Array.isArray(t) ? t : String(t).split(/[ ,]+/);
    return { time: a[0], open: +a[1], close: +a[2], high: +a[3], low: +a[4], volume: +a[5] };
  }).filter(b => b.time && b.time.startsWith(today8) && b.open > 0 && b.volume > 0);
  if (!bars.length) return [];
  let cumAmt = 0, cumVol = 0;
  return bars.map(b => {
    cumVol += b.volume;
    cumAmt += b.volume * 100 * b.open;
    return { time: b.time, open: b.open, close: b.close, high: b.high, low: b.low, volume: cumVol, amount: cumAmt };
  });
}

async function minuteCached(sym, ndays = 1) {
  const key = (ndays > 1 ? 'min2/' : 'min/') + sym;
  const c = cacheGet(key, ndays > 1 ? 12 * 3600 * 1000 : 5 * 60 * 1000);
  if (c && c.length) {
    const t0 = String(c[0].time);
    if (!t0.includes('-') || t0.startsWith(todayStr())) return c; // 东财格式需是今天，腾讯格式视为新鲜
  }
  await delay(CFG.REQ_DELAY_MS);
  const m = await emMinute(sym, ndays);
  if (m.length) {
    const t0 = String(m[0].time);
    if (!t0.includes('-') || t0.startsWith(todayStr())) cacheSet(key, m);
  }
  return m;
}

// 腾讯分时兜底（第一根 09:30 即竞价，与东财数值一致）
async function tencentMinute(sym) {
  const url = `https://web.ifzq.gtimg.cn/appstock/app/minute/query?code=${sym}`;
  const d = await txJson(url);
  const rows = (d.data && d.data[sym] && d.data[sym].data && d.data[sym].data.data) || [];
  return rows.map(t => {
    const a = String(t).split(' ');
    return { time: a[0], close: +a[1], volume: +a[2], amount: +a[3] };
  }).filter(x => isFinite(x.amount));
}

// 新浪全市场（按行业板块聚合，兜底东方财富不可用的情况）
async function fetchSinaUniverse() {
  const listUrl = 'http://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php';
  const body = await fetchBuf(listUrl, 'gbk');
  const m = body.match(/\{.*\}/);
  if (!m) throw new Error('新浪板块列表解析失败');
  const map = JSON.parse(m[0]);
  const boards = Object.keys(map).map(k => { const v = String(map[k]).split(','); return { code: v[0], name: v[1] }; });
  console.log(`  新浪板块 ${boards.length} 个，拉取成分股...`);
  const stocks = [];
  const seen = new Set();
  await pool(boards, async b => {
    let page = 1, got = [];
    while (page <= 3) {
      const u = `http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=${page}&num=200&sort=symbol&asc=1&node=${b.code}&symbol=&_s_r_a=init`;
      const arr = JSON.parse(await fetchBuf(u, 'gbk'));
      got = got.concat(arr);
      if (arr.length < 200) break;
      page++;
    }
    for (const x of got) {
      const code = String(x.code);
      if (seen.has(code)) continue;
      if (!/^(60|000|001|002|003|300|301|302)/.test(code)) continue;
      seen.add(code);
      const mkt = String(x.symbol).startsWith('sh') ? '1' : '0';
      stocks.push({
        code, mkt, name: x.name, industry: b.name,
        price: parseFloat(x.trade), chgPct: parseFloat(x.changepercent),
        amount: parseFloat(x.amount), turnover: parseFloat(x.turnoverratio),
        high: parseFloat(x.high), low: parseFloat(x.low),
        open: parseFloat(x.open), prevClose: parseFloat(x.settlement),
        totalCap: parseFloat(x.mktcap) * 1e4, floatCap: parseFloat(x.nmc) * 1e4,
        pct60: null, pctYtd: null,
      });
    }
    return 1;
  }, CFG.CONCURRENCY);
  return stocks;
}

// ============ 序列计算 ============
function pctSeries(bars) {
  const out = [];
  for (let i = 1; i < bars.length; i++) {
    if (PRE_MARKET && bars[i].date === todayStr()) continue; // 竞价前模式剔除今日未走完K线
    const p = bars[i].close / bars[i - 1].close - 1;
    out.push({ date: bars[i].date, pct: p, close: bars[i].close, high: bars[i].high, low: bars[i].low, volume: bars[i].volume });
  }
  return out;
}

// 上一交易日成交额（量×收盘价），用于板块采样排序
function lastTurnover(d) {
  const bars = d.bars || [];
  if (!bars.length) return 0;
  const b = bars[bars.length - 1].date === todayStr() ? bars[bars.length - 2] : bars[bars.length - 1];
  return b ? b.volume * b.close : 0;
}

function maOf(arr, n, i) {
  if (i < n - 1) return null;
  let s = 0;
  for (let k = 0; k < n; k++) s += arr[i - k];
  return s / n;
}

function retOver(pctArr, n) {
  if (pctArr.length < n + 1) return null;
  let p = 1;
  for (let i = pctArr.length - n; i < pctArr.length; i++) p *= (1 + pctArr[i]);
  return p - 1;
}

function corr(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 10) return null;
  let ma = 0, mb = 0;
  for (let i = 0; i < n; i++) { ma += a[i]; mb += b[i]; }
  ma /= n; mb /= n;
  let sxy = 0, sxx = 0, syy = 0;
  for (let i = 0; i < n; i++) {
    sxy += (a[i] - ma) * (b[i] - mb);
    sxx += (a[i] - ma) ** 2;
    syy += (b[i] - mb) ** 2;
  }
  if (sxx === 0 || syy === 0) return null;
  return sxy / Math.sqrt(sxx * syy);
}

// 由成分股聚合出板块日序列（等权）
function aggregateBoard(stockSeriesList) {
  const byDate = new Map();
  for (const series of stockSeriesList) {
    for (const p of series) {
      const e = byDate.get(p.date) || { s: 0, n: 0 };
      e.s += p.pct; e.n++;
      byDate.set(p.date, e);
    }
  }
  const dates = [...byDate.keys()].sort().filter(d => !(PRE_MARKET && d === todayStr()));
  let close = 1000;
  const rows = [];
  for (const d of dates) {
    const e = byDate.get(d);
    if (e.n < Math.min(8, stockSeriesList.length)) continue;
    close = close * (1 + e.s / e.n);
    rows.push({ date: d, pct: e.s / e.n, close });
  }
  return rows;
}

// ============ 筛选 ============
function boardPass(rows) {
  // 剔除今天未收盘的一根（用昨日及以前判断）
  const today = rows[rows.length - 1] && rows[rows.length - 1].date;
  const data = today && today === todayStr() ? rows.slice(0, -1) : rows;
  if (data.length < CFG.MIN_BOARD_SERIES) return null;
  const closes = data.map(r => r.close);
  const pcts = data.map(r => r.pct);
  const i = closes.length - 1;
  const ma5 = maOf(closes, 5, i), ma10 = maOf(closes, 10, i), ma20 = maOf(closes, 20, i);
  const ma20b = maOf(closes, 20, i - 3);
  if (ma5 == null || ma20 == null || ma20b == null) return null;
  // 上升趋势：收盘 > MA20 且 MA20 上行 且 MA5 > MA10
  const uptrend = closes[i] > ma20 && ma20 > ma20b && ma5 > ma10;
  // 一阶段分歧：前两日内存在 <= -2% 大阴线，且之后收复（收盘回到分歧日上方或MA20上方）
  let div = null;
  for (let k = 0; k < CFG.DIV_LOOKBACK && k < pcts.length; k++) {
    const j = pcts.length - 1 - k;
    if (pcts[j] <= CFG.DIV) {
      const recovered = closes[i] > closes[j] || closes[i] > ma20;
      if (recovered) {
        div = { date: data[j].date, pct: pcts[j], deep: pcts[j] <= CFG.DIV_DEEP, recovered };
        break;
      }
    }
  }
  if (!uptrend || !div) return null;
  return { uptrend, div, todayPct: rows[rows.length - 1].pct };
}

function todayStr() {
  const d = new Date();
  const p = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

// ============ 主流程 ============
// 竞价前模式：用上一交易日（周五）收盘数据筛候选，低开/竞价留待 09:25 验证
async function runPreMarket(candidateList) {
  const t0 = Date.now();
  const mktOf = s => String(s.mkt != null ? s.mkt : (s.code[0] === '6' ? '1' : '0'));
  const today8 = todayStr().replace(/-/g, '');

  console.log(`竞价前模式：对 ${candidateList.length} 只 RS>0 候选做周五收盘口径检查...`);
  const m15Res = await poolRetry(candidateList.map(s => s), s => m15KlineCached(symOf(mktOf(s), s.code), 320).then(bars => ({ bars })), CFG.CONCURRENCY);
  const minRes = await poolRetry(candidateList.map(s => s), s => minuteCached(symOf(mktOf(s), s.code), 2).then(m => ({ m })), CFG.CONCURRENCY);

  const rows = [];
  candidateList.forEach((s, idx) => {
    const mk = m15Res[idx];
    const mn = minRes[idx];
    const all15 = mk && !mk.error ? mk.bars : [];
    const minutesAll = mn && !mn.error ? mn.m : [];

    // 参考日 = 最后一根完整日K（周五）
    const bars = s.dayBars || [];
    const last = bars.length ? bars[bars.length - 1] : null;
    const refDate = last && last.date !== todayStr() ? last.date : (bars.length > 1 ? bars[bars.length - 2].date : (last ? last.date : null));
    const refDate8 = refDate ? refDate.replace(/-/g, '') : null;

    // 15分钟线：只取到参考日收盘
    const m15 = refDate8 ? all15.filter(b => b.date.slice(0, 8) <= refDate8) : all15;
    let ma5 = null, ma80 = null, pullDist = null, bias = null, buyGrade = '—';
    if (m15.length >= 80) {
      const closes = m15.map(x => x.close);
      ma5 = closes.slice(-5).reduce((a, b) => a + b, 0) / 5;
      ma80 = closes.slice(-80).reduce((a, b) => a + b, 0) / 80;
      const last3 = m15.slice(-3);
      const minLow = Math.min(...last3.map(x => x.low));
      pullDist = (minLow / ma80 - 1) * 100;
      bias = (ma5 / ma80 - 1) * 100;
      const bullish = ma5 > ma80;
      if (!bullish) buyGrade = '未多头';
      else if (bias <= 2.0) buyGrade = '回踩A(最佳)';
      else if (bias <= 4.0) buyGrade = '回踩B';
      else buyGrade = '多头(未回踩)';
    }

    // 周五分时出货参考
    const fridayMin = refDate ? minutesAll.filter(x => String(x.time).startsWith(refDate)) : [];
    let distFlag = '参考不足';
    if (fridayMin.length >= 60) {
      const lastM = fridayMin[fridayMin.length - 1];
      const rng = (lastM.high != null ? lastM.high : lastM.close) - (lastM.low != null ? lastM.low : lastM.close);
      const closePos = rng > 0 ? (lastM.close - (lastM.low != null ? lastM.low : lastM.close)) / rng : 1;
      const half = Math.floor(fridayMin.length / 2);
      const avg1 = fridayMin.slice(0, half).reduce((a, x) => a + x.close, 0) / half;
      const avg2 = fridayMin.slice(half).reduce((a, x) => a + x.close, 0) / (fridayMin.length - half);
      const flags = [];
      if (closePos < 0.3) flags.push('收在低位');
      if (avg2 < avg1 && lastM.close < (lastM.open != null ? lastM.open : lastM.close)) flags.push('尾盘走弱');
      distFlag = flags.length ? '疑似:' + flags.join(',') : '无明显';
    }

    const zhuang = s.zhuangFlags || '';
    const independent = s.corr != null && s.corr < CFG.CORR_MIN;
    const noZhuang = !zhuang && !independent;
    const thr = s.auctionTh || (s.capClass === '大容量' ? CFG.AUCTION_LARGE : CFG.AUCTION_MID);
    const line = thr / 100 * (s.prevDayAmt || 0) / 1e8; // 竞价达标线（亿）
    // 涨幅空间 + 异动状态（10日200%口径；剔除今日未走完K线）
    const spBars = PRE_MARKET ? bars.filter(b => b.date !== todayStr()) : bars;
    const sp = calcSpace(s.code, spBars);
    let spAlert = sp ? spaceAlerts(sp, s.code) : '无首板数据';
    // 2026-08-25 口径：昨日（参考日）跌停 → 今日低开才重新评估；不因跌停本身剔除，仅标注提醒
    if (spBars.length >= 2) {
      const lb = spBars[spBars.length - 1];
      const pb = spBars[spBars.length - 2];
      if (lb && pb && (lb.close / pb.close - 1) * 100 <= downPctOf(s.code)) spAlert += ' | 昨日跌停·次日低开再核';
    }

    rows.push({
      board: s.board, code: s.code, name: s.name,
      refDate,
      price: s.price, chgPct: s.chgPct,
      rs20: s.rs20, rs10: s.rs10, corr: s.corr,
      buyGrade, bias, pullDist,
      sp, spAlert,
      totalCapYi: s.totalCapYi, capClass: s.capClass, thr,
      prevDayAmtYi: s.prevDayAmt ? s.prevDayAmt / 1e8 : null,
      aucLineYi: line,
      zhuang, independent, noZhuang, distFlag,
      pass580: buyGrade !== '未多头' && buyGrade !== '—',
    });
  });

  const ban = r => r.sp && r.sp.gain >= 200; // 已到200%停牌区，直接剔除
  const A = rows.filter(r => r.pass580 && r.noZhuang && !r.distFlag.startsWith('疑似') && !ban(r));
  const B = rows.filter(r => !A.includes(r) && r.noZhuang && !ban(r));
  A.sort((a, b) => (gradeRank(a.buyGrade) - gradeRank(b.buyGrade)) || (b.rs20 - a.rs20));
  B.sort((a, b) => (gradeRank(a.buyGrade) - gradeRank(b.buyGrade)) || (b.rs20 - a.rs20));

  console.log('\n================ 竞价前候选（上一交易日收盘口径） ================\n');
  console.log(`【A档】板块分歧+上升 + RS>0 + 5/80多头 + 非庄 + 前日无出货  共 ${A.length} 只（低开/竞价待 09:25 验证）`);
  printPreTable(A);
  console.log(`\n【B档】板块+RS+非庄达标，但 5/80 未多头或周五出货存疑  共 ${B.length} 只`);
  printPreTable(B.slice(0, 25));

  const today = todayStr().replace(/-/g, '');
  const csvPath = path.join(__dirname, `竞价前候选_${today}.csv`);
  const csv = ['板块,代码,名称,参考日,昨收,昨涨%,RS20%,RS10%,5/80,5/80乖离%,价格距80线%,总市值亿,类型,竞价阈值%,昨成交额亿(估),竞价达标线亿,起涨日,已涨幅%,剩余空间%,可实现%,3日涨幅%,异动提示,庄股特征,独立走势,周五出货分时,备注']
    .concat([...A, ...B].map(r => [
      r.board, r.code, r.name, r.refDate, r.price, r.chgPct, r.rs20, r.rs10,
      r.pass580 ? '多头' : '未多头', r.bias, r.pullDist, r.totalCapYi, r.capClass, r.thr,
      r.prevDayAmtYi, r.aucLineYi,
      r.sp ? r.sp.startDate : '-', r.sp ? r.sp.gain.toFixed(1) : '-', r.sp ? r.sp.remaining.toFixed(1) : '-',
      r.sp ? r.sp.feasible.toFixed(1) : '-', r.sp && r.sp.threeDay != null ? r.sp.threeDay.toFixed(1) : '-',
      r.spAlert.replace(/,/g, '；'),
      r.zhuang || '-', r.independent, r.distFlag,
      (r.price != null && r.price < 3 ? '低价提示' : '') + (r.buyGrade.startsWith('回踩') ? ' 回踩买点' : '')
    ].join(',')))
    .join('\n');
  fs.writeFileSync(csvPath, '\ufeff' + csv, 'utf8');

  // 保存给 09:25 竞价检查用的清单：只存 A+B 合格候选（2026-08-25 修复：不再把庄股/出货存疑的排除票交给检查）
  const checkPath = path.join(CACHE_DIR, 'premarket_candidates.json');
  const checkPathDated = path.join(CACHE_DIR, `premarket_candidates_${today}.json`);
  fs.mkdirSync(CACHE_DIR, { recursive: true });
  const checkItems = [...A, ...B];
  const checkJson = JSON.stringify({ date: today, generated: new Date().toISOString(), items: checkItems.map(r => ({
    code: r.code, name: r.name, mkt: String(r.code[0] === '6' ? '1' : '0'),
    board: r.board, rs20: r.rs20, buyGrade: r.buyGrade,
    thr: r.thr, prevDayAmt: r.prevDayAmtYi * 1e8, capClass: r.capClass, price: r.price,
    distFlag: r.distFlag, // 前几日分时结构（竞价买入只参考前几日，不看当日）
    spGain: r.sp ? r.sp.gain : null, spRemain: r.sp ? r.sp.remaining : null, spStart: r.sp ? r.sp.startDate : null,
    spAlert: r.spAlert,
  })) }, null, 1);
  fs.writeFileSync(checkPath, checkJson);
  fs.writeFileSync(checkPathDated, checkJson);
  console.log(`\n已保存: ${csvPath}`);
  console.log(`已保存竞价检查清单: ${checkPath}`);
  console.log(`耗时 ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  process.exit(0);
}

function gradeRank(g) {
  if (g.startsWith('回踩A')) return 0;
  if (g.startsWith('回踩B')) return 1;
  if (g.startsWith('多头')) return 2;
  return 3;
}

function printPreTable(rows) {
  if (!rows.length) { console.log('  （无）'); return; }
  const head = ['板块', '代码', '名称', '昨收', '昨涨%', 'RS20%', '5/80', '乖离%', '距80%', '买点', '已涨幅%', '剩余%', '异动提示', '市值亿', '类型', '阈值%', '昨额亿≈', '达标线亿', '周五出货'];
  const w = head.map(h => Math.max(h.length, ...rows.map(r => {
    const c = {
      '板块': r.board, '代码': r.code, '名称': r.name, '昨收': fmt(r.price, 2), '昨涨%': fmt(r.chgPct),
      'RS20%': fmt(r.rs20), '5/80': r.pass580 ? '✓' : '✗', '乖离%': fmt(r.bias), '距80%': fmt(r.pullDist),
      '买点': r.buyGrade, '已涨幅%': r.sp ? fmt(r.sp.gain, 0) : '-', '剩余%': r.sp ? fmt(r.sp.remaining, 0) : '-',
      '异动提示': r.spAlert, '市值亿': fmt(r.totalCapYi), '类型': r.capClass, '阈值%': fmt(r.thr, 1),
      '昨额亿≈': fmt(r.prevDayAmtYi), '达标线亿': fmt(r.aucLineYi),
      '周五出货': r.distFlag,
    }[h];
    return String(c == null ? '' : c).length;
  })));
  const line = h => h.map((x, i) => String(x).padEnd(w[i] + 1)).join('');
  console.log(line(head));
  console.log(head.map((_, i) => '-'.repeat(w[i] + 1)).join(''));
  for (const r of rows) {
    console.log(line(['板块', '代码', '名称', '昨收', '昨涨%', 'RS20%', '5/80', '乖离%', '距80%', '买点', '已涨幅%', '剩余%', '异动提示', '市值亿', '类型', '阈值%', '昨额亿≈', '达标线亿', '周五出货'].map(h => {
      return {
        '板块': r.board, '代码': r.code, '名称': r.name, '昨收': fmt(r.price, 2), '昨涨%': fmt(r.chgPct),
        'RS20%': fmt(r.rs20), '5/80': r.pass580 ? '✓' : '✗', '乖离%': fmt(r.bias), '距80%': fmt(r.pullDist),
        '买点': r.buyGrade, '已涨幅%': r.sp ? fmt(r.sp.gain, 0) : '-', '剩余%': r.sp ? fmt(r.sp.remaining, 0) : '-',
        '异动提示': r.spAlert, '市值亿': fmt(r.totalCapYi), '类型': r.capClass, '阈值%': fmt(r.thr, 1),
        '昨额亿≈': fmt(r.prevDayAmtYi), '达标线亿': fmt(r.aucLineYi),
        '周五出货': r.distFlag,
      }[h];
    })));
  }
}

// 09:25 竞价检查：只检查竞价前候选清单（低开 + 竞价抢筹占比 + 今日5/80）
async function runAuctionCheck() {
  const t0 = Date.now();
  const listPath = path.join(CACHE_DIR, 'premarket_candidates.json');
  if (!fs.existsSync(listPath)) {
    console.log('未找到竞价前候选清单，请先运行 --premarket');
    process.exit(1);
  }
  const items = (JSON.parse(fs.readFileSync(listPath, 'utf8')).items || [])
    .map(i => ({ ...i, spAlert: withoutRetired3DayAlert(i.spAlert) }));
  console.log(`竞价检查：${items.length} 只（建议 09:25–09:30 运行）`);
  const now = new Date();
  const sessionReady = now.getHours() > 9 || (now.getHours() === 9 && now.getMinutes() >= 15);
  const todayStr8 = todayStr().replace(/-/g, '');

  // 腾讯批量行情：低开（今开/昨收）
  const syms = items.map(i => (String(i.mkt) === '1' ? 'sh' : 'sz') + i.code).join(',');
  let quotes = {};
  try {
    const txt = await fetchBuf('https://qt.gtimg.cn/q=' + syms, 'gbk');
    for (const line of txt.split(';')) {
      const m = line.match(/v_(\w+)="(.*)"/);
      if (!m) continue;
      const f = m[2].split('~');
      if (f.length > 37) quotes[m[1].slice(2)] = { open: parseFloat(f[5]), preclose: parseFloat(f[4]), name: f[1], amtWan: parseFloat(f[37]), chg: parseFloat(f[32]) };
    }
  } catch (e) { console.log('  腾讯行情取数失败:', String(e).slice(0, 80)); }

  // 板块强弱代理：候选池内同板块平均涨跌 + 板块内接近涨停的票（冲板氛围）
  const boardPctAll = {}, boardAvg = {}, boardHasLimit = {};
  for (const it of items) {
    const qq = quotes[it.code];
    if (!qq || qq.chg == null || !isFinite(qq.chg)) continue;
    (boardPctAll[it.board] = boardPctAll[it.board] || []).push(qq.chg);
  }
  for (const [b, cs] of Object.entries(boardPctAll)) boardAvg[b] = cs.reduce((a, c) => a + c, 0) / cs.length;
  for (const it of items) {
    const qq = quotes[it.code];
    if (qq && qq.chg != null && qq.chg >= (limitPctOf(it.code) > 1.1 ? 19.5 : 9.8)) boardHasLimit[it.board] = true;
  }

  const mktOf = i => String(i.mkt != null ? i.mkt : (i.code[0] === '6' ? '1' : '0'));
  const now0 = new Date();
  const inWin = now0.getHours() === 9 && now0.getMinutes() >= 25 && now0.getMinutes() <= 30;
  // 竞价窗口内直接用腾讯批量行情竞价额，无需拉分时（沪市分时接口偶发501）
  const minRes = inWin ? items.map(() => ({ m: [] })) : await poolRetry(items, i => minuteCached(symOf(mktOf(i), i.code)).then(m => ({ m })), CFG.CONCURRENCY);
  const m15Res = await poolRetry(items, i => m15KlineCached(symOf(mktOf(i), i.code), 320).then(bars => ({ bars })), CFG.CONCURRENCY);
  const errs = minRes.map((r, i) => r && r.error ? `${items[i].code}: ${r.error}` : null).filter(Boolean);
  if (errs.length) console.log('  [debug] 分时取数失败样本:', errs.slice(0, 8).join(' | '));

  const rows = [];
  items.forEach((i, idx) => {
    const q = quotes[i.code] || {};
    const mn = minRes[idx];
    const mk = m15Res[idx];
    const minutes = mn && !mn.error ? mn.m : [];
    // 优先用分时首笔（东财09:30定格竞价额，最准）；腾讯批量行情仅作兜底
    const now = new Date();
    const inAuctionWindow = now.getHours() === 9 && now.getMinutes() >= 25 && now.getMinutes() <= 30;
    const auctionAmt = minutes.length ? minutes[0].amount : (inAuctionWindow && q.amtWan ? q.amtWan * 1e4 : null);
    // 数据新鲜度校验：竞价未开始或分时数据仍是上一交易日 → 视为未就绪
    const minTime = minutes.length ? String(minutes[0].time) : '';
    const fresh = sessionReady && (!minTime || (minTime.includes('-') ? minTime.startsWith(todayStr()) : true));
    const aucAmtFresh = fresh ? auctionAmt : null;
    const prevAmt = i.prevDayAmt || 0;
    const ratio = aucAmtFresh && prevAmt ? aucAmtFresh / prevAmt * 100 : null;
    const gap = q.open && q.preclose ? (q.open / q.preclose - 1) * 100 : null;
    const gapFresh = sessionReady ? gap : null;
    const thr = i.thr || 5;
    const cbs = chongbanScore({
      gap: gapFresh, ratio, thr,
      boardPct: boardAvg[i.board], boardHasLimit: !!boardHasLimit[i.board],
      distLimitPct: null, limitHit: false,
    });
    const cbLabel = chongbanLabel(cbs);

    const m15 = mk && !mk.error ? mk.bars : [];
    let buyGrade = '—';
    if (m15.length >= 80) {
      const closes = m15.map(x => x.close);
      const ma5 = closes.slice(-5).reduce((a, b) => a + b, 0) / 5;
      const ma80 = closes.slice(-80).reduce((a, b) => a + b, 0) / 80;
      const bias = (ma5 / ma80 - 1) * 100;
      if (ma5 <= ma80) buyGrade = '未多头';
      else if (bias <= 2.0) buyGrade = '回踩A(最佳)';
      else if (bias <= 4.0) buyGrade = '回踩B';
      else buyGrade = '多头(未回踩)';
    }

    let sig = 'WAIT';
    if (gapFresh == null || ratio == null) sig = '数据未就绪(09:25后跑)';
    else if (gapFresh > 2) sig = 'HIGHOPEN 高开不追';
    else if (gapFresh <= downPctOf(i.code)) sig = 'SELLER 跌停开盘·不接飞刀';
    else if (gapFresh <= -3 && ratio >= thr) sig = 'SELLER 放量低开·卖方主导(核竞价明细)';
    else if (ratio >= thr && cbs >= 3) sig = 'TRIGGER 低开+抢筹+冲板';
    else if (ratio >= thr && cbs === 2) sig = 'NEAR 抢筹达标·冲板待核';
    else if (ratio >= thr * 0.6 && cbs >= 3) sig = 'NEAR 低开+抢筹接近';
    else sig = 'WAIT';

    rows.push({
      board: i.board, code: i.code, name: q.name || i.name,
      gap: gapFresh, ratio, thr, aucLineYi: thr / 100 * prevAmt / 1e8,
      buyGrade, rs20: i.rs20, sig, cb: cbLabel, distFlag: i.distFlag || '', spAlert: i.spAlert || '',
    });
  });

  rows.sort((a, b) => {
    const rk = { 'TRIGGER': 0, 'NEAR': 1, 'SELLER': 2, '数据未就绪(09:25后跑)': 3, 'WAIT': 4, 'HIGHOPEN': 5 };
    const rank = s => rk[s.split(' ')[0]] ?? 9; // 信号串带后缀（如 "TRIGGER 低开+抢筹+冲板"），取首词排名
    return (rank(a.sig) - rank(b.sig)) || (b.rs20 - a.rs20);
  });
  const grpOf = r => isNukeRisk(r.spAlert) ? '核按钮风险' : '正常';
  rows.sort((a, b) => (grpOf(a) !== grpOf(b) ? (grpOf(a) === '正常' ? -1 : 1) : 0));

  const head = ['板块', '代码', '名称', '开盘%', '竞价占比%', '冲板趋势', '阈值%', '达标线亿', '5/80', '前日出货', 'RS20%', '异动提示', '信号'];
  const cell = r => ({
    '板块': r.board, '代码': r.code, '名称': r.name, '开盘%': fmt(r.gap), '竞价占比%': fmt(r.ratio), '冲板趋势': r.cb,
    '阈值%': fmt(r.thr, 1), '达标线亿': fmt(r.aucLineYi), '5/80': r.buyGrade, '前日出货': r.distFlag || '-', 'RS20%': fmt(r.rs20),
    '异动提示': r.spAlert || '-', '信号': r.sig,
  });
  const show = (group, title) => {
    if (!group.length) { console.log(`\n【${title}】  （无）`); return; }
    const w = head.map(h => Math.max(h.length, ...group.map(r => String(cell(r)[h]).length)));
    const line = h => h.map((x, i) => String(x).padEnd(w[i] + 1)).join('');
    console.log(`\n【${title}】 ${group.length} 只`);
    console.log(line(head));
    console.log(head.map((_, i) => '-'.repeat(w[i] + 1)).join(''));
    for (const r of group) console.log(line(head.map(h => cell(r)[h])));
  };
  show(rows.filter(r => grpOf(r) === '正常'), '竞价检查·正常');
  show(rows.filter(r => grpOf(r) === '核按钮风险'), '竞价检查·核按钮风险·不接飞刀(需人工核对公告)');

  const today = todayStr().replace(/-/g, '');
  const csvPath = path.join(__dirname, `竞价检查_${today}.csv`);
  const csv = ['分组,板块,代码,名称,开盘%,竞价占比%,冲板趋势,阈值%,达标线亿,今日5/80,RS20%,异动提示,信号']
    .concat(rows.map(r => [grpOf(r), r.board, r.code, r.name, fmt(r.gap), fmt(r.ratio), r.cb, r.thr, r.aucLineYi, r.buyGrade, r.rs20,
      (r.spAlert || '-').replace(/,/g, '；'), r.sig].join(',')))
    .join('\n');
  fs.writeFileSync(csvPath, '\ufeff' + csv, 'utf8');
  console.log(`\n已保存: ${csvPath}  耗时 ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  process.exit(0);
}

// 分时出货结构（腾讯分时行：time/close/volume/amount）
function distAfter(minutes) {
  if (minutes.length < 60) return '待确认';
  const last = minutes[minutes.length - 1];
  const rng = (last.high != null ? last.high : last.close) - (last.low != null ? last.low : last.close);
  const closePos = rng > 0 ? (last.close - (last.low != null ? last.low : last.close)) / rng : 1;
  const half = Math.floor(minutes.length / 2);
  const avg1 = minutes.slice(0, half).reduce((a, x) => a + x.close, 0) / half;
  const avg2 = minutes.slice(half).reduce((a, x) => a + x.close, 0) / (minutes.length - half);
  const fl = [];
  if (closePos < 0.3) fl.push('收在低位');
  if (avg2 < avg1 && last.close < (last.open != null ? last.open : last.close)) fl.push('盘中走弱');
  const hi = Math.max(...minutes.map(x => x.high != null ? x.high : x.close));
  const hiIdx = minutes.findIndex(x => (x.high != null ? x.high : x.close) === hi);
  if (hiIdx >= 0 && hiIdx < minutes.length * 0.5 && last.close < hi * 0.97) fl.push('冲高回落');
  return fl.length ? '疑似出货:' + fl.join(',') : '无明显出货';
}

// 核按钮风险提示：命中任一即单独列为"核按钮·不接飞刀"
// 超10日/创业板口径等仅作提示，不列入核按钮风险组
function withoutRetired3DayAlert(a) {
  return String(a || '').split(' | ').filter(x => x !== '3日接近异动红线·需卡异动').join(' | ');
}

function isNukeRisk(a) {
  if (!a) return false;
  return ['已到10日200%停牌区', '已触发10日100%', '剩余空间<10%'].some(k => a.includes(k));
}

// 09:30+ 竞价后确认：只对 --check 口径挑出的低开+抢筹达标票做盘中确认
// 数据源：腾讯批量行情（低开/现价/涨跌）+ 东财优先分时（竞价额/均价/出货，沪市空数据时 PowerShell 通道重试）+ 15分钟K（5/80），腾讯分时兜底
async function runAuctionAfter() {
  const t0 = Date.now();
  const listPath = path.join(CACHE_DIR, 'premarket_candidates.json');
  if (!fs.existsSync(listPath)) {
    console.log('未找到竞价前候选清单，请先运行 --premarket');
    process.exit(1);
  }
  const items = (JSON.parse(fs.readFileSync(listPath, 'utf8')).items || [])
    .map(i => ({ ...i, spAlert: withoutRetired3DayAlert(i.spAlert) }));
  const now = new Date();
  if (now.getHours() < 9 || (now.getHours() === 9 && now.getMinutes() < 30)) {
    console.log('  [提示] 当前在竞价/刚开盘阶段，建议 09:30 后运行（分时首笔才定格竞价额）');
  }
  const today8 = todayStr().replace(/-/g, '');
  const mktOf = i => String(i.mkt != null ? i.mkt : (i.code[0] === '6' ? '1' : '0'));

  // 1) 腾讯批量行情：现价/今开/昨收/涨跌/时间戳
  const quotes = {};
  try {
    const syms = items.map(i => symOf(mktOf(i), i.code)).join(',');
    const txt = await fetchBuf('https://qt.gtimg.cn/q=' + syms, 'gbk');
    for (const line of txt.split(';')) {
      const m = line.match(/v_(\w+)="(.*)"/);
      if (!m) continue;
      const f = m[2].split('~');
      if (f.length > 40) quotes[m[1].slice(2)] = {
        name: f[1], price: parseFloat(f[3]), preclose: parseFloat(f[4]), open: parseFloat(f[5]),
        ts: f[30], chg: parseFloat(f[32]), high: parseFloat(f[33]), low: parseFloat(f[34]),
        limit: parseFloat(f[4]) * limitPctOf(m[1].slice(2)),
      };
    }
  } catch (e) { console.log('  腾讯行情取数失败:', String(e).slice(0, 80)); }
  const fresh = q => q && q.ts && String(q.ts).slice(0, 8) === today8;

  // 板块强弱代理（同 --check）：候选池内同板块平均涨跌 + 板块内接近涨停氛围
  const boardPctAll = {}, boardAvg = {}, boardHasLimit = {};
  for (const it of items) {
    const qq = quotes[it.code];
    if (!qq || !fresh(qq) || qq.chg == null || !isFinite(qq.chg)) continue;
    (boardPctAll[it.board] = boardPctAll[it.board] || []).push(qq.chg);
  }
  for (const [b, cs] of Object.entries(boardPctAll)) boardAvg[b] = cs.reduce((a, c) => a + c, 0) / cs.length;
  for (const it of items) {
    const qq = quotes[it.code];
    if (qq && qq.chg != null && qq.chg >= (limitPctOf(it.code) > 1.1 ? 19.5 : 9.8)) boardHasLimit[it.board] = true;
  }

  // 2) 分时：首笔 = 竞价额；末笔累计额/量 = 均价；全程判断出货分时（同 --check 口径，东财优先、腾讯兜底）
  const minRes = await poolRetry(items, i => minuteCached(symOf(mktOf(i), i.code)).then(m => ({ m })), CFG.CONCURRENCY);

  // 3) 竞价筛选（同 --check 口径）：开盘≤+2% + 冲板趋势 + 占比达标
  const aucRows = [];
  items.forEach((i, idx) => {
    const q = quotes[i.code] || {};
    const mn = minRes[idx];
    const minutes = mn && !mn.error ? mn.m : [];
    const aucAmt = minutes.length ? minutes[0].amount : null;
    const ratio = aucAmt && i.prevDayAmt ? aucAmt / i.prevDayAmt * 100 : null;
    const gap = q.open && q.preclose ? (q.open / q.preclose - 1) * 100 : null;
    const thr = i.thr || 5;
    const cbs = chongbanScore({
      gap, ratio, thr,
      boardPct: boardAvg[i.board], boardHasLimit: !!boardHasLimit[i.board],
      distLimitPct: q.price && q.limit && q.preclose ? (q.limit - q.price) / q.preclose * 100 : null,
      limitHit: q.chg != null && q.chg >= (limitPctOf(i.code) > 1.1 ? 19.5 : 9.8),
    });
    let sig = 'WAIT';
    if (!fresh(q) || gap == null || ratio == null) sig = '数据未就绪';
    else if (gap > 2) sig = 'HIGHOPEN';
    else if (gap <= downPctOf(i.code)) sig = 'SELLER 跌停开盘·不接飞刀';
    else if (q.chg != null && q.chg <= downPctOf(i.code)) sig = 'SELLER 现价跌停·不接飞刀';
    else if (gap <= -3 && ratio >= thr) sig = 'SELLER 放量低开·卖方主导';
    else if (ratio >= thr && cbs >= 3) sig = 'TRIGGER';
    else if (ratio >= thr && cbs === 2) sig = 'NEAR';
    else if (ratio >= thr * 0.6 && cbs >= 3) sig = 'NEAR';
    else sig = 'WAIT';
    aucRows.push({ i, q, minutes, gap, ratio, thr, sig, cb: chongbanLabel(cbs) });
  });
  const qual = aucRows.filter(r => r.sig === 'TRIGGER' || r.sig === 'NEAR');
  console.log(`竞价后确认：候选 ${items.length} 只，低开+抢筹达标 ${qual.length} 只（TRIGGER/NEAR），只对这些做盘中确认...`);
  if (!qual.length) {
    console.log('  无竞价达标个股，跳过盘中确认');
    fs.writeFileSync(path.join(__dirname, `竞价后确认_${today8}.csv`), '\ufeff板块,代码,名称,低开%,竞价占比%,阈值%,信号\n', 'utf8');
    process.exit(0);
  }

  // 4) 只对达标票拉 15 分钟 5/80（缓存优先，盘前已拉过基本免请求）
  const m15Res = await poolRetry(qual.map(r => r.i), s => m15KlineCached(symOf(mktOf(s), s.code), 320).then(bars => ({ bars })), CFG.CONCURRENCY);

  const out = [];
  qual.forEach((r, idx) => {
    const i = r.i, q = r.q, minutes = r.minutes;
    const mk = m15Res[idx];
    const m15 = mk && !mk.error ? mk.bars : [];
    let buyGrade = '—', pullDist = null;
    if (m15.length >= 80) {
      const closes = m15.map(x => x.close);
      const ma5 = closes.slice(-5).reduce((a, b) => a + b, 0) / 5;
      const ma80 = closes.slice(-80).reduce((a, b) => a + b, 0) / 80;
      const bias = (ma5 / ma80 - 1) * 100;
      const last3 = m15.slice(-3);
      pullDist = (Math.min(...last3.map(x => x.low)) / ma80 - 1) * 100;
      if (ma5 <= ma80) buyGrade = '未多头';
      else if (bias <= 2.0) buyGrade = '回踩A(最佳)';
      else if (bias <= 4.0) buyGrade = '回踩B';
      else buyGrade = '多头(未回踩)';
    }
    let avgPrice = null;
    if (minutes.length) {
      const last = minutes[minutes.length - 1];
      if (last.amount && last.volume) avgPrice = last.amount / (last.volume * 100);
    }
    const dFlag = distAfter(minutes);
    const upOpen = q.price > q.open;
    const hong = q.chg > 0;
    const aboveAvg = avgPrice != null && q.price >= avgPrice;
    const noDist = !dFlag.startsWith('疑似');
    const boardUp = (boardAvg[i.board] || 0) > 0;

    let cls = '等待确认';
    if (q.chg != null && q.chg <= downPctOf(i.code)) cls = '跌停·不接飞刀';
    else if (q.chg >= 9.5) cls = '涨停不追';
    else if (r.sig === 'TRIGGER') {
      if (upOpen && hong && buyGrade.startsWith('回踩') && noDist && boardUp) cls = 'P1 低吸买点';
      else if (upOpen && hong && noDist && boardUp) cls = 'P2 低开高走';
      else if (upOpen && noDist && boardUp) cls = 'P3 低开修复中';
    } else {
      cls = 'NEAR 接近未达标(观察)';
      if (upOpen && hong && buyGrade.startsWith('回踩') && noDist && boardUp) cls = 'NEAR→P1 接近达标且确认';
    }
    const grp = isNukeRisk(i.spAlert) ? '核按钮风险' : '正常';
    if (grp === '核按钮风险') cls = '核按钮·不接飞刀';
    out.push({
      board: i.board, code: i.code, name: q.name || i.name,
      gap: r.gap, ratio: r.ratio, thr: r.thr, cb: r.cb,
      price: q.price, chg: q.chg, upOpen: upOpen ? '是' : '否', hong: hong ? '红' : '绿',
      aboveAvg: aboveAvg ? '是' : '否', buyGrade, pullDist, dFlag,
      boardPct: boardAvg[i.board], rs20: i.rs20, sig: r.sig, cls, grp, spAlert: i.spAlert || '',
    });
  });

  const order = { 'P1': 0, 'P2': 1, 'P3': 2, 'NEAR→P1': 3, '涨停不追': 4, 'NEAR': 5, '等待确认': 6 };
  out.sort((a, b) => (a.grp !== b.grp ? (a.grp === '正常' ? -1 : 1)
    : (order[a.cls.split(' ')[0]] - order[b.cls.split(' ')[0]])) || (b.rs20 - a.rs20));

  const head = ['板块', '代码', '名称', '开盘%', '竞价%', '冲板趋势', '阈值%', '现价', '今涨%', '高走', '红/绿', '站均价', '5/80', '距80%', '出货分时', '板块均涨%', 'RS20', '异动提示', '信号', '评级'];
  const cell = r => ({
    '板块': r.board, '代码': r.code, '名称': r.name, '开盘%': fmt(r.gap), '竞价%': fmt(r.ratio), '冲板趋势': r.cb,
    '阈值%': fmt(r.thr, 1), '现价': fmt(r.price), '今涨%': fmt(r.chg), '高走': r.upOpen,
    '红/绿': r.hong, '站均价': r.aboveAvg, '5/80': r.buyGrade, '距80%': fmt(r.pullDist),
    '出货分时': r.dFlag, '板块均涨%': fmt(r.boardPct), 'RS20': fmt(r.rs20),
    '异动提示': r.spAlert || '-', '信号': r.sig, '评级': r.cls,
  });
  const show = (group, title) => {
    if (!group.length) { console.log(`\n【${title}】  （无）`); return; }
    const w = head.map(h => Math.max(h.length, ...group.map(r => String(cell(r)[h]).length)));
    const line = h => h.map((x, i) => String(x).padEnd(w[i] + 1)).join('');
    console.log(`\n【${title}】 ${group.length} 只`);
    console.log(line(head));
    console.log(head.map((_, i) => '-'.repeat(w[i] + 1)).join(''));
    for (const r of group) console.log(line(head.map(h => cell(r)[h])));
  };
  console.log('\n================ 竞价后确认（09:30+，仅低开+抢筹达标票） ================');
  show(out.filter(r => r.grp === '正常'), '达标·正常');
  show(out.filter(r => r.grp === '核按钮风险'), '达标·核按钮风险（不接飞刀·需人工核对公告）');

  const csvPath = path.join(__dirname, `竞价后确认_${today8}.csv`);
  const csv = ['分组,板块,代码,名称,开盘%,竞价占比%,冲板趋势,阈值%,现价,今涨%,低开高走,翻红,站均价,5/80,距80线%,出货分时,板块均涨%,RS20%,异动提示,信号,评级']
    .concat(out.map(r => [r.grp, r.board, r.code, r.name, fmt(r.gap), fmt(r.ratio), r.cb, r.thr, fmt(r.price), fmt(r.chg),
      r.upOpen, r.hong, r.aboveAvg, r.buyGrade, fmt(r.pullDist), r.dFlag, fmt(r.boardPct), fmt(r.rs20),
      (r.spAlert || '-').replace(/,/g, '；'), r.sig, r.cls].join(',')))
    .join('\n');
  fs.writeFileSync(csvPath, '\ufeff' + csv, 'utf8');
  console.log(`\n已保存: ${csvPath}  耗时 ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  process.exit(0);
}

async function main() {
  const t0 = Date.now();
  if (CHECK) {
    await runAuctionCheck();
    return;
  }
  if (AFTER) {
    await runAuctionAfter();
    return;
  }
  console.log('Step 1/5 拉取全市场股票...');
  let stocks = null;
  const uniCache = cacheGet('universe', 20 * 60 * 1000);
  if (uniCache && uniCache.length >= 3000) {
    stocks = uniCache;
    console.log('  命中缓存');
  } else {
    try {
      stocks = await fetchTencentUniverse();
      console.log('  数据源：腾讯');
    } catch (eT) {
      console.log(`  腾讯不可用（${String(eT).slice(0, 60)}），切换东方财富...`);
      try {
        stocks = await fetchUniverse();
        console.log('  数据源：东方财富');
      } catch (eE) {
        console.log('  东方财富不可用，切换新浪兜底...');
        stocks = await fetchSinaUniverse();
        console.log('  数据源：新浪');
      }
    }
    if (!stocks || !stocks.length) throw new Error('全市场列表为空');
    // 只缓存完整列表，避免新浪兜底（约 2290 只）污染后续 20 分钟
    if (stocks.length >= 3000) cacheSet('universe', stocks);
  }
  console.log(`  全市场 ${stocks.length} 只，按行业分组...`);

  const boards = new Map();
  for (const s of stocks) {
    if (!boards.has(s.industry)) boards.set(s.industry, []);
    boards.get(s.industry).push(s);
  }

  console.log('Step 2/5 估算板块日线（每板块取成交额前 %d 只成分股）...', CFG.SAMPLE);
  const boardNames = [...boards.keys()].sort();
  const sampling = [];
  for (const name of boardNames) {
    const list = boards.get(name).sort((a, b) => (b.amount || 0) - (a.amount || 0)).slice(0, CFG.SAMPLE + 10);
    sampling.push({ name, list });
  }
  const dayRes = await poolRetry(
    sampling.flatMap(g => g.list.map(s => s)),
    s => dayKlineCached(symOf(s.mkt, s.code)),
    CFG.CONCURRENCY
  );
  // 建立 code -> daySeries
  const uniFlat = sampling.flatMap(g => g.list.map(s => s.code));
  const dayByCode = new Map();
  uniFlat.forEach((code, idx) => {
    const r = dayRes[idx];
    if (r && !r.error && r.length) dayByCode.set(code, { stock: sampling.flatMap(g => g.list).find(s => s.code === code), bars: r, series: pctSeries(r) });
  });

  const boardSeriesMap = new Map();
  for (const g of sampling) {
    // 用上一交易日成交额（量×收盘）排序采样，避免盘中按今日活跃度污染板块序列
    const ranked = g.list.map(s => dayByCode.get(s.code)).filter(Boolean)
      .sort((a, b) => lastTurnover(b) - lastTurnover(a))
      .slice(0, CFG.SAMPLE);
    boardSeriesMap.set(g.name, aggregateBoard(ranked.map(x => x.series)));
  }

  console.log('Step 3/5 筛选板块（上升趋势 + 前两日一阶段分歧）...');
  const passBoards = [];
  const boardInfo = [];
  for (const name of boardNames) {
    const rows = boardSeriesMap.get(name) || [];
    const res = boardPass(rows);
    const todayPct = rows.length ? rows[rows.length - 1].pct : null;
    boardInfo.push({ name, rows, todayPct, res });
    if (res) passBoards.push(name);
  }
  console.log(`  通过板块 ${passBoards.length} 个:`);
  for (const name of passBoards) {
    const b = boardInfo.find(x => x.name === name);
    console.log(`    ${name}  分歧日=${b.res.div.date} 跌幅=${(b.res.div.pct * 100).toFixed(2)}%${b.res.div.deep ? ' [深度]' : ''} 今日板块≈${((b.todayPct || 0) * 100).toFixed(2)}%`);
  }
  if (!passBoards.length) {
    console.log('  严格条件无通过板块，尝试 --relax 或查看日志。');
    if (process.argv.includes('--verbose')) {
      const stats = boardInfo.map(b => {
        const rows = b.rows.length > 1 && b.rows[b.rows.length - 1].date === todayStr() ? b.rows.slice(0, -1) : b.rows;
        const closes = rows.map(r => r.close);
        const i = closes.length - 1;
        const ma5 = maOf(closes, 5, i), ma10 = maOf(closes, 10, i), ma20 = maOf(closes, 20, i);
        const ma20b = maOf(closes, 20, i - 3);
        const up = ma5 != null && ma20 != null && ma20b != null && closes[i] > ma20 && ma20 > ma20b && ma5 > ma10;
        const last10 = rows.slice(-10).map(r => r.pct);
        const minD = Math.min(...last10);
        return { name: b.name, up, minD: minD * 100, n: rows.length };
      }).filter(x => x.n >= 40);
      stats.sort((a, b) => (b.up - a.up) || (b.minD - a.minD));
      console.log('  [verbose] 前 20 板块: 上升趋势 / 近10日最大单日跌幅%');
      for (const s of stats.slice(0, 20)) console.log(`    ${s.name}  uptrend=${s.up}  minDiv10=${s.minD.toFixed(2)}  n=${s.n}`);
    }
  }

  // 按分歧深度排序，保留最强的 30 个板块
  passBoards.sort((a, b) => {
    const da = boardInfo.find(x => x.name === a).res.div.pct;
    const db = boardInfo.find(x => x.name === b).res.div.pct;
    return da - db;
  });
  passBoards.length = Math.min(passBoards.length, CFG.MAX_BOARDS);
  console.log(`  按分歧深度保留前 ${passBoards.length} 个板块`);

  console.log('Step 4/5 拉取候选板块个股日线（RS/庄股特征）...');
  const candStocks = passBoards.flatMap(n => boards.get(n).map(s => ({ board: n, ...s })));
  const need = candStocks.filter(s => !dayByCode.has(s.code));
  const extraRes = await poolRetry(
    need.map(s => s),
    s => dayKlineCached(symOf(s.mkt, s.code)),
    CFG.CONCURRENCY
  );
  need.forEach((s, idx) => {
    const r = extraRes[idx];
    if (r && !r.error && r.length) dayByCode.set(s.code, { stock: s, bars: r, series: pctSeries(r) });
  });

  const out = [];
  let consistentCount = 0;
  for (const name of passBoards) {
    const boardRows = boardSeriesMap.get(name) || [];
    if (boardRows.length < 30) continue;
    const bDates = boardRows.map(r => r.date);
    const bpct = boardRows.map(r => r.pct);
    const rows20 = bpct.slice(-CFG.RS_WINDOW - 1);
    const bRet20 = retOver(rows20, CFG.RS_WINDOW) || 0;
    const rows10 = bpct.slice(-CFG.RS_WINDOW2 - 1);
    const bRet10 = retOver(rows10, CFG.RS_WINDOW2) || 0;

    const cands = boards.get(name).filter(s => dayByCode.has(s.code));
    for (const s of cands) {
      const d = dayByCode.get(s.code);
      const bars = d.bars;
      const ser = d.series;
      if (ser.length < 30) continue;
      const sRet20 = retOver(ser.map(x => x.pct).slice(-CFG.RS_WINDOW - 1), CFG.RS_WINDOW);
      const sRet10 = retOver(ser.map(x => x.pct).slice(-CFG.RS_WINDOW2 - 1), CFG.RS_WINDOW2);
      if (sRet20 == null) continue;
      const rs20 = (1 + sRet20) / (1 + bRet20) - 1;
      const rs10 = sRet10 != null && bRet10 != null ? (1 + sRet10) / (1 + bRet10) - 1 : null;
      if (rs20 <= 0) continue;

      // 庄股特征
      const last30 = ser.slice(-30);
      const boardPctMap = new Map(boardRows.map((r, i) => [r.date, r.pct]));
      const a = [], b = [];
      for (const p of last30) {
        const bp = boardPctMap.get(p.date);
        if (bp != null) { a.push(p.pct); b.push(bp); }
      }
      const rel = corr(a, b);
      const last20 = ser.slice(-20);
      let weaving = 0, ampSum = 0;
      for (let i = 0; i < last20.length; i++) {
        const prev = bars.find(x => x.date === last20[i].date);
        const prevClose = prev && prev.close ? prev.close : 0;
        if (!prevClose) continue;
        const amp = (prev.high - prev.low) / prevClose * 100;
        ampSum += amp;
        if (amp < 1) weaving++;
      }
      const weavingFlag = weaving >= 8 && ampSum / 20 < 1.5;
      const vols = bars.map(x => x.volume).sort((x, y) => x - y);
      const q75 = vols.length ? vols[Math.floor(vols.length * 0.75)] : 0;
      let ddc = 0;
      for (let i = 1; i < bars.length; i++) {
        if (bars[i].volume >= q75 && Math.abs(bars[i].close / bars[i - 1].close - 1) <= 0.005) ddc++;
      }
      const duiDao = ddc >= 3;
      const freeShares = s.floatCap && s.price ? s.floatCap / s.price : 0;
      let lock = false;
      if (freeShares > 0) {
        let tot = 0, n = 0;
        for (let i = Math.max(1, bars.length - 20); i < bars.length; i++) {
          tot += bars[i].volume * 100 / freeShares * 100; n++;
        }
        lock = n > 0 && tot / n < 0.5;
      }
      const zhuangFlags = [];
      if (weavingFlag) zhuangFlags.push('织布机');
      if (duiDao) zhuangFlags.push('对倒');
      if (lock) zhuangFlags.push('锁仓');
      // 昨日成交额（用最后一根完整日K的成交量×典型价(开高低收均值)近似）
      const full = bars[bars.length - 1].date === todayStr() ? bars[bars.length - 2] : bars[bars.length - 1];
      const prevDayAmt = full ? full.volume * 100 * ((full.open + full.high + full.low + full.close) / 4) : null;
      // 竞价前模式：展示价/涨跌用最后一根完整日K（周五），避免盘中实时价混入
      let dispPrice = s.price, dispChg = s.chgPct;
      if (PRE_MARKET && bars.length >= 2) {
        const li = bars[bars.length - 1].date === todayStr() ? bars.length - 2 : bars.length - 1;
        dispPrice = bars[li].close;
        if (li - 1 >= 0) dispChg = (bars[li].close / bars[li - 1].close - 1) * 100;
      }
      // 一致日涨停过滤：昨日个股涨停(一致)且板块未分歧 → 今日是分歧第一天，不进池
      // 只有"板块分歧日(≤-2%)逆势涨停的强票"才保留（次日开盘≤+2% 才符合策略时点）
      const li = bars[bars.length - 1].date === todayStr() ? bars.length - 2 : bars.length - 1;
      if (li >= 1) {
        const stockPct = (bars[li].close / bars[li - 1].close - 1) * 100;
        const limitTh2 = /^30/.test(s.code) ? 19.5 : 9.8;
        const brY = boardRows.find(r => r.date === bars[li].date) || boardRows[boardRows.length - 1];
        const boardPctY = brY ? brY.pct * 100 : 0;
        if (stockPct >= limitTh2 && boardPctY > -2.0) {
          consistentCount++;
          continue;
        }
      }

      out.push({
        board: name, code: s.code, name: s.name,
        mkt: s.mkt,
        price: dispPrice, chgPct: dispChg,
        lowOpen: s.open && s.prevClose ? (s.open / s.prevClose - 1) * 100 : null,
        floatCap: s.floatCap,
        floatCapYi: s.floatCap ? s.floatCap / 1e8 : null,
        totalCapYi: s.totalCap ? s.totalCap / 1e8 : null,
        prevDayAmt,
        capClass: capInfo(s).cls,
        auctionTh: capInfo(s).thr,
        rs20: rs20 * 100, rs10: rs10 != null ? rs10 * 100 : null,
        corr: rel, zhuangFlags: zhuangFlags.join('/'),
        dayBars: bars,
      });
    }
  }

  out.sort((x, y) => y.rs20 - x.rs20);
  if (consistentCount) console.log(`  [时点过滤] ${consistentCount} 只昨日一致涨停(板块未分歧)，今日为分歧第一天，剔除出池`);
  const top = out.slice(0, Math.min(passBoards.length * CFG.MAX_PER_BOARD, CFG.MAX_CANDIDATES));
  console.log(`  RS>0 个股 ${out.length} 只，取前 ${top.length} 只拉 15 分钟线/竞价...`);

  if (FETCHDAY) {
    const pendingPath = path.join(CACHE_DIR, 'pending_m15.json');
    fs.mkdirSync(CACHE_DIR, { recursive: true });
    fs.writeFileSync(pendingPath, JSON.stringify({ ts: Date.now(), items: top }));
    console.log(`  已写入待取列表 ${pendingPath}（${top.length} 只），接下来运行 fetch_phase_b.ps1`);
    return;
  }

  console.log('Step 5/5 计算竞价占比、开盘、5/80、买点...');
  let candidateList = top;
  if (OFFLINE) {
    try {
      const pj = JSON.parse(fs.readFileSync(path.join(CACHE_DIR, 'pending_m15.json'), 'utf8'));
      candidateList = pj.items || [];
      console.log(`  离线模式，使用待取列表 ${candidateList.length} 只`);
    } catch (e) { /* 无待取列表则用缓存全量计算 */ }
  }

  if (PRE_MARKET) {
    const mins = new Date().getHours() * 60 + new Date().getMinutes();
    if (mins >= 9 * 60 + 15) console.log('  [提示] 已过竞价时段：按上一交易日收盘口径计算，仅供复盘核对，以盘前 08:45 运行为准');
    await runPreMarket(candidateList);
    return;
  }
  const mktOf = s => String(s.mkt != null ? s.mkt : (s.code[0] === '6' ? '1' : '0'));
  const m15Res = await poolRetry(candidateList.map(s => s), s => m15KlineCached(symOf(mktOf(s), s.code), 320).then(bars => ({ bars })), CFG.CONCURRENCY);
  const minRes = await poolRetry(candidateList.map(s => s), s => minuteCached(symOf(mktOf(s), s.code)).then(m => ({ m })), CFG.CONCURRENCY);

  const results = [];
  candidateList.forEach((s, idx) => {
    const mk = m15Res[idx];
    const mn = minRes[idx];
    const m15 = mk && !mk.error ? mk.bars : [];
    const minutes = mn && !mn.error ? mn.m : [];
    const auctionAmt = minutes.length ? minutes[0].amount : null;
    const auctionPct = auctionAmt && s.floatCap ? auctionAmt / s.floatCap * 100 : null;
    const aucVsPrev = auctionAmt && s.prevDayAmt ? auctionAmt / s.prevDayAmt * 100 : null;
    const auctionTh = s.auctionTh || (s.capClass === '大容量' ? CFG.AUCTION_LARGE : CFG.AUCTION_MID);

    // 15分钟 5/80
    let ma5 = null, ma80 = null, pullDist = null, bias = null, buyGrade = '—';
    if (m15.length >= 80) {
      const closes = m15.map(x => x.close);
      ma5 = closes.slice(-5).reduce((a, b) => a + b, 0) / 5;
      ma80 = closes.slice(-80).reduce((a, b) => a + b, 0) / 80;
      const last3 = m15.slice(-3);
      const minLow = Math.min(...last3.map(x => x.low));
      pullDist = (minLow / ma80 - 1) * 100;
      bias = (ma5 / ma80 - 1) * 100;
      const bullish = ma5 > ma80;
      if (!bullish) buyGrade = '未多头';
      else if (bias <= 2.0) buyGrade = '回踩A(最佳)';       // 手册：5/80乖离收敛到0~2%
      else if (bias <= 4.0) buyGrade = '回踩B';
      else buyGrade = '多头(未回踩)';
    }

    // 出货分时（盘中数据不足则待确认）
    let distFlag = '盘中待确认';
    if (minutes.length >= 30) {
      const last = minutes[minutes.length - 1];
      const rng = last.high - last.low;
      const closePos = rng > 0 ? (last.close - last.low) / rng : 1;
      const half = Math.floor(minutes.length / 2);
      const avg1 = minutes.slice(0, half).reduce((a, x) => a + x.close, 0) / half;
      const avg2 = minutes.slice(half).reduce((a, x) => a + x.close, 0) / (minutes.length - half);
      const flags = [];
      if (closePos < 0.3) flags.push('收在低位');
      if (avg2 < avg1 && last.close < last.open) flags.push('盘中走弱');
      distFlag = flags.length ? '疑似出货:' + flags.join(',') : '无明显出货';
    }

    const zhuang = s.zhuangFlags || '';
    const independent = s.corr != null && s.corr < CFG.CORR_MIN;
    const noZhuang = !zhuang && !independent;

    results.push({
      board: s.board, code: s.code, name: s.name,
      price: s.price, chgPct: s.chgPct, lowOpen: s.lowOpen,
      auctionPct, auctionTh, capClass: s.capClass, floatCapYi: s.floatCapYi, totalCapYi: s.totalCapYi,
      aucVsPrev,
      rs20: s.rs20, rs10: s.rs10, corr: s.corr,
      ma5, ma80, pullDist, bias, buyGrade,
      zhuang, independent, noZhuang, distFlag,
      passLow: s.lowOpen != null && s.lowOpen <= CFG.OPEN_MAX,
      passAuc: (aucVsPrev != null && aucVsPrev >= auctionTh) || (aucVsPrev == null && auctionPct != null && auctionPct >= auctionTh),
      pass580: buyGrade !== '未多头' && buyGrade !== '—',
    });
  });

  // 竞价买入判定不看当日出货分时（当天未走出来）；当日出货结构只作盘中卖出提醒
  const strict = results.filter(r => r.passLow && r.passAuc && r.pass580 && r.noZhuang);
  const tierB = results.filter(r => !strict.includes(r) && r.passLow && r.noZhuang);
  strict.sort((a, b) => b.rs20 - a.rs20);
  tierB.sort((a, b) => (b.passAuc - a.passAuc) || (b.rs20 - a.rs20));

  console.log('\n================ 筛选结果 ================\n');
  console.log(`【A档】开盘≤+2% + 竞价达标 + 5/80多头 + 非庄股（当日出货分时仅作卖出提醒）  共 ${strict.length} 只`);
  printTable(strict);
  console.log(`\n【B档】开盘≤+2% + 非庄股（竞价或5/80未达标，观察）  共 ${tierB.length} 只`);
  printTable(tierB.slice(0, 30));

  const today = todayStr().replace(/-/g, '');
  const csv = ['板块,代码,名称,现价,今涨%,开盘%,竞价额/昨额%,竞价额/流通%,竞价阈值%,市值亿,类型,RS20%,RS10%,相关性,5/80,距80线%,买点,庄股特征,独立走势,出货分时,达标情况']
    .concat([...strict, ...tierB].map(r => [
      r.board, r.code, r.name, r.price, r.chgPct, r.lowOpen, r.aucVsPrev, r.auctionPct, r.auctionTh,
      r.floatCapYi, r.capClass, r.rs20, r.rs10, r.corr, r.pass580, r.pullDist, r.buyGrade,
      r.zhuang || '-', r.independent, r.distFlag,
      `${r.passLow ? '开盘✓' : '开盘✗'}${r.passAuc ? ' 竞价✓' : ' 竞价✗'}${r.pass580 ? ' 5/80✓' : ' 5/80✗'}${r.noZhuang ? ' 非庄✓' : ' 非庄✗'}`
    ].join(',')))
    .join('\n');
  const csvPath = path.join(__dirname, `今日候选_${today}.csv`);
  fs.writeFileSync(csvPath, '\ufeff' + csv, 'utf8');
  console.log(`\n已保存: ${csvPath}`);
  console.log(`耗时 ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  process.exit(0); // 主动退出并回收 PowerShell 子进程
}

function fmt(v, d = 2) { return v == null || !isFinite(v) ? '-' : v.toFixed(d); }
function fmt1(v, d = 1) { return v == null || !isFinite(v) ? '-' : v.toFixed(d); }

function printTable(rows) {
  if (!rows.length) { console.log('  （无）'); return; }
  const head = ['板块', '代码', '名称', '现价', '今涨%', '开盘%', '竞价/昨额%', '阈值', '市值亿', '类型', 'RS20%', 'RS10%', '5/80', '距80%', '买点', '庄股/独立', '出货分时'];
  const w = head.map(h => Math.max(h.length, ...rows.map(r => {
    const c = {
      '板块': r.board, '代码': r.code, '名称': r.name, '现价': fmt(r.price, 2), '今涨%': fmt(r.chgPct),
      '开盘%': fmt(r.lowOpen), '竞价/昨额%': fmt(r.aucVsPrev), '阈值': fmt(r.auctionTh, 1), '市值亿': fmt(r.floatCapYi),
      '类型': r.capClass, 'RS20%': fmt(r.rs20), 'RS10%': fmt(r.rs10), '5/80': r.pass580 ? '✓' : '✗',
      '距80%': fmt(r.pullDist), '买点': r.buyGrade, '庄股/独立': (r.zhuang || '-') + (r.independent ? '/独立' : ''),
      '出货分时': r.distFlag,
    }[h];
    return String(c == null ? '' : c).length;
  })));
  const line = h => h.map((x, i) => String(x).padEnd(w[i] + 1)).join('');
  console.log(line(head));
  console.log(head.map((_, i) => '-'.repeat(w[i] + 1)).join(''));
  for (const r of rows) {
    console.log(line(['板块', '代码', '名称', '现价', '今涨%', '开盘%', '竞价/昨额%', '阈值', '市值亿', '类型', 'RS20%', 'RS10%', '5/80', '距80%', '买点', '庄股/独立', '出货分时'].map(h => {
      return {
        '板块': r.board, '代码': r.code, '名称': r.name, '现价': fmt(r.price, 2), '今涨%': fmt(r.chgPct),
        '开盘%': fmt(r.lowOpen), '竞价/昨额%': fmt(r.aucVsPrev), '阈值': fmt(r.auctionTh, 1), '市值亿': fmt(r.floatCapYi),
        '类型': r.capClass, 'RS20%': fmt(r.rs20), 'RS10%': fmt(r.rs10), '5/80': r.pass580 ? '✓' : '✗',
        '距80%': fmt(r.pullDist), '买点': r.buyGrade, '庄股/独立': (r.zhuang || '-') + (r.independent ? '/独立' : ''),
        '出货分时': r.distFlag,
      }[h];
    })));
  }
}

main().catch(e => { console.error(e); process.exit(1); });
