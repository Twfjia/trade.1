// 每日竞价跳空高开汇总 + 公开资讯归因
// 默认：开盘涨幅 >= 2%，且开盘价 > 上一交易日最高价
// 运行：node auction_gapup.js
// 可选：--min-gap 1.5  --no-strict  --no-news  --self-test

'use strict';

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const PS_HELPER = path.join(__dirname, 'fetch_helper.ps1');
const UA = { 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://quote.eastmoney.com/' };
const DAY_MS = 24 * 3600 * 1000;
const NEWS_DAYS = 7;

function argValue(name, fallback) {
  const i = process.argv.indexOf(name);
  return i >= 0 && process.argv[i + 1] != null ? process.argv[i + 1] : fallback;
}

const MIN_GAP = Number(argValue('--min-gap', 2));
const STRICT_GAP = !process.argv.includes('--no-strict');
const NO_NEWS = process.argv.includes('--no-news');
const SELF_TEST = process.argv.includes('--self-test');

if (!Number.isFinite(MIN_GAP) || MIN_GAP < 0 || MIN_GAP > 30) {
  console.error('--min-gap 必须是 0~30 之间的数字');
  process.exit(2);
}

class PsFetcher {
  constructor() {
    this.buf = '';
    this.queue = [];
    this.proc = spawn('powershell.exe', [
      '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', PS_HELPER,
    ], { stdio: ['pipe', 'pipe', 'ignore'] });
    this.proc.stdout.setEncoding('utf8');
    this.proc.stdout.on('data', d => this.onData(d));
    this.proc.on('exit', () => {
      const err = new Error('PowerShell 取数通道退出');
      this.queue.forEach(q => q.reject(err));
      this.queue = [];
    });
  }

  onData(data) {
    this.buf += data;
    let i;
    while ((i = this.buf.indexOf('\n')) >= 0) {
      const line = this.buf.slice(0, i).replace(/\r$/, '');
      this.buf = this.buf.slice(i + 1);
      const q = this.queue.shift();
      if (!q) continue;
      if (line.startsWith('OK ')) q.resolve(Buffer.from(line.slice(3), 'base64'));
      else q.reject(new Error('PowerShell: ' + line.slice(0, 160)));
    }
  }

  fetch(url) {
    return new Promise((resolve, reject) => {
      this.queue.push({ resolve, reject });
      this.proc.stdin.write(url + '\n');
    });
  }
}

const psPool = Array.from({ length: 6 }, () => new PsFetcher());
let psIndex = 0;

function psFetch(url) {
  psIndex = (psIndex + 1) % psPool.length;
  return psPool[psIndex].fetch(url);
}

process.on('exit', () => {
  for (const p of psPool) {
    try { p.proc.kill(); } catch (_) { /* ignore */ }
  }
});

async function fetchText(url, charset = 'utf8') {
  try {
    const r = await fetch(url, { headers: UA, signal: AbortSignal.timeout(12000) });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return new TextDecoder(charset).decode(Buffer.from(await r.arrayBuffer()));
  } catch (_) {
    const buf = await psFetch(url);
    return new TextDecoder(charset).decode(buf);
  }
}

async function fetchJson(url, charset = 'utf8', tries = 3) {
  let last;
  for (let i = 0; i < tries; i++) {
    try {
      return JSON.parse(await fetchText(url, charset));
    } catch (e) {
      last = e;
      await new Promise(r => setTimeout(r, 500 * (i + 1)));
    }
  }
  throw last;
}

async function emJson(url, tries = 2) {
  const hosts = ['push2delay', '1.push2delay', '8.push2delay', '82.push2delay'];
  let last;
  for (let round = 0; round < tries; round++) {
    for (const host of hosts) {
      try {
        const candidate = url.replace('https://push2delay', `https://${host}`);
        return await fetchJson(candidate);
      } catch (e) {
        last = e;
      }
    }
  }
  throw last;
}

async function pool(items, worker, concurrency) {
  const out = new Array(items.length);
  let index = 0;
  async function run() {
    while (index < items.length) {
      const i = index++;
      try {
        out[i] = await worker(items[i], i);
      } catch (e) {
        out[i] = { error: String(e && e.message ? e.message : e).slice(0, 160) };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, run));
  return out;
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function date8(d = new Date()) {
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`;
}

function dateDash(d = new Date()) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function validPrice(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function pctOf(open, prevClose) {
  return open && prevClose ? (open / prevClose - 1) * 100 : null;
}

function fmt(v, digits = 2) {
  return v == null || !Number.isFinite(Number(v)) ? '-' : Number(v).toFixed(digits);
}

function csvCell(v) {
  let s = v == null ? '' : String(v).replace(/\r?\n/g, ' ').replace(/\s+/g, ' ').trim();
  if (/^[=+\-@]/.test(s)) s = "'" + s;
  return '"' + s.replace(/"/g, '""') + '"';
}

function cleanText(s) {
  return String(s || '')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function marketLabel(code) {
  if (/^(30|301|302)/.test(code)) return '创业板';
  if (/^68/.test(code)) return '科创板';
  if (/^60/.test(code)) return '沪主板';
  if (/^(000|001|002|003)/.test(code)) return '深主板';
  return '其他';
}

function limitPct(code, name = '') {
  if (/^(\*?ST|退)/i.test(name)) return 4.8;
  return /^(30|68)/.test(code) ? 19.5 : 9.8;
}

async function fetchUniverse() {
  const base = 'https://push2delay.eastmoney.com/api/qt/clist/get';
  const fsParam = 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23';
  const fields = 'f12,f13,f14,f2,f3,f6,f17,f18,f20,f21,f100';
  const query = pn => `${base}?pn=${pn}&pz=100&po=1&np=1&fltt=2&invt=2&fid=f12&fs=${fsParam}&fields=${fields}`;
  const first = await emJson(query(1));
  if (!first || !first.data) throw new Error('全市场行情返回为空');
  const total = Number(first.data.total);
  const pages = Math.ceil(total / 100);
  const rest = await pool(Array.from({ length: Math.max(0, pages - 1) }, (_, i) => i + 2),
    p => emJson(query(p)), 6);
  const all = (first.data.diff || []).concat(...rest.map(r => (r && r.data && r.data.diff) || []));
  const stocks = [];
  for (const x of all) {
    const code = String(x.f12 || '');
    const name = cleanText(x.f14);
    const open = validPrice(x.f17);
    const prevClose = validPrice(x.f18);
    if (!/^\d{6}$/.test(code) || !name || !open || !prevClose) continue;
    stocks.push({
      code,
      mkt: String(x.f13 || (/^6/.test(code) ? 1 : 0)),
      name,
      industry: cleanText(x.f100) || '未分类',
      open,
      prevClose,
      openPct: pctOf(open, prevClose),
      amountWan: validPrice(x.f6) ? Number(x.f6) / 1e4 : null,
      totalCap: validPrice(x.f20),
      floatCap: validPrice(x.f21),
      risk: /^(\*?ST|退)/i.test(name) ? 'ST/退市风险' : '',
    });
  }
  return stocks;
}

async function fetchDailyBatches(stocks) {
  const batches = chunk(stocks, 45);
  const results = await pool(batches, async batch => {
    const params = batch.map(s => `param=${s.mkt === '1' ? 'sh' : 'sz'}${s.code},day,,,5,qfq`);
    const d = await fetchJson('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?' + params.join('&'));
    return d && d.data ? Object.entries(d.data).map(([sym, item]) => ({ sym, item })) : [];
  }, 4);
  return results.filter(x => x && !x.error).flat();
}

async function fetchTencentQuotes(stocks) {
  const results = await pool(chunk(stocks, 60), async batch => {
    const syms = batch.map(s => `${s.mkt === '1' ? 'sh' : 'sz'}${s.code}`).join(',');
    const text = await fetchText('https://qt.gtimg.cn/q=' + syms, 'gbk');
    const map = {};
    for (const line of text.split(';')) {
      const m = line.match(/v_(\w+)="(.*)"/);
      if (m) map[m[1]] = m[2].split('~');
    }
    return map;
  }, 4);
  return Object.assign({}, ...results.filter(x => x && !x.error));
}

async function fetchEmDaily(stock) {
  const secid = `${stock.mkt === '1' ? 1 : 0}.${stock.code}`;
  const url = `https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=${secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&beg=0&end=20500101&lmt=6`;
  const d = await fetchJson(url);
  const rows = (d && d.data && d.data.klines) || [];
  if (!rows.length) throw new Error('东方财富日K为空');
  return rows.map(r => String(r).split(','));
}

async function fetchAuctionAmount(sym, fallbackWan) {
  const now = new Date();
  const mins = now.getHours() * 60 + now.getMinutes();
  if (mins >= 9 * 60 + 25 && mins < 9 * 60 + 31) return fallbackWan;
  const secid = `${sym.startsWith('sh') ? 1 : 0}.${sym.slice(2)}`;
  const url = `https://push2delay.eastmoney.com/api/qt/stock/trends2/get?secid=${secid}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57&ndays=1&iscr=0`;
  try {
    const d = await emJson(url, 1);
    const first = d && d.data && d.data.trends && d.data.trends[0];
    if (first) {
      const fields = String(first).split(',');
      const amount = Number(fields[6]);
      if ((!fields[0] || fields[0].startsWith(dateDash())) && Number.isFinite(amount) && amount > 0) {
        return amount / 1e4;
      }
    }
  } catch (_) { /* 走腾讯 m1 */ }
  try {
    const d = await fetchJson(`https://ifzq.gtimg.cn/appstock/app/kline/mkline?param=${sym},m1,,30`);
    const rows = (d.data && d.data[sym] && d.data[sym].m1) || [];
    const today = date8();
    const first = rows.map(r => Array.isArray(r) ? r : String(r).split(/[ ,]+/))
      .find(r => String(r[0]).startsWith(today) && Number(r[5]) > 0);
    if (first) return Number(first[5]) * 100 * Number(first[1]) / 1e4;
  } catch (_) { /* ignore */ }
  return null;
}

const CATEGORIES = [
  ['业绩', /业绩预告|业绩快报|净利|利润|营收|扭亏|预增|预盈|季报|季度报告|年报|中报/i],
  ['并购重组', /收购|并购|重组|控制权|股权转让|资产注入|借壳/i],
  ['订单合作', /中标|合同|订单|战略合作|合作协议|签约|框架协议/i],
  ['融资事项', /定向增发|增发|向特定对象发行|可转债|配股|发行股份购买/i],
  ['回购增持', /回购|增持|股权激励|员工持股/i],
  ['政策/产品', /获批|批准|注册证|专利|新品|量产|涨价|提价|政策|补贴|入选|认证/i],
  ['风险事项', /立案|处罚|问询|风险提示|减持|终止|亏损|诉讼|冻结/i],
  ['市场异动', /涨停原因|异动解析|竞价高开|大涨原因/i],
];

function categoriesOf(text) {
  return CATEGORIES.filter(([, re]) => re.test(text || '')).map(([name]) => name);
}

function recent(text, cutoff) {
  const t = Date.parse(String(text || '').trim().replace(' ', 'T'));
  return Number.isFinite(t) && t >= cutoff;
}

async function fetchAnnouncements(stocks, cutoff) {
  const map = new Map();
  const batches = chunk(stocks, 30);
  const results = await pool(batches, async batch => {
    const codes = batch.map(s => s.code).join(',');
    const url = `https://np-anotice-stock.eastmoney.com/api/security/ann?sr=-1&page_size=100&page_index=1&ann_type=A&client_source=web&stock_list=${encodeURIComponent(codes)}`;
    const d = await fetchJson(url);
    return (d && d.data && d.data.list) || [];
  }, 3);
  for (const list of results) {
    if (!Array.isArray(list)) continue;
    for (const a of list) {
      if (!recent(a.notice_date || a.display_time, cutoff)) continue;
      for (const c of a.codes || []) {
        const code = String(c.stock_code || '');
        if (!code) continue;
        if (!map.has(code)) map.set(code, []);
        map.get(code).push({
          date: String(a.notice_date || a.display_time || '').slice(0, 10),
          title: cleanText(a.title_ch || a.title),
          url: `https://data.eastmoney.com/notices/detail/${code}/${a.art_code}.html`,
        });
      }
    }
  }
  return map;
}

async function searchNews(keyword, cutoff) {
  const param = {
    uid: '',
    keyword,
    type: ['cmsArticleWebOld'],
    client: 'web',
    clientType: 'web',
    clientVersion: 'curr',
    param: {
      cmsArticleWebOld: {
        searchScope: 'default',
        sort: 'default',
        pageIndex: 1,
        pageSize: 8,
        preTag: '',
        postTag: '',
      },
    },
  };
  const url = 'https://search-api-web.eastmoney.com/search/jsonp?cb=cb&param=' + encodeURIComponent(JSON.stringify(param));
  const text = await fetchText(url);
  const body = text.slice(text.indexOf('(') + 1, text.lastIndexOf(')'));
  const d = JSON.parse(body);
  const list = (d.result && d.result.cmsArticleWebOld) || [];
  const seen = new Set();
  return list
    .filter(x => recent(x.date, cutoff))
    .map(x => ({
      date: String(x.date || '').slice(0, 10),
      title: cleanText(x.title),
      media: cleanText(x.mediaName),
      content: cleanText(x.content),
      url: x.url,
    }))
    .filter(x => x.title && !seen.has(x.title) && seen.add(x.title));
}

async function fetchStockNews(stock, cutoff) {
  const terms = [...new Set([stock.code, stock.name.replace(/\s+/g, '')])];
  const groups = await Promise.all(terms.map(term => searchNews(term, cutoff).catch(() => [])));
  const seen = new Set();
  return groups.flat().filter(x => !seen.has(x.title) && seen.add(x.title));
}

function enrichReason(row, announcements, news, boardCount, boardAvg, marketMedian) {
  const ann = announcements.slice(0, 2);
  const strongAnn = announcements.find(a => categoriesOf(a.title).length);
  const strongNews = [...news]
    .map(n => {
      const text = `${n.title} ${n.content || ''}`;
      let score = categoriesOf(text).length * 2;
      if (/净利润|营业收入|营收|同比增长|重大资产重组|中标|重大合同/.test(text)) score += 3;
      if (/机构风向标|持仓机构|基金持仓/.test(n.title)) score -= 3;
      return { n, score };
    })
    .filter(x => x.score > 0)
    .sort((a, b) => b.score - a.score)[0]?.n;
  const directNews = news.find(n => `${n.title} ${n.content || ''}`.includes(row.name.replace(/\s+/g, '')) ||
    `${n.title} ${n.content || ''}`.includes(row.code));
  const cats = new Set();
  if (strongAnn) categoriesOf(strongAnn.title).forEach(x => cats.add(x));
  if (strongNews) categoriesOf(`${strongNews.title} ${strongNews.content || ''}`).forEach(x => cats.add(x));

  const reasons = [];
  if (strongAnn) reasons.push(`公告${categoriesOf(strongAnn.title).join('/')}：${strongAnn.title}`);
  if (strongNews) {
    const newsCats = categoriesOf(`${strongNews.title} ${strongNews.content || ''}`).join('/');
    reasons.push(`公开资讯${newsCats}：${strongNews.title}`);
  }
  if (!strongNews && directNews) reasons.push(`近期媒体关注（未给出明确催化）：${directNews.title}`);
  if (boardCount >= 2) reasons.push(`板块联动：${row.industry}有${boardCount}只高开，候选均高开${fmt(boardAvg, 1)}%`);
  if (row.priorLimit) reasons.push(`情绪延续：昨日涨幅${fmt(row.priorChg, 1)}%，接近涨停`);
  if (!reasons.length && marketMedian > 0.3) reasons.push(`全市场竞价偏强（中位高开${fmt(marketMedian, 2)}%），暂未检索到强个股催化`);
  if (!reasons.length) reasons.push('未检索到强个股催化，需继续观察盘口与盘中公告');

  let confidence = '低';
  if (strongAnn && (strongNews || boardCount >= 2)) confidence = '较高';
  else if (strongAnn || (strongNews && boardCount >= 2)) confidence = '中';
  else if (strongNews || directNews || boardCount >= 2 || row.priorLimit) confidence = '中低';

  const clueUrls = [
    ...ann.map(x => x.url),
    ...news.slice(0, 3).map(x => x.url),
  ].filter(Boolean);

  return {
    ...row,
    categories: [...cats].join('、') || (boardCount >= 2 ? '板块联动' : (directNews ? '媒体关注' : '未识别')),
    confidence,
    reason: reasons.join('；'),
    announcementClues: ann.map(x => `${x.date} ${x.title}`).join(' | '),
    newsClues: news.slice(0, 3).map(x => `${x.date} ${x.media ? x.media + '：' : ''}${x.title}`).join(' | '),
    sourceUrls: [...new Set(clueUrls)].slice(0, 5).join(' | '),
  };
}

function makeMarkdown(rows, meta) {
  const lines = [
    `# ${meta.day} 竞价跳空高开汇总`,
    '',
    `- 口径：开盘涨幅 ≥ ${fmt(meta.minGap, 1)}%${meta.strict ? '，且开盘价 > 上一交易日最高价' : '（未启用昨高严格跳空）'}`,
    `- 有效行情：${meta.valid} 只；全市场中位高开：${fmt(meta.marketMedian, 2)}%`,
    `- 入选：${rows.length} 只`,
    '- 原因来自东方财富公开公告/资讯与板块联动规则，是线索归因，不代表官方确认。',
    '',
  ];

  if (!rows.length) {
    lines.push('今日没有符合当前口径的股票。', '');
    return lines.join('\n');
  }

  lines.push('## 板块聚集', '');
  lines.push('| 行业 | 入选数 | 平均开盘涨幅 |');
  lines.push('|---|---:|---:|');
  for (const b of meta.topBoards) lines.push(`| ${b.name} | ${b.count} | ${fmt(b.avg, 2)}% |`);

  lines.push('', '## 个股明细', '');
  lines.push('| # | 代码 | 名称 | 行业 | 开盘% | 严格跳空% | 竞价占比% | 昨涨% | 归因 | 可能原因 |');
  lines.push('|---:|---|---|---|---:|---:|---:|---:|---|---|');
  rows.forEach((r, i) => {
    lines.push(`| ${i + 1} | ${r.code} | ${r.name.replace(/\|/g, '/')} | ${r.industry.replace(/\|/g, '/')} | ${fmt(r.openPct)} | ${fmt(r.gapHighPct)} | ${fmt(r.auctionToPrevPct)} | ${fmt(r.priorChg)} | ${r.categories} (${r.confidence}) | ${r.reason.replace(/\|/g, '/')} |`);
  });

  lines.push('', '## 核验建议', '');
  lines.push('1. “较高/中”只表示公开线索与盘面共振更完整，不表示存在确定因果关系。');
  lines.push('2. 公告类原因应打开原文核对时间、金额、是否已在历史行情中计价。');
  lines.push('3. 资讯类原因可能包含媒体转述或事后解释，需与竞价额、板块联动和昨日位置共同判断。');
  lines.push('4. ST/退市风险股仅作市场统计，不作为本交易系统候选。');
  lines.push('');
  return lines.join('\n');
}

function selfTest() {
  const assert = require('assert');
  assert.strictEqual(pctOf(11, 10).toFixed(0), '10');
  assert.deepStrictEqual(categoriesOf('签署重大合同并中标'), ['订单合作']);
  assert.deepStrictEqual(categoriesOf('前三季度净利润同比增长'), ['业绩']);
  assert.strictEqual(csvCell('=1+1'), '"\'=1+1"');
  const sample = enrichReason({
    code: '000001', name: '样例股份', industry: '样例行业', openPct: 5, gapHighPct: 1,
    auctionToPrevPct: 3, priorChg: 10, priorLimit: true,
  }, [{ date: '2026-10-26', title: '签署重大合同', url: 'https://example.com/a' }], [], 3, 4.5, 0.2);
  assert.ok(sample.reason.includes('订单合作'));
  assert.ok(sample.reason.includes('板块联动'));
  console.log('self-test OK');
}

async function main() {
  if (SELF_TEST) {
    selfTest();
    process.exit(0);
  }
  const t0 = Date.now();
  const today8 = date8();
  const day = dateDash();
  const beforeAuction = new Date().getHours() * 60 + new Date().getMinutes() < 9 * 60 + 25;

  console.log(`取全市场行情... 口径：开盘≥${MIN_GAP}%${STRICT_GAP ? ' + 高于昨日最高' : ''}`);
  const universe = await fetchUniverse();
  const valid = universe.filter(s => s.openPct != null);
  const positive = valid.filter(s => s.openPct >= MIN_GAP);
  if (beforeAuction && positive.length < 20) {
    console.log('09:25 前竞价数据尚未定格，请于 09:25:10 后再运行。');
    return;
  }

  const rawByCode = new Map(positive.map(s => [s.code, s]));
  let dailyNodes = await fetchDailyBatches(positive);
  if (positive.length && !dailyNodes.length) {
    console.log('腾讯日K受限，切换东方财富日K + 腾讯实时行情...');
    const [quoteMap, barResults] = await Promise.all([
      fetchTencentQuotes(positive),
      pool(positive, s => fetchEmDaily(s), 6),
    ]);
    dailyNodes = positive.map((s, i) => {
      const sym = `${s.mkt === '1' ? 'sh' : 'sz'}${s.code}`;
      const qt = quoteMap[sym];
      const bars = barResults[i];
      if (!qt || !Array.isArray(bars) || bars.error) return null;
      return { sym, item: { qt: { [sym]: qt }, qfqday: bars } };
    }).filter(Boolean);
  }
  const candidates = [];
  let freshQuotes = 0;
  let latestQuoteTime = '';
  for (const node of dailyNodes) {
    const { sym, item } = node;
    if (!sym || !item) continue;
    const code = sym.slice(2);
    const s = rawByCode.get(code);
    if (!s) continue;
    const qt = item.qt && item.qt[sym];
    latestQuoteTime = String((qt && qt[30]) || latestQuoteTime);
    if (!qt || String(qt[30] || '').slice(0, 8) !== today8) continue;
    freshQuotes++;
    const open = validPrice(qt[5]);
    const prevClose = validPrice(qt[4]);
    const bars = (item.qfqday || item.day || [])
      .map(r => Array.isArray(r) ? r : String(r).split(','))
      .map(r => ({ date: String(r[0]), open: +r[1], close: +r[2], high: +r[3], low: +r[4], volume: +r[5] }))
      .filter(r => r.date && r.high > 0 && r.date.replace(/-/g, '') < today8);
    const prev = bars[bars.length - 1];
    const prev2 = bars[bars.length - 2];
    if (!open || !prevClose || !prev) continue;
    const openPct = pctOf(open, prevClose);
    const gapHighPct = pctOf(open, prev.high);
    if (openPct == null || openPct < MIN_GAP) continue;
    if (STRICT_GAP && !(open > prev.high)) continue;
    const priorChg = prev2 ? (prev.close / prev2.close - 1) * 100 : null;
    const prevAmountWan = prev.volume ? prev.volume * 100 * prev.close / 1e4 : null;
    candidates.push({
      ...s,
      sym,
      open,
      prevClose,
      openPct,
      gapHighPct,
      yesterdayHigh: prev.high,
      priorChg,
      priorLimit: priorChg != null && priorChg >= limitPct(code, s.name),
      prevAmountWan,
      auctionAmountWan: validPrice(qt[37]) ? Number(qt[37]) : null,
      quoteTime: String(qt[30] || ''),
      market: marketLabel(code),
    });
  }

  if (positive.length && !freshQuotes) {
    throw new Error(`竞价行情尚未刷新，腾讯最新时间 ${latestQuoteTime || '-'}；请于 09:25:10 后重试`);
  }
  if (!candidates.length) {
    if (beforeAuction) console.log('09:25 前竞价数据尚未刷新，请稍后运行。');
    else console.log('今日没有符合当前口径的股票。');
    if (!positive.length) return;
  }

  console.log(`技术筛选 ${candidates.length} 只，补齐竞价额与公开线索...`);
  const prevBars = await pool(candidates, s => fetchEmDaily(s), 6);
  candidates.forEach((s, i) => {
    if (!Array.isArray(prevBars[i]) || prevBars[i].error) return;
    const bars = prevBars[i]
      .map(r => String(r).split(','))
      .filter(r => r[0] && r[0].replace(/-/g, '') < today8);
    const prev = bars[bars.length - 1];
    const prev2 = bars[bars.length - 2];
    if (!prev) return;
    s.yesterdayHigh = +prev[3];
    s.prevAmountWan = +prev[6] / 1e4;
    if (prev2) {
      s.priorChg = (+prev[2] / +prev2[2] - 1) * 100;
      s.priorLimit = s.priorChg >= limitPct(s.code, s.name);
    }
  });
  const amounts = await pool(candidates, s => fetchAuctionAmount(s.sym, s.auctionAmountWan), 6);
  candidates.forEach((s, i) => {
    s.auctionAmountWan = amounts[i] && !amounts[i].error ? amounts[i] : s.auctionAmountWan;
    s.auctionToPrevPct = s.auctionAmountWan && s.prevAmountWan ? s.auctionAmountWan / s.prevAmountWan * 100 : null;
  });

  const cutoff = Date.now() - NEWS_DAYS * DAY_MS;
  let annMap = new Map();
  const newsMap = new Map();
  if (!NO_NEWS && candidates.length) {
    console.log('检索近 7 日公告与公开资讯...');
    annMap = await fetchAnnouncements(candidates, cutoff);
    const newsResults = await pool(candidates, async s => fetchStockNews(s, cutoff), 4);
    candidates.forEach((s, i) => {
      newsMap.set(s.code, newsResults[i] && !newsResults[i].error ? newsResults[i] : []);
    });
  }

  const validOpenPcts = valid.map(s => s.openPct).sort((a, b) => a - b);
  const marketMedian = validOpenPcts.length
    ? validOpenPcts[Math.floor(validOpenPcts.length / 2)]
    : null;
  const boardMap = new Map();
  for (const s of candidates) {
    if (!boardMap.has(s.industry)) boardMap.set(s.industry, []);
    boardMap.get(s.industry).push(s.openPct);
  }
  const topBoards = [...boardMap.entries()]
    .map(([name, pcts]) => ({ name, count: pcts.length, avg: pcts.reduce((a, b) => a + b, 0) / pcts.length }))
    .filter(x => x.count >= 2)
    .sort((a, b) => b.count - a.count || b.avg - a.avg)
    .slice(0, 10);

  const rows = candidates.map(s => {
    const pcts = boardMap.get(s.industry) || [s.openPct];
    const boardAvg = pcts.reduce((a, b) => a + b, 0) / pcts.length;
    return enrichReason(
      s,
      annMap.get(s.code) || [],
      newsMap.get(s.code) || [],
      pcts.length,
      boardAvg,
      marketMedian,
    );
  }).sort((a, b) => b.openPct - a.openPct);

  const headers = [
    '排名', '代码', '名称', '市场', '行业', '风险', '开盘价', '昨收', '昨日最高',
    '开盘涨幅%', '严格跳空%', '竞价额万', '占昨额%', '昨涨%', '昨涨停',
    '同行业入选数', '原因分类', '归因置信度', '可能原因', '公告线索', '资讯线索',
    '来源', '行情时间',
  ];
  const csvRows = rows.map((r, i) => [
    i + 1, r.code, r.name, r.market, r.industry, r.risk, fmt(r.open, 2), fmt(r.prevClose, 2),
    fmt(r.yesterdayHigh, 2), fmt(r.openPct), fmt(r.gapHighPct), fmt(r.auctionAmountWan, 0),
    fmt(r.auctionToPrevPct), fmt(r.priorChg), r.priorLimit ? '是' : '否',
    (boardMap.get(r.industry) || []).length, r.categories, r.confidence, r.reason,
    r.announcementClues, r.newsClues, r.sourceUrls, r.quoteTime,
  ]);
  const csv = [headers, ...csvRows].map(row => row.map(csvCell).join(',')).join('\n');
  const csvPath = path.join(__dirname, `竞价跳空高开_${today8}.csv`);
  const mdPath = path.join(__dirname, `竞价跳空高开_${today8}.md`);
  fs.writeFileSync(csvPath, '\ufeff' + csv, 'utf8');
  fs.writeFileSync(mdPath, '\ufeff' + makeMarkdown(rows, {
    day, minGap: MIN_GAP, strict: STRICT_GAP, valid: valid.length, marketMedian, topBoards,
  }), 'utf8');

  console.log(`\n【候选 ${rows.length} 只】全市场中位高开 ${fmt(marketMedian)}%`);
  if (topBoards.length) {
    console.log('板块聚集：' + topBoards.slice(0, 6)
      .map(b => `${b.name}(${b.count}只/${fmt(b.avg, 1)}%)`).join(' | '));
  }
  for (const r of rows.slice(0, 15)) {
    console.log(`${r.code} ${r.name} 开${fmt(r.openPct)}% 跳${fmt(r.gapHighPct)}% 竞价占比${fmt(r.auctionToPrevPct)}% | ${r.reason}`);
  }
  if (rows.length > 15) console.log(`...其余 ${rows.length - 15} 只见 CSV/Markdown`);
  console.log(`\n已保存:\n  ${csvPath}\n  ${mdPath}\n耗时 ${((Date.now() - t0) / 1000).toFixed(1)}s`);
}

main().then(() => process.exit(0)).catch(e => {
  console.error('FATAL:', String(e && e.stack ? e.stack : e).slice(0, 800));
  process.exit(1);
});
