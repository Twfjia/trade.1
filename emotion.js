'use strict';

// 情绪周期选股工具（MVP，随时可跑）
// 数据源：腾讯（行业/概念板块 + 成分股 + 批量行情 + 日K），沙箱内可直连
// 主流程：行业板块排名 -> 龙头梯队 -> 竞价/高开统计 -> 情绪周期近似 -> 买点标签
// 运行：node emotion.js

const fs = require('fs');
const path = require('path');
const { calcSpace, spaceAlerts } = require('./lib_space');

const H = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
  'Referer': 'https://gu.qq.com/',
  'Accept': 'application/json, text/plain, */*',
};

function nowPadded(n) { return String(n).padStart(2, '0'); }
function ymd(d = new Date()) { return `${d.getFullYear()}-${nowPadded(d.getMonth() + 1)}-${nowPadded(d.getDate())}`; }
function ymd8(d = new Date()) { return ymd(d).replace(/-/g, ''); }
function num(v) { const n = parseFloat(v); return isFinite(n) ? n : null; }
function limitPctOf(code) { return /^(30|68)/.test(code) ? 1.2 : 1.1; }
function normCode(x) { return String(x || '').replace(/^(sh|sz)/i, ''); }

async function jget(url, tries = 3) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(url, { headers: H, signal: AbortSignal.timeout(15000) });
      return await r.json();
    } catch (e) {
      if (i === tries - 1) throw e;
      await new Promise(s => setTimeout(s, 500 * (i + 1)));
    }
  }
}

const RANK = 'https://proxy.finance.qq.com/cgi/cgi-bin/rank';
async function fetchBoards(type) {
  const out = [];
  let offset = 0;
  while (true) {
    const d = await jget(`${RANK}/pt/getRank?board_type=${type}&sort_type=priceChange&direct=down&offset=${offset}&count=100`);
    const list = (d.data && d.data.rank_list) || [];
    out.push(...list);
    if (list.length < 100) break;
    offset += 100;
  }
  return out;
}
async function boardMembers(boardCode, type, count = 80) {
  const d = await jget(`${RANK}/hs/getBoardRankList?board_type=${type}&board_code=${boardCode}&sort_type=priceChange&direct=down&offset=0&count=${count}`);
  return (d.data && d.data.rank_list) || [];
}

// 批量行情（腾讯 qt.gtimg.cn，每批 60）
async function batchQuotes(codes) {
  const map = new Map();
  const pref = c => (c[0] === '6' ? 'sh' : 'sz') + c;
  const groups = [];
  for (let i = 0; i < codes.length; i += 60) groups.push(codes.slice(i, i + 60).map(pref));
  let idx = 0;
  async function run() {
    while (idx < groups.length) {
      const g = groups[idx++];
      try {
        const r = await fetch('https://qt.gtimg.cn/q=' + g.join(','), { headers: { 'User-Agent': 'Mozilla/5.0' }, signal: AbortSignal.timeout(15000) });
        const txt = new TextDecoder('gbk').decode(await r.arrayBuffer());
        for (const ln of txt.split(';')) {
          const m = ln.match(/v_(\w+)="([^"]*)"/);
          if (!m) continue;
          const f = m[2].split('~');
          if (f.length < 50) continue;
          map.set(m[1].slice(2), {
            code: m[1].slice(2), name: f[1], price: num(f[3]), prevClose: num(f[4]), open: num(f[5]),
            zdf: num(f[32]), high: num(f[33]), low: num(f[34]),
            amountWan: num(f[37]), turnover: num(f[38]), floatCap: num(f[44]), totalCap: num(f[45]),
            limit: num(f[47]), volRatio: num(f[49]), time: f[30],
          });
        }
      } catch (e) { /* ignore single batch error */ }
    }
  }
  const ws = [];
  for (let w = 0; w < Math.min(10, groups.length); w++) ws.push(run());
  await Promise.all(ws);
  return map;
}

// 腾讯日K（前复权）
async function txDay(sym, limit = 60) {
  const d = await jget(`https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=${sym},day,,,${limit},qfq`);
  const node = d.data && d.data[sym];
  const rows = (node && (node.qfqday || node.day)) || [];
  return rows.map(r => {
    const a = Array.isArray(r) ? r : String(r).split(',');
    return { date: a[0], open: +a[1], close: +a[2], high: +a[3], low: +a[4], volume: +a[5] };
  }).filter(b => b.date && isFinite(b.close) && b.close > 0);
}
function countBoards(bars, code) {
  const lim = /^(30|68)/.test(code) ? 19.8 : 9.8;
  let n = 0;
  for (let i = bars.length - 1; i >= 1; i--) {
    const up = (bars[i].close / bars[i - 1].close - 1) * 100;
    if (Math.abs(bars[i].close - bars[i].high) < 0.011 && up >= lim) n++;
    else break;
  }
  return n;
}

function marketMode(now) {
  const h = now.getHours(), m = now.getMinutes();
  if (h === 9 && m >= 25 && m <= 30) return { label: '竞价窗口(09:25-09:30)', auction: true };
  if ((h === 9 && m > 30) || (h >= 10 && h < 15) || (h === 15 && m === 0)) return { label: '盘中', auction: false };
  return { label: '非交易时段(最近快照)', auction: false };
}

async function main() {
  const t0 = Date.now();
  const now = new Date();
  const today = ymd(now);
  const mode = marketMode(now);
  console.log(`情绪周期选股 | ${ymd(now)} ${nowPadded(now.getHours())}:${nowPadded(now.getMinutes())} | ${mode.label}`);
  if (!mode.auction) console.log('  (非竞价窗口：成交额万/开盘% 为最近快照，占昨额% 仅在 09:25–09:30 竞价窗口有值)');

  console.log('取行业板块排名...');
  const hy = await fetchBoards('hy');
  const boardZdfOk = hy.some(b => Math.abs(num(b.zdf) || 0) > 0.001);
  const boardSortKey = b => boardZdfOk ? (num(b.zdf) || 0) : (num(b.lzg && b.lzg.zdf) || 0);
  const boards = hy.filter(b => b.name && !/ST|退市/.test(b.name))
    .sort((a, b) => boardSortKey(b) - boardSortKey(a));
  console.log(`  行业板块 ${boards.length} 个`);
  if (!boardZdfOk) console.log('  板块指数涨跌未刷新，按领涨股涨幅排序');

  console.log('取涨停/连板概念池...');
  let hotBoards = [];
  try {
    const gn = await fetchBoards('gn');
    hotBoards = gn.filter(b => /涨停|连板/.test(b.name));
  } catch (e) { console.log('  概念池取数失败（忽略）:', String(e).slice(0, 60)); }

  // 主线板块：涨幅榜前 10
  const topBoards = boards.slice(0, 10);
  const memberByBoard = [];
  const memberLb = new Map();
  for (const b of topBoards) {
    const ms = await boardMembers(b.code, 'hy', 80);
    for (const m of ms) memberLb.set(normCode(m.code), num(m.lb));
    memberByBoard.push({ board: b.name, zdf: num(b.zdf), zgb: b.zgb, members: ms });
  }

  // 涨停/连板池（辨识度高标）
  const hotSet = new Set();
  const hotNames = new Map();
  for (const hb of hotBoards.slice(0, 4)) {
    const ms = await boardMembers(hb.code, 'gn', 300);
    for (const m of ms) { const c = normCode(m.code); hotSet.add(c); if (m.name) hotNames.set(c, m.name); }
  }

  // 汇总候选代码并取批量行情
  const candCodeSet = new Set(hotSet);
  for (const mb of memberByBoard) for (const m of mb.members) candCodeSet.add(normCode(m.code));
  const candCodes = [...candCodeSet].filter(c => /^(60|000|001|002|003|300|301|302)/.test(c));
  console.log(`取批量行情 ${candCodes.length} 只...`);
  const quotes = await batchQuotes(candCodes);

  // 组装候选
  const cands = [];
  for (const code of candCodes) {
    const q = quotes.get(code);
    if (!q) continue;
    const gap = q.open && q.prevClose ? (q.open / q.prevClose - 1) * 100 : null;
    const lb = memberLb.get(code);
    const volRatio = (q.volRatio != null && q.volRatio > 0) ? q.volRatio : (lb != null ? lb : q.volRatio);
    cands.push({ ...q, volRatio, gap, board: '', hot: hotSet.has(code) });
  }
  for (const mb of memberByBoard) {
    for (const c of cands) if (!c.board && mb.members.some(m => normCode(m.code) === c.code)) c.board = mb.board;
  }

  // 市场与板块统计
  const upBoards = boards.filter(b => num(b.zdf) > 0).length;
  const boardAvg = boards.reduce((s, b) => s + (num(b.zdf) || 0), 0) / boards.length;
  const limitOpen = cands.filter(c => c.gap != null && c.gap >= (limitPctOf(c.code) - 1) * 100 - 0.5).length;
  const highOpen = cands.filter(c => c.gap != null && c.gap >= 4).length;
  const upShare = cands.length ? cands.filter(c => c.gap != null && c.gap > 0).length / cands.length : 0;
  console.log(`  板块均涨 ${boardAvg.toFixed(2)}% | 上涨板块 ${upBoards}/${boards.length}`);
  console.log(`  候选内 涨停开盘 ${limitOpen} | 高开>4% ${highOpen} | 上涨占比 ${(upShare * 100).toFixed(1)}%`);
  const cycle = classifyCycle({ limitOpen, highOpen, upShare, boardAvg });
  console.log(`  情绪周期(近似): ${cycle}`);

  // 取日K：只对“可打/可追 + 昨日涨停连板 + 前30只低吸观察”取日K，控制请求量
  const actionable = cands.filter(c => c.gap != null && (c.gap >= 3 || c.hot));
  const lowAbs = cands.filter(c => c.gap != null && c.gap < 3 && c.gap > -5)
    .sort((a, b) => (b.amountWan || 0) - (a.amountWan || 0)).slice(0, 30);
  const uniqMap = new Map();
  for (const c of [...actionable, ...lowAbs]) uniqMap.set(c.code, c);
  const uniq = [...uniqMap.values()];
  const symOf = c => (c.code[0] === '6' ? 'sh' : 'sz') + c.code;
  const poolRun = async (items, worker, conc = 10) => {
    const res = new Array(items.length); let i = 0;
    async function run() { while (i < items.length) { const k = i++; try { res[k] = await worker(items[k]); } catch (e) { res[k] = { error: String(e).slice(0, 80) }; } } }
    const ws = []; for (let w = 0; w < Math.min(conc, items.length); w++) ws.push(run());
    await Promise.all(ws); return res;
  };
  console.log(`取日K ${uniq.length} 只...`);
  const dayRes = await poolRun(uniq, c => txDay(symOf(c)).then(bars => ({ bars })));

  const rows = [];
  uniq.forEach((c, i) => {
    const allBars = dayRes[i] && !dayRes[i].error ? dayRes[i].bars : [];
    const bars = allBars.filter(b => b.date < today);
    const prev = bars.length ? bars[bars.length - 1] : null;
    const prevAmt = prev ? prev.volume * 100 * (prev.open + prev.high + prev.low + prev.close) / 4 : null;
    const ratio = mode.auction && c.amountWan && prevAmt ? c.amountWan * 1e4 / prevAmt * 100 : null;
    const thr = (c.totalCap || 0) >= 800e8 ? 3 : 5;
    const boards = bars.length ? countBoards(bars, c.code) : 0;
    const sp = bars.length ? calcSpace(c.code, bars) : null;
    const spAlert = sp ? spaceAlerts(sp, c.code) : '无数据';
    const lim = (limitPctOf(c.code) - 1) * 100;

    let tag = '观察';
    if (c.gap != null && c.gap >= lim - 0.5) tag = '打板(一字/秒板候选)';
    else if (c.gap != null && c.gap >= 3 && ratio != null && ratio >= thr) tag = '竞价抢筹';
    else if (c.gap != null && c.gap > 2) tag = '高开(弱主线不追)';
    else if (c.gap != null && c.gap > -5) tag = '低吸观察';
    else if (c.gap != null) tag = '深低开(卖方主导)';
    if (/核按钮|停牌区|剩余空间<10%/.test(spAlert)) tag += ' · 核按钮风险';

    rows.push({
      board: c.board || (c.hot ? '涨停/连板池' : '其他'), code: c.code, name: c.name, gap: c.gap,
      amountWan: c.amountWan, ratio, volRatio: c.volRatio, turnover: c.turnover,
      capYi: (c.totalCap || 0), boards, tag, spAlert, hot: c.hot,
    });
  });

  const rankOf = r => r.tag.startsWith('打板') ? 0 : r.tag.startsWith('竞价抢筹') ? 1 : r.tag.startsWith('高开') ? 2 : r.tag.startsWith('低吸') ? 3 : 4;
  rows.sort((a, b) => (rankOf(a) - rankOf(b)) || (b.boards - a.boards) || ((b.amountWan || 0) - (a.amountWan || 0)));

  const head = ['板块', '代码', '名称', '开盘%', '成交额万', '占昨额%', '量比', '换手%', '市值亿', '连板', '买点', '异动提示'];
  const cell = r => ({
    '板块': r.board, '代码': r.code, '名称': r.name,
    '开盘%': r.gap == null ? '-' : r.gap.toFixed(2),
    '成交额万': r.amountWan == null ? '-' : Math.round(r.amountWan),
    '占昨额%': r.ratio == null ? '-' : r.ratio.toFixed(1),
    '量比': r.volRatio == null ? '-' : r.volRatio.toFixed(2),
    '换手%': r.turnover == null ? '-' : r.turnover.toFixed(1),
    '市值亿': r.capYi == null ? '-' : (r.capYi / 1e8).toFixed(0),
    '连板': r.boards, '买点': r.tag, '异动提示': r.spAlert || '-',
  });
  const w = head.map(h => Math.max(h.length, ...rows.map(r => String(cell(r)[h]).length)));
  const line = hs => hs.map((h, i) => String(h).padEnd(w[i] + 1)).join('');

  console.log('\n【主线板块】(按板块涨幅)');
  for (const b of topBoards) {
    const bz = boardZdfOk ? num(b.zdf) : (num(b.lzg && b.lzg.zdf) || 0);
    const limN = memberByBoard.find(x => x.board === b.name)?.members.filter(m => {
      const c = cands.find(y => y.code === normCode(m.code));
      return c && c.gap != null && c.gap >= (limitPctOf(c.code) - 1) * 100 - 0.5;
    }).length || 0;
    console.log(`  ${b.name} | ${boardZdfOk ? '板块涨' : '领涨股'} ${bz.toFixed(2)}% | 上涨/总数 ${b.zgb} | 竞价涨停(候选内) ${limN}`);
  }
  console.log('\n【候选】');
  const show = (group, title, n = 30) => {
    console.log(`\n  ▍${title} ${group.length ? group.length + ' 只' : '（无）'}`);
    if (!group.length) return;
    const g = group.slice(0, n);
    console.log('  ' + line(head));
    console.log('  ' + head.map((_, i) => '-'.repeat(w[i] + 1)).join(''));
    for (const r of g) console.log('  ' + line(head.map(h => cell(r)[h])));
  };
  show(rows.filter(r => r.tag.startsWith('打板')), '打板候选');
  show(rows.filter(r => r.tag.startsWith('竞价抢筹')), '竞价抢筹候选');
  show(rows.filter(r => r.tag.startsWith('低吸观察')), '低吸观察');

  const csv = ['板块,代码,名称,开盘%,成交额万,占昨额%,量比,换手%,市值亿,连板,买点,异动提示']
    .concat(rows.map(r => [r.board, r.code, r.name,
      r.gap == null ? '' : r.gap.toFixed(2), r.amountWan == null ? '' : Math.round(r.amountWan),
      r.ratio == null ? '' : r.ratio.toFixed(1), r.volRatio == null ? '' : r.volRatio.toFixed(2),
      r.turnover == null ? '' : r.turnover.toFixed(1), r.capYi == null ? '' : (r.capYi / 1e8).toFixed(0),
      r.boards, r.tag, (r.spAlert || '').replace(/,/g, '；')].join(',')))
    .join('\n');
  const csvPath = path.join(__dirname, `情绪周期选股_${ymd8(now)}_${nowPadded(now.getHours())}${nowPadded(now.getMinutes())}.csv`);
  fs.writeFileSync(csvPath, '\ufeff' + csv, 'utf8');
  console.log(`\n已保存: ${csvPath}  耗时 ${((Date.now() - t0) / 1000).toFixed(1)}s`);
}

function classifyCycle({ limitOpen, highOpen, upShare, boardAvg }) {
  if (limitOpen >= 20 && highOpen >= 30) return '高潮/主升';
  if (limitOpen >= 10) return '主升/发酵';
  if (upShare < 0.3 && limitOpen <= 3 && boardAvg < 0) return '冰点/退潮';
  if (upShare < 0.55) return '分歧';
  return '修复';
}

main().catch(e => { console.error('运行失败:', String(e).slice(0, 300)); process.exit(1); });
