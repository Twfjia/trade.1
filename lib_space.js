// 龙头涨幅空间 + 异动状态测算（公共模块，screener/space 共用）
// 口径：从本轮首板（起涨日）算起；10日/200%上限；剩余 = 3 ÷ (1+已涨幅) − 1
// 同时输出 3日累计涨幅（仅数据展示）与 30日口径提示
'use strict';

function calcSpace(code, bars) {
  if (!bars || bars.length < 3) return null;
  const limit = /^30/.test(code) ? 19.5 : 9.8; // 创业板20%涨跌幅
  const lu = [];
  for (let i = 1; i < bars.length; i++) {
    const pct = (bars[i].close / bars[i - 1].close - 1) * 100;
    if (pct >= limit * 0.95) lu.push(i);
  }
  if (!lu.length) return null;
  // 涨停日按间隔≤6个交易日聚成"一波"，取含最近涨停的那一波首日为起涨日
  const waves = [];
  let w = [lu[0]];
  for (let k = 1; k < lu.length; k++) {
    if (lu[k] - lu[k - 1] <= 6) w.push(lu[k]);
    else { waves.push(w); w = [lu[k]]; }
  }
  waves.push(w);
  const lastWave = waves[waves.length - 1];
  const firstIdx = lastWave[0];
  const lastIdx = bars.length - 1;
  const firstClose = bars[firstIdx].close;
  const lastClose = bars[lastIdx].close;
  const daysUsed = lastIdx - firstIdx + 1;
  // 10日窗口口径：≤10日用首板为基准；超窗按近10日滚动
  const baseIdx = daysUsed <= 10 ? firstIdx : lastIdx - 10;
  const baseClose = bars[baseIdx].close;
  const gain = (lastClose / baseClose - 1) * 100;
  const remaining = ((1 + 2.0) / (1 + gain / 100) - 1) * 100;
  const daysLeft = Math.max(0, 10 - (lastIdx - baseIdx));
  const feasible = Math.min(remaining, daysLeft * limit);
  // 3日累计涨幅（仅数据展示，不参与异动或核按钮判断）
  const threeDay = bars.length >= 4 ? (bars[lastIdx].close / bars[lastIdx - 3].close - 1) * 100 : null;
  // 30日累计涨幅（30日200%上限参考）
  const gain30 = bars.length >= 31 ? (bars[lastIdx].close / bars[lastIdx - 30].close - 1) * 100 : null;
  return {
    startDate: bars[firstIdx].date,
    startClose: firstClose,
    gain, remaining, feasible,
    daysUsed: lastIdx - baseIdx + 1,
    daysLeft, luCount: lastWave.length,
    rolled: daysUsed > 10,
    limitPct: limit,
    threeDay, gain30,
  };
}

// 异动/监管提示（自动部分；是否在监管期需人工查公告确认）
function spaceAlerts(sp, code) {
  if (!sp) return '无首板数据';
  const st = [];
  if (sp.gain >= 200) st.push('已到10日200%停牌区');
  else if (sp.gain >= 100) st.push('已触发10日100%·或进监管');
  if (sp.remaining <= 10) st.push('剩余空间<10%');
  if (sp.daysLeft === 0 && !sp.rolled) st.push('10日窗口已满');
  if (sp.rolled) st.push('超10日·滚动口径');
  if (/^30/.test(code)) st.push('创业板口径复核');
  return st.join(' | ') || '空间正常';
}

// 是否已触发严重异动（10日100% / 30日200%）
function triggeredAbnormal(sp, code) {
  if (!sp) return false;
  if (sp.gain >= 100) return true;      // 10日窗口已涨幅≥100%
  if (sp.gain30 != null && sp.gain30 >= 200) return true; // 30日200%
  return false;
}

module.exports = { calcSpace, spaceAlerts, triggeredAbnormal };
