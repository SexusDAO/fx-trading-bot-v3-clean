#!/usr/bin/env python3
"""
analyze_scanner.py — 分析所有 scanner.log 的扫描统计报告
"""
import re, sys, json, os
from collections import defaultdict, Counter
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
FX_LOG    = ROOT / "fx/logs/scanner.log"
V3_LOG    = ROOT / "v3/logs/scanner.log"
FX_MAIN   = ROOT / "fx/logs/main.log"
FX_SYSTEM = ROOT / "fx/logs/system.log"
POOL_CFG  = ROOT / "fx/scanner.py"

# ─── 读取 scanner.py 配置参数 ─────────────────────────────────────────────────
def read_pool_config():
    cfg = {}
    try:
        txt = POOL_CFG.read_text()
        for key in ["POOL_VOLUME_TOP_N","POOL_GAINERS_N","POOL_LOSERS_N","POOL_FUNDING_EXTREME_N"]:
            m = re.search(rf"{key}\s*=\s*(\d+)", txt)
            if m: cfg[key] = int(m.group(1))
    except Exception as e:
        pass
    return cfg

pool_cfg = read_pool_config()

# ─── 解析 fx/scanner.log ──────────────────────────────────────────────────────
def parse_fx_scanner_log(path):
    rounds = []
    current = None
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except Exception:
        return rounds

    for line in lines:
        # 汇总行
        m = re.match(
            r'\[scan\] (\S+)\s+扫描(\d+)\s+reversal=(\d+)\s+breakout=(\d+)\s+pullback=(\d+)'
            r'\s+原始信号(\d+)\s+最终信号(\d+)\s+开仓(\d+)\s+耗时([\d.]+)s',
            line.strip()
        )
        if m:
            if current: rounds.append(current)
            current = {
                "ts": m.group(1), "scanned": int(m.group(2)),
                "reversal": int(m.group(3)), "breakout": int(m.group(4)),
                "pullback": int(m.group(5)),
                "signals_raw": int(m.group(6)), "signals_final": int(m.group(7)),
                "placed": int(m.group(8)), "elapsed": float(m.group(9)),
                "signals": [], "rejected": [],
            }
            continue

        if current is None:
            continue

        # 信号行
        ms = re.match(
            r'\s*\[signal\]\s+(\S+)\s+(LONG|SHORT)\s+(\S+)'
            r'\s+chg24=([+-][\d.]+)%\s+rsi1h=([\d.]+)\s+vol_ratio=([\d.]+)',
            line
        )
        if ms:
            current["signals"].append({
                "symbol": ms.group(1), "dir": ms.group(2),
                "mode": ms.group(3), "chg24": float(ms.group(4)),
                "rsi1h": float(ms.group(5)), "vol_ratio": float(ms.group(6)),
            })
            continue

        # 拒绝行
        mr = re.match(r'\s*\[rejected\]\s+(\S+):\s+(.+)', line)
        if mr:
            current["rejected"].append({"symbol": mr.group(1), "reason": mr.group(2).strip()})

    if current: rounds.append(current)
    return rounds

# ─── 解析 v3/scanner.log（旧版格式）─────────────────────────────────────────
def parse_v3_scanner_log(path):
    """v3 日志格式不同，提取有限信息"""
    placed_symbols = []
    scanned_counts = []
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except Exception:
        return placed_symbols, scanned_counts

    for line in lines:
        m = re.search(r'扫描完成.*扫(?:描)?\s*(\d+)\s*个', line)
        if m:
            scanned_counts.append(int(m.group(1)))
        m2 = re.search(r'(\w+USDT)\s+(LONG|SHORT)\s+\d+分\s+→\s+pending', line)
        if m2:
            placed_symbols.append({"symbol": m2.group(1), "dir": m2.group(2)})
    return placed_symbols, scanned_counts

# ─── 从 main.log 提取开仓记录 ────────────────────────────────────────────────
def parse_placed_orders(main_log):
    orders = []
    try:
        for line in Path(main_log).read_text(encoding="utf-8").splitlines():
            m = re.search(r'(\S+) 开仓单提交 orderId=(\S+) price=([\d.]+) qty=([\d.]+)', line)
            if m:
                orders.append({
                    "symbol": m.group(1), "orderId": m.group(2),
                    "price": float(m.group(3)), "qty": float(m.group(4)),
                })
    except Exception:
        pass
    return orders

# ─── 分析候选池来源（从 scanner.py 静态推断）────────────────────────────────
def analyze_pool_source():
    """
    scanner.py 用 _add() 去重，顺序：成交量 → 涨幅 → 跌幅 → 资费
    各来源名额配置从 scanner.py 读取
    """
    cfg = pool_cfg
    vol_n  = cfg.get("POOL_VOLUME_TOP_N", 60)
    gain_n = cfg.get("POOL_GAINERS_N", 20)
    loss_n = cfg.get("POOL_LOSERS_N", 20)
    fund_n = cfg.get("POOL_FUNDING_EXTREME_N", 10)
    return vol_n, gain_n, loss_n, fund_n

# ─── 从候选池 json 读来源信息 ────────────────────────────────────────────────
def read_candidate_pool():
    try:
        d = json.loads((ROOT / "fx/logs/candidate_pool.json").read_text())
        return d.get("pool", [])
    except Exception:
        return []

# ─── 运行分析 ─────────────────────────────────────────────────────────────────
fx_rounds = parse_fx_scanner_log(FX_LOG)
v3_placed, v3_scanned = parse_v3_scanner_log(V3_LOG)
placed_orders = parse_placed_orders(FX_MAIN)
pool = read_candidate_pool()
vol_n, gain_n, loss_n, fund_n = analyze_pool_source()

print("=" * 70)
print("  SCANNER 统计报告")
print("=" * 70)
print(f"\n数据来源：")
print(f"  fx/logs/scanner.log  : {len(fx_rounds)} 轮扫描")
print(f"  v3/logs/scanner.log  : {len(v3_scanned)} 轮扫描记录（旧版格式，字段有限）")
print(f"  fx/logs/main.log     : {len(placed_orders)} 笔开仓提交")
print(f"  当前候选池            : {len(pool)} 个标的")

# ═══════════════════════════════════════════════════════════════════════════════
# 第一部分：候选池来源分布
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 70)
print("一、候选池来源分布（配置参数）")
print("─" * 70)
print(f"  构建逻辑（scanner.py _add 去重顺序）：成交量 → 涨幅 → 跌幅 → 资费")
print(f"  成交量榜  : 配置 TOP {vol_n}  → 最先加入，理论贡献 {vol_n} 个")
print(f"  涨幅榜    : 配置 TOP {gain_n}  → 与成交量去重后贡献剩余部分")
print(f"  跌幅榜    : 配置 TOP {loss_n}  → 与前两组去重后贡献剩余部分")
print(f"  资费率榜  : 配置 TOP {fund_n}  → 最后加入，去重后贡献")
print(f"  理论上限  : {vol_n + gain_n + loss_n + fund_n} 个（无重叠时）")
print(f"  实际候选池: {len(pool)} 个（最近一次刷新）")
print()
print(f"  注：scanner.log 未记录每轮各来源贡献数，下方为静态配置参数")
print(f"  实际重叠率约 {(vol_n+gain_n+loss_n+fund_n - len(pool)) / (vol_n+gain_n+loss_n+fund_n) * 100:.1f}%（当前候选池 vs 理论上限）" if pool else "")

# ═══════════════════════════════════════════════════════════════════════════════
# 第二部分：分类结果统计
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 70)
print("二、分类结果统计（fx 新系统，扫描标的的模式分布）")
print("─" * 70)

total_scanned   = sum(r["scanned"]    for r in fx_rounds)
total_pullback  = sum(r["pullback"]   for r in fx_rounds)
total_breakout  = sum(r["breakout"]   for r in fx_rounds)
total_reversal  = sum(r["reversal"]   for r in fx_rounds)
total_raw_sig   = sum(r["signals_raw"]  for r in fx_rounds)
total_fin_sig   = sum(r["signals_final"] for r in fx_rounds)
total_placed    = sum(r["placed"]     for r in fx_rounds)
total_rejected  = sum(len(r["rejected"]) for r in fx_rounds)

classified = total_pullback + total_breakout + total_reversal
print(f"  总扫描标的数（去重后各轮累计）: {total_scanned}")
print(f"  分类为 pullback  : {total_pullback:5d}  占已分类 {total_pullback/classified*100:.1f}%" if classified else "")
print(f"  分类为 breakout  : {total_breakout:5d}  占已分类 {total_breakout/classified*100:.1f}%" if classified else "")
print(f"  分类为 reversal  : {total_reversal:5d}  占已分类 {total_reversal/classified*100:.1f}%" if classified else "")
print(f"  ──")
print(f"  通过分类 → 触发原始信号: {total_raw_sig}  ({total_raw_sig/total_scanned*100:.1f}% of scanned)" if total_scanned else "")
print(f"  原始信号 → 最终信号    : {total_fin_sig}  (过滤掉 {total_raw_sig - total_fin_sig} 个槽位满)")
print(f"  最终信号 → 开仓提交    : {total_placed}")

# 信号按方向分布
long_sigs  = sum(1 for r in fx_rounds for s in r["signals"] if s["dir"] == "LONG")
short_sigs = sum(1 for r in fx_rounds for s in r["signals"] if s["dir"] == "SHORT")
print(f"\n  信号方向分布：LONG {long_sigs} / SHORT {short_sigs}")

# 信号按模式分布（来自 signal 行）
mode_cnt = Counter(s["mode"] for r in fx_rounds for s in r["signals"])
print(f"  信号模式分布（已过槽位筛选前）：")
for mode, cnt in mode_cnt.most_common():
    print(f"    {mode:12s}: {cnt}")

# ═══════════════════════════════════════════════════════════════════════════════
# 第三部分：judge 函数通过率
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 70)
print("三、judge 函数通过率")
print("─" * 70)
print("  注：scanner.log 未记录 judge 拒绝原因（被 analyze() 内部消化）")
print("  以下为可间接推算的数据：")
print()
print(f"  判断逻辑：每个扫描标的 → classify() → 对应 judge() → 通过则产生原始信号")
print(f"  judge 总调用次数（≈扫描标的数）: {total_scanned}")
print(f"  judge 返回 True（产生原始信号） : {total_raw_sig}  ({total_raw_sig/total_scanned*100:.1f}%)" if total_scanned else "")
print(f"  judge 返回 False（拒绝）        : {total_scanned - total_raw_sig}  ({(total_scanned-total_raw_sig)/total_scanned*100:.1f}%)" if total_scanned else "")
print()

# 按模式估算各 judge 通过率
print(f"  按模式估算（classified 口径）：")
print(f"    judge_pullback  调用 ~{total_scanned} 次（大多数走 pullback 路径）")
print(f"      → 通过 {total_pullback} 次，通过率 {total_pullback/total_scanned*100:.1f}%" if total_scanned else "")
print(f"    judge_breakout  调用估算（classified breakout 数）: {total_breakout}")
print(f"    judge_reversal  调用估算（classified reversal 数）: {total_reversal}")
print()
print("  ⚠ judge 拒绝原因明细需要修改 scanner.py 写入日志，当前日志未采集")
print("  以下为 judges.py 代码中定义的所有拒绝条件（供参考）：")
reject_reasons_code = [
    "4H空头结构，禁止做多",
    "1H EMA不满足多头结构",
    "4H多头结构，禁止做空",
    "1H EMA不满足空头结构",
    "价格不在回踩区间",
    "无有效15M止跌/止涨信号",
    "价格不在成交密集区",
    "高波动层多头：资金费率过高",
    "高波动层空头：资金费率过低",
    "无有效1H止跌/止涨信号（高波动层）",
    "信号强度弱，跳过",
    "突破幅度不足",
    "成交量不足：量比<阈值 或非近3根最大",
    "15M确认K线未闭合，等待下一轮",
    "RSI1H超买追多 / 超卖追空",
    "OI上升不足",
    "内部异常 (NoneType等)",
]
for i, r in enumerate(reject_reasons_code, 1):
    print(f"    {i:2d}. {r}")

# ═══════════════════════════════════════════════════════════════════════════════
# 第四部分：被拒信号分析（slot 拒绝）
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 70)
print("四、最终信号拒绝原因分布（slot 槽位级别）")
print("─" * 70)
reject_counter = Counter()
for r in fx_rounds:
    for rej in r["rejected"]:
        reject_counter[rej["reason"]] += 1

print(f"  总拒绝次数: {sum(reject_counter.values())}")
for reason, cnt in reject_counter.most_common(15):
    bar = "█" * int(cnt / max(reject_counter.values()) * 20)
    print(f"  {reason:30s}: {cnt:5d}  {bar}")

# 被拒标的 TOP
rejected_syms = Counter()
for r in fx_rounds:
    for rej in r["rejected"]:
        rejected_syms[rej["symbol"]] += 1
print(f"\n  被拒最频繁的标的 TOP 10（信号强但反复被槽位拒绝）：")
for sym, cnt in rejected_syms.most_common(10):
    print(f"    {sym:20s}: {cnt} 次")

# ═══════════════════════════════════════════════════════════════════════════════
# 第五部分：开仓成功标的分析
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 70)
print("五、开仓提交情况（fx 新系统）")
print("─" * 70)
placed_sym_counter = Counter(o["symbol"] for o in placed_orders)
print(f"  总开仓提交次数: {len(placed_orders)}")
print(f"  涉及标的数    : {len(placed_sym_counter)}")
print(f"\n  开仓频次 TOP 15（含重复挂单）：")
for sym, cnt in placed_sym_counter.most_common(15):
    # 检查是否有相应的信号行
    sig_found = [s for r in fx_rounds for s in r["signals"] if s["symbol"] == sym]
    mode_str = "/" .join(set(s["mode"] for s in sig_found)) if sig_found else "?"
    print(f"    {sym:22s}: {cnt:3d} 次  mode={mode_str}")

# ═══════════════════════════════════════════════════════════════════════════════
# 第六部分：整体运行健康度
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 70)
print("六、系统整体健康度指标")
print("─" * 70)

avg_scan_time  = sum(r["elapsed"] for r in fx_rounds) / len(fx_rounds) if fx_rounds else 0
max_scan_time  = max((r["elapsed"] for r in fx_rounds), default=0)
avg_scanned    = total_scanned / len(fx_rounds) if fx_rounds else 0
signal_rate    = total_raw_sig / total_scanned if total_scanned else 0
open_rate      = total_placed / total_fin_sig if total_fin_sig else 0

print(f"  扫描轮数            : {len(fx_rounds)}")
print(f"  平均每轮耗时        : {avg_scan_time:.1f}s  (最长 {max_scan_time:.1f}s)")
print(f"  平均每轮扫描标的    : {avg_scanned:.1f}")
print(f"  整体信号产生率      : {signal_rate*100:.1f}%  (标的→原始信号)")
print(f"  信号→开仓转化率     : {total_placed}/{total_fin_sig} = {open_rate*100:.1f}%")
print()

# 按小时统计活跃度
hour_counter = Counter()
for r in fx_rounds:
    try:
        h = datetime.fromisoformat(r["ts"].replace("Z","")).hour
        hour_counter[h] += 1
    except Exception:
        pass
if hour_counter:
    print(f"  扫描活跃小时分布（UTC）：")
    for h in sorted(hour_counter):
        bar = "█" * hour_counter[h]
        print(f"    {h:02d}:00  {bar}  ({hour_counter[h]}轮)")

# ═══════════════════════════════════════════════════════════════════════════════
# 第七部分：v3 旧系统对比
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 70)
print("七、v3 旧系统扫描数据（2026-08-03 ~ 08-04）")
print("─" * 70)
print(f"  扫描轮数: {len(v3_scanned)}")
if v3_scanned:
    print(f"  平均扫描标的: {sum(v3_scanned)/len(v3_scanned):.1f}")
    print(f"  范围: {min(v3_scanned)} ~ {max(v3_scanned)}")
print(f"  产生挂单信号: {len(v3_placed)} 笔")
if v3_placed:
    for p in v3_placed[:10]:
        print(f"    {p['symbol']} {p['dir']}")
    if len(v3_placed) > 10:
        print(f"    ... 共 {len(v3_placed)} 笔")

print("\n" + "=" * 70)
print("  分析完成")
print("=" * 70)
