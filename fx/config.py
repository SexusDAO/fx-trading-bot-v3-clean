"""
fx/config.py — 全局参数集中管理（V3.0 完整版）
规则：所有其他文件禁止出现硬编码数字，全部从这里 import。
敏感参数从环境变量读取，不硬编码。
"""
import os
from pathlib import Path

_FX_ROOT = Path(__file__).parent  # fx/ 目录绝对路径

# ═══════════════════════════════════════════════════════════════════════════════
# 第一组：系统基础配置
# ═══════════════════════════════════════════════════════════════════════════════
# ── API Key ─────────────────────────────────────────────────────────────────────────────────────────────
# API Key 必须通过环境变量注入，禁止硬编码。
# export BINANCE_API_KEY="your_key"
# export BINANCE_API_SECRET="your_secret"
BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY") or ""
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET") or ""
# 导入时只警告，不立即报错——允许测试/回测脚本在没有 key 的环境导入 config。
# 真正需要下单/查账时，binance_api 层会因请求失败而报错。
import logging as _cfg_log
if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    _cfg_log.getLogger(__name__).warning(
        "[fx/config] BINANCE_API_KEY / BINANCE_API_SECRET 未设置，"
        "任何需要签名的 API 调用都会失败。"
    )

# [fix #39] 必填环境变量：启动时（main.py）缺失直接报错退出；导入 config 本身不报错，便于测试/回测。
REQUIRED_ENV_VARS = ("BINANCE_API_KEY", "BINANCE_API_SECRET", "OPENAI_API_KEY", "OPENAI_BASE_URL")   # 代理改为可选，默认直连


def require_env() -> None:
    """缺失任一必填环境变量时抛 RuntimeError。"""
    missing = [k for k in REQUIRED_ENV_VARS if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"缺少必填环境变量: {', '.join(missing)}（参见 fx/.env.example）")


# ── SOCKS5 代理 ────────────────────────────────────────────────────────────────
PROXY_URL = (os.environ.get("PROXY_URL") or "").strip()  # 可选；未设置 → 直连。格式 socks5://user:pass@host:port
PROXIES   = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else {}
BINANCE_BASE_URL   = "https://fapi.binance.com"
BINANCE_WS_URL     = "wss://fstream.binance.com"

# WebSocket 参数
WS_KEEPALIVE_INTERVAL = 20 * 60   # listenKey 续期间隔，秒（20 分钟）
WS_RECONNECT_BASE     = 3         # 断线重连初始等待，秒
WS_RECONNECT_MAX      = 60        # 断线重连最大等待，秒
# websockets 15.x 默认 max_queue=16；此处 32 略大于默认值，给突发帧少量缓冲。
# 注意：队列满后 websockets 是暂停读 socket（TCP 背压），不是丢帧；markPrice 丢旧帧由 ws_listener 的有限并发分发实现。
WS_MARK_PRICE_MAX_QUEUE = 32   # 三条 WS 连接共用的接收队列高水位
# USE_TESTNET 已删除 —— 代码从未引用此常量，保留会造成"改了就切测试网"的错觉。
# 如需切测试网，请修改 BINANCE_BASE_URL 和 BINANCE_WS_BASE_URL。

# Clawby Pro（已于 2026-08-28 迁移，旧 keystore.com.cn 代理已废弃）
# 接口配置集中在 fx/shared/clawby_adapter.py
# COINGLASS_CACHE_TTL 已删除 —— clawby_adapter.py 内部用自己的 CACHE_TTL=15*60，此常量从未被引用。

# Telegram
# TELEGRAM_BOT_TOKEN — Telegram 推送已弃用，改用 Jarvis notifier
# TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")  # 保留兼容
# TELEGRAM_CHAT_ID — 同上
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")  # 保留兼容
TELEGRAM_DAILY_REPORT_HOUR = 2   # UTC，对应北京时间 10:00  # [fix #41] calc_sl_tp 已改为读取此常量

# ═══════════════════════════════════════════════════════════════════════════════
# 第二组：候选池参数
# ═══════════════════════════════════════════════════════════════════════════════
UNIVERSE_VOLUME_TOP     = 50          # 成交量榜前 N（v3.6 缩减，加入 CG 清算量来源）
UNIVERSE_GAIN_TOP       = 30          # 涨幅榜前 N（v3.3.5: 25→30）
UNIVERSE_LOSS_TOP       = 30          # 跌幅榜前 N（v3.3.5: 25→30）
UNIVERSE_FUNDING_TOP    = 25          # 资金费率极端榜前 N（v3.3.5: 20→25）
UNIVERSE_FUNDING_MIN_ABS = 0.001      # 资金费率绝对值门槛，0.1%

UNIVERSE_MIN_VOLUME_24H = 5_000_000   # 24H 最低成交额（300万 → 500万，过滤易被拉盘的薄流动性山寨）
UNIVERSE_BLACKLIST      = [
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "USDTUSDT", "FDUSDUSDT",
]

# ═══════════════════════════════════════════════════════════════════════════════
# 第三组：扫描参数
# ═══════════════════════════════════════════════════════════════════════════════

# 资金费率结算窗口
FUNDING_SETTLE_HOURS           = {0, 8, 16}   # UTC 结算时点（整点）
FUNDING_WINDOW_NORMAL_BEFORE   = 900    # 普通费率结算前，秒
FUNDING_WINDOW_NORMAL_AFTER    = 300    # 普通费率结算后，秒
FUNDING_WINDOW_EXTREME_BEFORE  = 1200   # 极端费率结算前，秒
FUNDING_WINDOW_EXTREME_AFTER   = 600    # 极端费率结算后，秒
FUNDING_EXTREME_THRESHOLD      = 0.005  # 资金费率极端阈值，0.5%

# BTC 异常波动拦截
BTC_SHOCK_THRESHOLD = 0.03   # BTC 15 分钟涨跌超过 3% 暂停扫描

# OI 背离拦截
OI_DIVERGENCE_PRICE_PCT = 0.98   # 价格达到 24H 高点 98% 触发检查
OI_DIVERGENCE_DROP_PCT  = 0.20   # OI 同期下降超过 20% 触发拦截

# pending confirm
PENDING_CONFIRM_FILE    = "logs/pending_confirm_state.json"

# ═══════════════════════════════════════════════════════════════════════════════
# 第四组：标的分层阈值
# ═══════════════════════════════════════════════════════════════════════════════
# ATR 比率分层

# 结构回踩
PULLBACK_LOWER_LIMIT_LOW   = 1.2   # 低波动层回踩区间（v3.4.3 放宽，与 judges.py 同步）
PULLBACK_LOWER_LIMIT_MID   = 2.0   # 中波动层回踩区间（v3.4.3 放宽）

# 动量突破
BREAKOUT_RSI_MAX_LONG       = 75    # 做多 RSI 上限，低中  # [fix D] 65→75：避免系统性过滤强势突破
BREAKOUT_RSI_MAX_LONG_HIGH  = 75    # 高波动层  # [fix D] 65→75：与低中档一致
BREAKOUT_RSI_MIN_SHORT      = 35    # 做空 RSI 下限，低中  # [v3.85 FIX-1] 25→35：同步为代码中实际生效值，行为不变
BREAKOUT_RSI_MIN_SHORT_HIGH = 35    # 高波动层  # [v3.85 FIX-1] 同步为实际生效值

# 极端反转
                                    # 不加任何限制（弹性评分），负费率则触发强信号
                                    # 负费率 < -0.3% 为强信号，中性费率为普通信号
# 以下 REVERSAL_RSI4H_* 为历史遗留占位值，judge_reversal 实际用 chg24<=-8%+rsi4h<55 逻辑
# 代码实际值与此不同，调参请直接修改 _judge_reversal_impl 内的门槛
REVERSAL_RSI4H_MAX_LONG       = 55      # 实际用值（原25已废弃，代码用55）
REVERSAL_RSI4H_MIN_SHORT      = 55      # 实际用值（原78已废弃，代码用55）

# 高波动层专属

# 候选池分类（向后兼容旧 scanner 代码）
CLASSIFY_REVERSAL_CHG24_MIN    = 15.0
REVERSAL_CHG24_SHORT_MIN        = 10.0  # [fix C] reversal SHORT 主通道 chg24 门槛（原硬编码 +5.0%）  # 注意：高于判断层8%，8%~14%只走RSI极端通道(rsi_1h<25)，15%+走主通道
                               # 若想让8%~14%走主通道改回8.0，等实盘数据后决定
CLASSIFY_REVERSAL_FUNDING_MIN  = 0.001
REVERSAL_RSI1H_SHORT_MIN       = 65    # 做空主通道 RSI1H 下限
CLASSIFY_REVERSAL_RSI4H_LONG   = REVERSAL_RSI4H_MAX_LONG    # 25
CLASSIFY_REVERSAL_RSI4H_SHORT  = REVERSAL_RSI4H_MIN_SHORT   # 78
CLASSIFY_BREAKOUT_CHG24_MIN    = 5.0   # 8% 降至 5%，成交量验证移到 judge_breakout

# ── final_score 门槛 ──────────────────────────────────────────────────────
MIN_SIGNAL_SCORE_WEEKDAY       = 60    # 工作日最低信号分（50→60），base=strong(70)/medium(50)+btc_adj+cg

# ── 相关性过滤：同方向信号上限 ────────────────────────────────────────────
CORR_MAX_REVERSAL_SAME_DIR     = 2     # 同方向 reversal 最多同时持仓+待开
CORR_MAX_PULLBACK_SAME_DIR     = 3     # 同方向 pullback 最多同时持仓+待开
CORR_MAX_SAME_DIR_TOTAL        = 4     # reversal+pullback 同方向合计上限

# ── 市场情绪感知：全市场下跌比例阈值 ─────────────────────────────────────
MARKET_SENTIMENT_BEAR_THRESH   = 0.70  # 超过70%标的在跌，做多信号仓位降级
MARKET_SENTIMENT_BULL_THRESH   = 0.30  # 超过70%标的在涨，做空信号仓位降级
MARKET_SENTIMENT_SCALE_DOWN    = 0.6   # 降级后仓位系数

# ── 信号有效期动态化：按 ATR ratio 调整 ──────────────────────────────────
EXPIRE_LOW_VOL_H               = 8     # ATR<2% 低波动：等8H
EXPIRE_HIGH_VOL_H              = 2     # ATR>5% 高波动：等2H
EXPIRE_NORMAL_H                = 4     # 正常波动：等4H（原默认）
EXPIRE_LOW_VOL_THRESH          = 2.0   # ATR ratio 低波动阈值
EXPIRE_HIGH_VOL_THRESH         = 5.0   # ATR ratio 高波动阈值
EXPIRE_BREAKOUT_MIN            = 30    # breakout 信号有效期（分钟）
WEEKEND_AMP_PROXY_MIN          = 0.8   # 周末振幅代理最低门槛（%）
WEEKEND_SCORE_CAP              = 85    # 周末评分上限（80→85，保留 hub 加分空间）
MIN_SIGNAL_SCORE_WEEKEND       = 70    # 周末最低信号分（已有逻辑，此处参数化）
# ── v3.92 周末模式实盘改造（scanner._filter_and_register_signals / run_funding_arb_scan 读取）──
WEEKEND_MIN_SCORE_STRONG          = 60   # 周末 strong 信号门槛（维持原值）
WEEKEND_MIN_SCORE_MEDIUM_DEFAULT  = 70   # 周末 medium 默认门槛（reversal/range_bounce/oi_divergence/breakout 等）
WEEKEND_MIN_SCORE_MEDIUM_PULLBACK = 60   # 周末 pullback medium 门槛
WEEKEND_MIN_SCORE_MEDIUM_EMA      = 60   # 周末 ema_trend medium 门槛
WEEKEND_MIN_SCORE_MEDIUM_REVERSAL = 60   # 周末 reversal medium 门槛
WEEKEND_CUTOFF_SUNDAY_UTC_HOUR    = 17   # 周日 UTC 此时起不再注册新信号（主扫描 + funding_arb）
WEEKEND_MAX_NEW_POSITIONS         = 10   # 整个周末累计注册信号上限（两条路径共用，按注册计，不持久化）
WEEKEND_SIGNAL_EXPIRE_CUTOFF_HOUR = 17   # 周日 UTC 信号到期时间不得晚于此（breakout 除外）
WEEKEND_POSITION_SCALE_STRONG     = 0.75 # 周末 strong 仓位上限系数（所有仓位调整之后最后乘）
WEEKEND_POSITION_SCALE_MEDIUM     = 0.50 # 周末 medium 仓位上限系数
WEEKEND_FUNDING_ARB_SCALE         = 0.50 # 周末 funding_arb 仓位上限系数
WEEKEND_BREAKOUT_MEDIUM_VOL_MIN   = 3.0  # 周末 medium breakout 放开所需量比（reason.volume_ratio）
WEEKEND_BREAKOUT_MEDIUM_OI_REQD   = True # True = OI 确认（reason.oi_confirm）也可放开
WEEKEND_EXPIRE_LOW_VOL_H          = 4    # 周末低波动档（atr_ratio<EXPIRE_LOW_VOL_THRESH）有效期 8H→4H
# signal_strength → base_score 映射
SCORE_BASE_STRONG              = 70
SCORE_BASE_MEDIUM              = 50
SCORE_BASE_WEAK                = 30    # weak 信号已在 judge 层过滤，不会到达 score 计算
# 评分附加加分项（读 hub 的 K线/miniTicker；hub 未就绪时该项跳过，不加不减）
SCORE_TAKER_BONUS              = True   # 近6根1h主动买入占比：顺向 +10 / 逆向 -5
SCORE_RANGE_BONUS              = True   # 24h区间位置：顺向 +8 / 逆向 -5
SCORE_VOLPRICE_BONUS           = True   # 放量(近4h/前20h>=1.6)且24h涨跌幅方向上有限：+8
SCORE_HUB_BONUS_CAP            = 12     # hub 三项加分（taker/range/volprice）合计上限
SCORE_HUB_PENALTY_FLOOR        = -12    # hub 三项加分负向下限

# CLASSIFY_BREAKOUT_VOL_RATIO = 1.5  # 已废弃：分类层不再校验量比，移至 judge_breakout

# ═══════════════════════════════════════════════════════════════════════════════
# 第五组：仓位与风控参数
# ═══════════════════════════════════════════════════════════════════════════════
# 持仓上限（按市场模式）
MAX_POSITIONS_TOTAL_BULL  = 8   # 原 5
MAX_POSITIONS_LONG_BULL   = 5   # 原 3
MAX_POSITIONS_SHORT_BULL  = 3   # 原 2
MAX_POSITIONS_TOTAL_BEAR  = 8   # 原 5
MAX_POSITIONS_LONG_BEAR   = 3   # 原 2
MAX_POSITIONS_SHORT_BEAR  = 5   # 原 3
MAX_POSITIONS_TOTAL_RANGE = 8   # 原 4
MAX_POSITIONS_LONG_RANGE  = 4   # 原 2
MAX_POSITIONS_SHORT_RANGE = 4   # 原 2

# 兼容旧代码
RISK_MAX_POSITIONS = MAX_POSITIONS_TOTAL_BULL
RISK_MAX_LONG      = MAX_POSITIONS_LONG_BULL
RISK_MAX_SHORT     = MAX_POSITIONS_SHORT_BULL

# 市场模式判断（BTC 4H EMA）
MARKET_MODE_EMA_FAST = 20
MARKET_MODE_EMA_MID  = 50   # EMA50，bull/bear 结构辅助判断
MARKET_MODE_EMA_SLOW = 99

# 名义仓位上限
MAX_TOTAL_NOMINAL  = 5.0     # 总名义仓位上限 = 账户余额 × 5（v3.36: 3.0→5.0）

# ── 杠杆利用率优化（v3.90）─────────────────────────────────────────────────
# 按多空方向分配名义仓位预算：预算 = 权益 × LEVERAGE_TARGET × 该方向比例；开仓名义被截断到剩余预算。
LEVERAGE_UTILIZATION_ENABLED = True
LEVERAGE_TARGET = 5.0              # 目标总名义杠杆（权益倍数）
MAX_SINGLE_NOMINAL_RATIO = 1.5     # 单笔名义上限 = 权益 × 该值

# 多空名义预算比例（各档多+空=1.0）
LEVERAGE_LONG_RATIO_BULL_EARLY   = 0.75
LEVERAGE_SHORT_RATIO_BULL_EARLY  = 0.25
LEVERAGE_LONG_RATIO_BULL_MID     = 0.70
LEVERAGE_SHORT_RATIO_BULL_MID    = 0.30
LEVERAGE_LONG_RATIO_BULL_LATE    = 0.55
LEVERAGE_SHORT_RATIO_BULL_LATE   = 0.45
LEVERAGE_LONG_RATIO_RANGE        = 0.50
LEVERAGE_SHORT_RATIO_RANGE       = 0.50
LEVERAGE_LONG_RATIO_BEAR_WEAK    = 0.40
LEVERAGE_SHORT_RATIO_BEAR_WEAK   = 0.60
LEVERAGE_LONG_RATIO_BEAR_MODERATE= 0.30
LEVERAGE_SHORT_RATIO_BEAR_MODERATE= 0.70
LEVERAGE_LONG_RATIO_BEAR_STRONG  = 0.20
LEVERAGE_SHORT_RATIO_BEAR_STRONG = 0.80
# 单仓上限公式 v3.1：本金 ÷ 最大仓位数（5倍杠杆，每仓占满 1/N 本金）
# 例：本金1000U / 5仓 = 200U保证金 × 5杠杆 = 1000U名义
# 代码里用 net_value / MAX_POSITIONS_TOTAL_BULL 动态计算，此值仅作兜底上限
MAX_SINGLE_NOMINAL = 1000    # 单仓名义上限（v3.36: 2000→1000，单仓不超1000U）
MIN_NOTIONAL_GUARD = 50.0   # 单仓名义下限（v3.36: 6.0→100.0；v3.96: 100→50，小账户也能开仓）
TAKER_FEE_RATE     = 0.0005  # [fix #44] 无法从成交回报取手续费时的估算用 taker 费率（0.05%）
RISK_PER_TRADE     = 0.03    # 每笔最大风险比例（止损定仓核心参数）  # [fix] 5% → 1%

# 熔断参数
# [fix B] 恢复日内亏损熔断：改为按账户权益比例触发（默认 8%）。
CIRCUIT_BREAKER_DAILY_LOSS_PCT = 0.20   # 当日净亏损超过权益的 8% 触发熔断
CIRCUIT_BREAKER_DAILY_LOSS = 999.0      # 兜底绝对值：取不到账户权益时使用（≈1250U 权益 × 8%）

# 止损距离上限（动态，按波动层级）
# low tier(ATR<3%): 12%  mid tier(ATR 3-8%): 15%  high tier(ATR>8%): 20%
SL_MAX_PCT = 0.12       # 兼容旧代码（low tier 默认值）
SL_MAX_PCT_LOW  = 0.12  # low tier 止损上限
SL_MAX_PCT_MID  = 0.15  # mid tier 止损上限
SL_MAX_PCT_HIGH = 0.20  # high tier 止损上限
TP1_LIQ_ADVANCE_ATR = 0.2   # TP1比清算区上沿提前成交的距离（仅做空方向：缓冲 _tp1_buf - 0.2ATR，下限0）
TP1_MAX_PCT = 0.15      # TP1 距入场价上限（low/mid tier）
TP1_MAX_PCT_HIGH = 0.25 # TP1 距入场价上限（high tier，配合更宽止损保证 RR）

# 动态止损倍数
SL_MULT_LOW  = 1.5   # v3.23: 1.2→1.5，low tier 1.2 ATR 容易被噪声打掉
SL_MULT_MID  = 1.5
SL_MULT_HIGH = 2.0

# 盈亏比要求（复盘优化：益亏比 0.61→2.0+）
RR_MIN_TP1      = 2.0   # TP1 盈亏比要求（low/mid tier）
RR_MIN_TP1_REVERSAL = 1.2   # reversal 专属 TP1 盈亏比要求
                              # 低于通用 2.0，因为 reversal 目标就是清算区，
                              # 强行要求 2.0 会系统性过滤掉最近的清算区
RR_MIN_TP1_HIGH = 1.5   # TP1 盈亏比要求（high tier，SL 较宽故放宽 RR）
RR_MIN_TP2      = 3.0   # TP2 盈亏比要求（原2.5）

# TP 比例
TP1_CLOSE_RATIO = 0.60   # TP1 平仓 60%
TP2_CLOSE_RATIO = 0.40   # TP2 平仓 40%
# TP1_RATIO = TP1_CLOSE_RATIO  # 已废弃，请直接使用 TP1_CLOSE_RATIO
# TP2_RATIO = TP2_CLOSE_RATIO  # 已废弃，请直接使用 TP2_CLOSE_RATIO

# 保证金缓冲
MARGIN_BUFFER = 1.1   # 保证金需求 × 1.1 留手续费空间

# ── USDC 交易对支持 ─────────────────────────────────────────────────────────
ENABLE_USDC_PAIRS = False  # 是否扫描 USDC 保证金交易对（False 时只认 USDT）

# ── 流动性 Gate（v3.57）──────────────────────────────────────────────────────
LIQUIDITY_GATE_MIN_U      = 3_000    # 最小深度硬底 3000U（相对化后降低，OI×0.1% 为动态主阈值）
LIQUIDITY_GATE_DEPTH_MULT = 5        # 深度需 >= 名义仓位 × 5

# ── 净 Delta 监控（v3.57）────────────────────────────────────────────────────
NET_DELTA_MAX_PCT = 0.70   # 净多头/空头 Delta 上限 = 权益 × MAX_TOTAL_NOMINAL × 70%（原 30%）

# ── 全局最大回撤保护（v3.57）────────────────────────────────────────────────
DRAWDOWN_HALT_PCT  = 0.15   # 从高水位回撤 >= 15% → 暂停开仓
DRAWDOWN_HALT_DAYS = 3      # 暂停天数
DRAWDOWN_WARN_PCT  = 0.08   # >= 8% → 警告日志

# ── 保证金安全垫（check_margin_safety）──────────────────────────────────────
MARGIN_SAFETY_RATIO_HALT  = 0.5   # 维持保证金/余额 >= 50% → 禁止开仓
MARGIN_SAFETY_RATIO_TIGHT = 0.3   # >= 30% → 仓位缩减至 SCALE
MARGIN_SAFETY_AVAIL_MIN   = 0.1   # 可用余额/余额 < 10% → 禁止开仓
MARGIN_SAFETY_SCALE_TIGHT = 0.5   # 偏紧时仓位缩减系数

# 杠杆缓存
# LEVERAGE_CACHE_TTL 已删除 —— 代码从未引用此常量，避免误导。

# 执行参数（向后兼容）
DEFAULT_LEVERAGE = 10

# 挂单超时
ORDER_EXPIRE_BREAKOUT_MIN = 30    # 动量突破挂单超时，分钟
BREAKOUT_OI_GAIN_MIN      = 1.0   # breakout OI 15m 涨幅加分门槛（低/中波动，%）
BREAKOUT_OI_GAIN_MIN_HIGH = 3.0   # breakout OI 15m 涨幅加分门槛（高波动，%）
BREAKOUT_MAX_CHASE_ATR = 2.0      # 突破后最多追入2ATR，超过不开仓
ORDER_EXPIRE_PULLBACK_MIN = 240   # 回踩/反转挂单超时，分钟

# 扫描超时保护
SCAN_SYMBOL_TIMEOUT_SEC   = 10    # 单标的分析超时（秒），超时跳过
SCAN_TOTAL_TIMEOUT_SEC    = 180   # 整轮扫描总超时（秒）；v3.26: 240→120，并发25线程理论8~12s
SCAN_MAX_WORKERS          = 2     # [2核服务器收紧 4→2] 并行分析线程数（10→4：降低 GIL 竞争导致事件循环阻塞）；v3.82: 25→10，控制并发内存峰值（25线程×11MB=275MB峰值，pymalloc不归还导致水位积累至3.5GB）
FUNDING_ARB_SCAN_WORKERS = 2  # funding_arb并发扫描线程数，降低SSL并发防止阻塞事件循环
SCAN_USE_PROCESS_POOL     = False  # True：并行分析走 spawn 进程池（shared/analysis_worker.py）；False：保持线程池（默认）
SCAN_KLINE_TIMING       = True  # 是否统计 K 线请求耗时（每轮 info 一条汇总），生产环境建议开启
KLINE_CACHE_ENABLED     = True  # 是否启用 K 线 REST 缓存（按收盘时间失效；False 时每轮直接 REST）
KLINE_CACHE_INTERVALS   = ["1h", "4h", "15m"]  # 缓存哪些周期
KLINE_REQUEST_TIMEOUT   = (3, 5)  # (connect_s, read_s)：K 线单次 HTTP 请求超时
KLINE_REQUEST_MAX_RETRY = 1       # K 线请求最多重试次数（总尝试 = 1 + 此值）
MARKET_DATA_HUB_ENABLED = True   # 公共市场数据 WS 中心（阶段一：全市场标记价/资金费率）；False 时行为与未引入 hub 前完全一致；hub 未就绪/过旧时 scanner 自动回落 REST
HUB_LOAD_MAX_WORKERS = 2  # hub历史K线加载并发上限
# ── 启动分批初始化（避免重启瞬间并发大量 REST 触发 Binance -1003 限频/IP 封禁）──
# P1 立即：get_account 校验 + 持仓模式；P2：持仓/挂单对齐；P3：OI 缓存 + 系统性风险；P4：hub K 线订阅/清算区预热/扫描等非关键初始化
STARTUP_BATCH_DELAY_P2 = 5    # P1 → P2 等待秒数
STARTUP_BATCH_DELAY_P3 = 8    # P2 → P3 等待秒数
STARTUP_BATCH_DELAY_P4 = 12   # P3 → P4 等待秒数
HUB_NO_MSG_TIMEOUT = 180  # 120→180：给历史加载留余量，减少阻塞引发的假断线； WebSocket 无消息超时（秒），过短会因GIL饿死误触发重连
KLINE_FETCH_CONCURRENCY = 2  # hub历史K线REST拉取并发上限
API_418_COOLDOWN_SEC = 60  # 收到 Binance 418 后全局暂停所有 REST 请求的秒数
HUB_KLINE_FETCH_TIMEOUT = 8.0  # hub 单次历史K线拉取总时限（秒），超时按空结果处理、30s 后重试
LIQ_PREFETCH_MAX_WORKERS = 2   # 预热线程数；8 线程实测加剧 GIL 竞争，事件循环反复阻塞 2~4s，回退到 4
LIQ_PREFETCH_SLEEP_SEC   = 0.05 # 每个标的预热后的节流间隔（秒），50ms 防止 API 429 限速
LIQ_PREFETCH_FETCH_TIMEOUT_SEC = 5.0  # 清算图预热单次取数的等待上限（秒）：在独立线程里取，超时直接跳过且晚到结果不写缓存；<=0 关闭超时（旧行为）
LIQ_PREFETCH_MAX_SYMBOLS = 30            # 后台预热的候选池上限（评分前 N 名）；其余标的不预热，用到时实时拉，拉不到走 ATR 降级
LIQ_PREFETCH_PRIORITY_POSITIONS = True   # 持仓标的优先且必预热（不受上限限制）；False 时回退为旧行为（全池预热）
LIQ_PRESCAN_PREFETCH_ENABLED     = False  # 扫描前同步预热（已改为后台持续预热，扫描只读缓存；True=恢复旧行为，扫描会等 Clawby）
LIQ_SCAN_CACHE_ONLY              = True   # 扫描分析阶段的清算区只读缓存、不发网络请求（缓存缺失按无清算区降级，并登记后台优先预热）
LIQ_SCAN_STALE_MAX_SEC           = 2400.0 # 扫描允许使用的缓存最大年龄（秒，约两个预热周期）；超过按缺失处理
LIQ_BG_REFRESH_INTERVAL_SEC      = 600.0  # 后台预热线程每轮间隔（秒），只请求已过 12 分钟的标的
LIQ_PRESCAN_PREFETCH_TIMEOUT_SEC = 20.0   # 扫描前预热的最长等待（秒），超时直接进入分析（直连后 8 线程足以跑完）
KLINE_LOAD_BATCH_SIZE = 200  # hub 每次对账最多新建的历史K线加载任务数（超出留到下一轮，每 2s 一轮）
KLINE_GAP_RELOAD_COOLDOWN_SEC = 60  # 同一 series 因K线缺口触发重拉的冷却时间（秒）
HUB_WS_PROXY = ""  # hub WebSocket 代理，空字符串=直连，"__env__"=跟随环境变量 PROXY_URL
KLINE_HUB_SYMBOLS_MAX = 165  # 对应候选池上限 SCAN_POOL_MAX=165；连接数=ceil(165/60)=3 条，共 495 个 stream（单连接上限 KLINE_STREAM_PER_CONN=180）
KLINE_REQUEST_TOTAL_SEC = 8.0     # 单次 get_klines 调用总时限（秒，含重试、重试间隔、直连失败后改走代理）
API_REQUEST_TIMEOUT     = (3, 5)    # 全局 REST 接口超时 (connect_s, read_s)：公开行情 _get 与签名查询/撤单/杠杆等
API_REQUEST_MAX_RETRY   = 1         # 全局 REST 接口最多重试次数（_retry 总尝试 = 1 + 此值）
API_SLOW_LOG_SEC        = 3.0       # [慢请求诊断] _retry 单次调用总耗时(含重试/退避)超过该秒数记 warning，便于看慢请求分布
PATROL_REST_POOL_WORKERS = 2        # patrol 路径读请求专用线程池大小（避免慢请求占满默认 executor）
PATROL_REST_TIMEOUT_SEC  = 10.0     # patrol 读请求在池内的等待上限（秒）；超时按「查询失败」处理，本轮跳过
ORDER_REQUEST_TIMEOUT   = (5, 10)   # 下单类接口（order / batchOrders / algoOrder）超时，比查询宽松

# ═══════════════════════════════════════════════════════════════════════════════
# 第六组：持仓管理参数
# ═══════════════════════════════════════════════════════════════════════════════
# 动态止损跟随阶梯
# 动态止损跟随阶梯（趋势类：回踩/突破/OI背离/EMA趋势）。格式：(浮盈ATR倍数, 止损移到入场价+偏移ATR倍数)
# [v3.84 优化] 原第一档 (0.8, 0.0) = 浮盈 0.8ATR 就把止损挪到「恰好入场价」。
#   问题：入场价无缓冲，手续费+滑点+正常回撤(0.5~1ATR)使「保本止损」被噪声扫出，
#         变成「赚了一点又被打回 0」的零收益止损，抬高止损次数、压低胜率统计。
#   新：第一档推迟到 1.0ATR，且止损放在入场价 +0.1ATR（覆盖往返手续费与滑点），其余档整体后移 0.3ATR 左右，
#       给趋势更多回撤空间。开关 OPT_TRAIL_BE_BUFFER 关闭即回到原阶梯。
TRAIL_STEPS_TREND_LEGACY = [
    (0.8, 0.0),
    (1.5, 0.5),
    (2.5, 1.0),
    (4.0, 1.5),
    (6.0, 2.0),
]
OPT_TRAIL_BE_BUFFER = True   # v3.84：保本档推迟并加缓冲（False=原阶梯）  # [v3.87 合并决策] 默认 False：OPT-4 按评审：先关闭，观察单笔最大回撤后再开
TRAIL_STEPS_TREND = [
    (2.0, 0.5),   # 第一档放宽：浮盈 2ATR 才保本，SL 设在入场价±0.5ATR（原 (1.0, 0.1)；已删除重叠的 (1.8, 0.5)）
    (2.8, 1.0),
    (4.0, 1.5),
    (6.0, 2.0),
] if OPT_TRAIL_BE_BUFFER else TRAIL_STEPS_TREND_LEGACY
# Trail SL 的 MFE 锚点模式（默认关闭=沿用上面的入场价阶梯 TRAIL_STEPS_TREND/REVERSAL，逻辑完全不变）
TRAIL_MFE_ANCHOR    = False   # True=MFE锚点，False=入场价锚点（原逻辑）
TRAIL_MFE_ATR_MULT  = 2.5     # MFE锚点的ATR倍数（软止损：1H收盘价触及才平仓）
TRAIL_MFE_HARD_MULT = 3.5     # 盘中硬止损的ATR倍数（交易所挂单，随 MFE 单向移动）
TRAIL_STEPS_REVERSAL = [
    (0.5, 0.0),
    (1.0, 0.5),
    (2.0, 1.0),
    (3.5, 1.5),
]

# 健康巡检
HEALTH_CHECK_INTERVAL       = 6    # 每几次 5 分钟巡检执行一次健康检查
HEALTH_FORCE_CLOSE_SCORE    = 50   # 双条件门槛：评分<50 且 浮亏>阈值才强制平仓（单独低分只告警）
HEALTH_FORCE_CLOSE_LOSS_PCT = -5.0 # 双条件之二：浮亏超过-5%才触发强制平仓（评分<50仅告警时用）
HEALTH_ALERT_SCORE          = 65   # 低于此分发告警（不平仓）
HEALTH_MIN_HOLDING_MINUTES  = 60

# 动态 TP/SL 调整
TP_SL_ADJUST_INTERVAL_MIN   = 30    # 每隔多少分钟重算一次 TP/SL（静默，写日志）
TP_SL_ADJUST_MIN_HOLD_MIN   = 15    # 开仓后至少持仓多久才开始动态调整（避免刚开仓就改）
TP_SL_ADJUST_TP_SHRINK_ATR  = 0.5   # TP 下调时：新 TP 比旧 TP 差超过此 ATR 倍数才更新（防噪音）
TP_SL_ADJUST_SL_WIDEN_ATR   = 0.3   # SL 外扩时：新 SL 比旧 SL 宽超过此 ATR 倍数才更新（防频繁外扩）
TP_SL_ADJUST_LOG            = _FX_ROOT / "logs" / "tp_sl_adjust_log.jsonl"

# 时间止损
TIME_STOP_HOURS            = 8    # breakout/pullback 时间止损
TIME_STOP_REVERSAL_HOURS   = 12   # reversal 类型允许更长持仓时间
TIME_STOP_HARD_HOURS       = 16   # 硬上限（所有类型）
TIME_STOP_PROFIT_THRESHOLD = 2.5   # 浮盈超过 ATR×2.5 不触发时间止损

# 挂单有效期（向后兼容别名）
PM_MAX_HOLD_HOURS        = TIME_STOP_HOURS
PM_HARD_MAX_HOLD_HOURS   = TIME_STOP_HARD_HOURS
PM_HEALTH_CHECK_INTERVAL = HEALTH_CHECK_INTERVAL
PM_HEALTH_FORCE_CLOSE_SCORE    = HEALTH_FORCE_CLOSE_SCORE
PM_HEALTH_FORCE_CLOSE_LOSS_PCT = HEALTH_FORCE_CLOSE_LOSS_PCT

# ── v3.68 改动二：TP 收紧后 RR < 1.0 主动平仓 ──────────────────────────────
TP_MIN_RR_LIVE = 1.0  # 持仓期间 RR 最低门槛，低于此值主动平仓

# ── v3.69 挂单存活 RSI 门槛参数化（_survival_check 使用）───────────────────
# reversal SHORT 撤单条件：超买消退（RSI 回落到此值以下）
SURVIVAL_RSI_SHORT_CANCEL = 60
# reversal LONG 撤单条件：超卖消退（RSI 回升到此值以上）
# 设为 45 而非 40：触发门槛 RSI < 28，反弹到 40 只是正常复苏初期，过早撤单
# 对称逻辑：28（触发）× 1.6 ≈ 45；SHORT 侧 72（触发）× 0.83 ≈ 60
SURVIVAL_RSI_LONG_CANCEL = 45

# ── v3.68 改动三：健康检查按持仓时间动态调整强平门槛 ──────────────────────────
PM_HEALTH_EARLY_MULT  = 0.5  # 持仓 < 2H 的门槛乘数（放宽一倍）
PM_HEALTH_MID_MULT    = 1.0  # 持仓 2~6H（标准）
PM_HEALTH_LATE_MULT   = 1.5  # 持仓 > 6H（收紧 50%）
PM_HEALTH_EARLY_HOURS = 2    # 早期/中期分界（小时）
PM_HEALTH_LATE_HOURS  = 6    # 中期/晚期分界（小时）

# ── v3.72 挂单竞争机制 ──────────────────────────────────────────────────────
# 挂单槽位（独立于 open 持仓槽位）
MAX_PENDING_LONG  = 5   # 多单挂单上限（原 3）
MAX_PENDING_SHORT = 5   # 空单挂单上限（原 3）
MAX_PENDING_TOTAL = 8   # 总挂单上限（原 5）

# 评分维度

# 策略类型分值映射
PENDING_SCORE_TYPE_MAP = {
    "reversal":     20,
    "breakout":     15,
    "stop_hunt":    15,
    "pullback":     12,
    "oi_divergence":12,
    "ema_trend":    10,
    "range_bounce":  8,
    "funding_arb":   5,
}

# 竞争门槛
PENDING_MIN_SCORE_TO_COMPETE = 40  # 低于此分直接丢弃，不参与竞争
PENDING_REPLACE_MARGIN       = 8   # 新信号需高出被替换信号的最小分差
PENDING_SCORE_ABANDON        = 30  # 低于此分主动撤单释放槽位（时效耗尽）
PM_HEALTH_WARN_SCORE           = HEALTH_ALERT_SCORE
PM_MIN_HOLD_BEFORE_HEALTH      = HEALTH_MIN_HOLDING_MINUTES
PM_BREAKOUT_FAIL_WINDOW     = 30   # 分钟
PM_COOLDOWN_SL_HOURS        = 6
PM_COOLDOWN_CRASH_HOURS     = 4
PM_COOLDOWN_OPEN_DIR_HOURS  = 4  # v3.44: 同标的同方向成功开仓后冷却时长（与止损冷却独立）

# 止损冷却

# 防抖
PRICE_DEBOUNCE_SEC = 1.0

# ═══════════════════════════════════════════════════════════════════════════════
# 第七组：文件路径
# ═══════════════════════════════════════════════════════════════════════════════
POS_STATE_FILE        = _FX_ROOT / "logs" / "pos_state.json"
PENDING_CONFIRM_FILE  = _FX_ROOT / "logs" / "pending_confirm_state.json"
TRADE_LOG_FILE        = _FX_ROOT / "logs" / "trade_log.jsonl"

# ── 钉钉推送（加签机器人；Jarvis 之外的额外副本，只 notify/notify_async 发，卡片不发）──
# 服务器需设置环境变量 DINGTALK_WEBHOOK / DINGTALK_SECRET；未设置则静默跳过
DINGTALK_ENABLED = True
DINGTALK_WEBHOOK = os.environ.get("DINGTALK_WEBHOOK", "")
DINGTALK_SECRET  = os.environ.get("DINGTALK_SECRET", "")
SCANNER_LOG_FILE      = _FX_ROOT / "logs" / "scanner.log"

# 日报时间（UTC 小时）
DAILY_REPORT_UTC_HOUR = TELEGRAM_DAILY_REPORT_HOUR  # 2

# 信号排序优先级（越小越优先）
SIGNAL_PRIORITY      = {"reversal": 0, "stop_hunt": 0, "breakout": 1, "pullback": 2}
SIGNAL_STRENGTH_ORDER = {"strong": 0, "medium": 1, "weak": 2}  # 信号强度排序权重

# 前置过滤（向后兼容旧 scanner 代码）
FILTER_MAX_FUNDING_RATE     = 0.01
FILTER_OI_DIVERGENCE_PRICE  = OI_DIVERGENCE_PRICE_PCT
FILTER_OI_DIVERGENCE_DROP   = OI_DIVERGENCE_DROP_PCT
FILTER_MIN_LIQUIDITY_USD    = UNIVERSE_MIN_VOLUME_24H
ORDER_CANCEL_BTC_MOVE_PCT   = BTC_SHOCK_THRESHOLD * 100
ORDER_CANCEL_FUNDING_DELTA  = 0.002
ORDER_CANCEL_OI_DROP_PCT    = 15.0
ORDER_MAINTAIN_EMA_CHANGE   = True
ORDER_MAINTAIN_LIQ_ATR_MULT = 1.0
POOL_MIN_VOLUME_USD         = UNIVERSE_MIN_VOLUME_24H
POOL_MAX_CHG24_PCT          = 50.0         # 24H 涨跌幅绝对值上限 50%（原 UNIVERSE_MAX_CHANGE_24H=60% 与 SCAN_POOL_EXCLUDE_VOLATILE 开关已合并删除）
POOL_STABLE_COINS           = set(UNIVERSE_BLACKLIST)
POOL_VOLUME_TOP_N           = UNIVERSE_VOLUME_TOP
POOL_GAINERS_N              = UNIVERSE_GAIN_TOP
POOL_LOSERS_N               = UNIVERSE_LOSS_TOP
POOL_FUNDING_EXTREME_N      = UNIVERSE_FUNDING_TOP
POOL_LIQ_TOP_N              = 30          # CG 清算量 Top N（新来源）
LIQ_BIAS_MIN_TOTAL_1H       = 10000       # coin-list 1h 多空失衡：最低总清算量门槛（USD），低于视为噪音
UNLOCK_FILTER_ENABLED       = False       # 代币解锁风险过滤总开关。False=只记录日志不拒绝（先观察）；True=命中则拒绝 LONG
UNLOCK_FILTER_DAYS          = 7           # 窗口：UTC 日历天，今天起 N 天内（含今天）有解锁
UNLOCK_FILTER_PCT           = 1.0         # 阈值：下次解锁占流通量百分比（接口 next_unlock_of_circulating 是百分数，1.34=1.34%）
LIQ_BIAS_MIN_RATIO          = 3.0         # coin-list 1h 多空失衡：多空清算量最低比例（大/小，小侧 +1 防除零）
POOL_FUNDING_EXTREME_MIN    = UNIVERSE_FUNDING_MIN_ABS
SCAN_POOL_MAX               = 165          # 200 → 165，对齐理论最大来源数，让评分排序真正生效
                                           # 配额：成交额 50 + 涨幅 30 + 跌幅 30 + 费率极端 25 + 清算量 30 = 165
SCAN_POOL_MIN_OI_U          = 1_000_000    # 已停用：_pool_filter_by_oi 在 a8b87e1d 停用，此参数保留供恢复时使用
# JUDGE_PULLBACK_PARAMS — 已废弃，judges 直接使用 config 参数
# JUDGE_PULLBACK_PARAMS = {}
# JUDGE_BREAKOUT_PARAMS — 已废弃
# JUDGE_BREAKOUT_PARAMS = {}
# JUDGE_REVERSAL_PARAMS — 已废弃
# JUDGE_REVERSAL_PARAMS = {}
BREAKOUT_ORDER_EXPIRE_MIN   = ORDER_EXPIRE_BREAKOUT_MIN


# ═══════════════════════════════════════════════════════════════════════════════
# 第八组：SignalWatcher 参数
# ═══════════════════════════════════════════════════════════════════════════════
# 信号入场价格监控（shared/signal_watcher.py）
SIGNAL_WATCH_BREAKOUT_EXPIRE_MIN  = 30     # breakout 信号有效期，分钟
SIGNAL_WATCH_PULLBACK_EXPIRE_MIN  = 240    # pullback/reversal 信号有效期，分钟
SIGNAL_WATCH_GC_INTERVAL_SEC      = 60     # GC 间隔，秒
SIGNAL_WATCH_ENTRY_BUFFER_PCT_PULLBACK  = 0.030   # pullback 触发区间 ±3%（山寨行情1分钟3%属正常，提前埋单）
SIGNAL_WATCH_ENTRY_BUFFER_PCT_REVERSAL  = 0.030   # reversal 触发区间 ±3%（v3.91 起 reversal 改用下方 ATR 半宽，此值不再用于 reversal）
SIGNAL_WATCH_ENTRY_BUFFER_ATR_REVERSAL = 0.3  # reversal 触发区间半宽，单位 ATR（原 0.5）
# reversal 有两处降级路径会产生 entry = mark ± 0.5ATR：
#   ① calc_sl_tp._entry_reversal 挂单价过远降级
#   ② signal_watcher recheck 清算区位移重挂
# 触发半宽必须 < 0.5ATR，否则降级后 mark 价在区间边界上立刻秒触发。
# 0.3ATR 留出 0.2ATR 余量，1.2ATR 降级路径本身没有冲突。
SIGNAL_WATCH_ENTRY_BUFFER_PCT_BREAKOUT  = 0.010   # breakout 触发区间 ±1%（追涨不宜太宽）

# ── scanner.py 使用的别名（与上方参数对应）────────────────────────────────────
FUNDING_WINDOW_NORMAL  = FUNDING_WINDOW_NORMAL_BEFORE   # 普通费率结算前窗口
FUNDING_WINDOW_EXTREME = FUNDING_WINDOW_EXTREME_BEFORE  # 极端费率结算前窗口
FUNDING_EXTREME_THRESH = FUNDING_EXTREME_THRESHOLD      # 极端费率门槛


# ═══════════════════════════════════════════════════════════════════════════════
# 第九组：funding_arb 资金费率套利场景
# ═══════════════════════════════════════════════════════════════════════════════
FUNDING_ARB_ENABLED              = True
FUNDING_ARB_MAX_POSITIONS        = 1      # 最大同时持仓数（独立槽位）
FUNDING_ARB_MAX_HOLD_HOURS       = 16     # 最大持有时间（小时）
FUNDING_ARB_SL_ATR_MULT          = 1.0   # 止损 ATR 倍数（v3.25: 1.5→1.0，套利单比趋势单更紧）
FUNDING_ARB_SIZE_STRONG          = 0.5   # strong 信号仓位系数
FUNDING_ARB_SIZE_MEDIUM          = 0.3   # medium 信号仓位系数
FUNDING_ARB_COOLDOWN_AFTER_SL    = 4     # 止损后冷却小时数
FUNDING_ARB_CONSEC_SL_LIMIT      = 2     # 连续止损次数触发熔断（暂停24H）
FUNDING_ARB_CANDIDATE_TOP_N      = 10    # 候选池大小

# 做空（正费率极端）门槛
FUNDING_ARB_MIN_RATE_SHORT       = 0.001    # 触发门槛
FUNDING_ARB_STRONG_RATE_SHORT    = 0.002    # strong 门槛
FUNDING_ARB_RATE_HIST_MIN_SHORT  = 0.0005   # 近3期均值门槛

# 做多（负费率极端）门槛
FUNDING_ARB_MIN_RATE_LONG        = -0.001   # 触发门槛（负值）
FUNDING_ARB_STRONG_RATE_LONG     = -0.002   # strong 门槛（负值）
FUNDING_ARB_RATE_HIST_MIN_LONG   = -0.0005  # 近3期均值门槛（负值）

# 退出判断参数
FUNDING_ARB_RATE_DECAY_THRESH    = 0.80   # 费率衰减超过80%触发退出
FUNDING_ARB_ENTRY_WINDOW_MIN_H   = 0.083  # 距结算最少时间（5分钟），改用限价单后可以临近结算才挂
# FUNDING_ARB_ENTRY_WINDOW_MAX_H 已废弃，动态上限 = 结算周期×90%（judge_funding_arb 内部计算）
FUNDING_ARB_ENTRY_WINDOW_MAX_H   = 99     # 占位，实际不用此值  # [v3.87 DOC] 占位值，不接入任何业务逻辑，修改无效（刻意保留，勿当作可调参数）
FUNDING_ARB_RSI4H_SHORT_MIN      = 50     # 做空 RSI4H 下限
FUNDING_ARB_RSI4H_SHORT_MAX      = 80     # 做空 RSI4H 上限
FUNDING_ARB_RSI4H_LONG_MIN       = 20     # 做多 RSI4H 下限
FUNDING_ARB_RSI4H_LONG_MAX       = 50     # 做多 RSI4H 上限
FUNDING_ARB_OI_DROP_LIMIT        = 0.10   # OI 下降超过10%拒绝入场
FUNDING_ARB_TREND_FILTER         = True   # 是否启用 EMA 趋势过滤（不接飞刀）
FUNDING_ARB_AMPLITUDE_LIMIT      = 30.0   # 近20H振幅上限（%），超过拒绝
FUNDING_ARB_PULSE_FILTER_ENABLED = True   # 是否启用脉冲大阳保护

# v3.37 funding_arb 续持门槛与动态 RSI4H 门槛
FUNDING_ARB_CONTINUE_THRESH     = 0.005   # 续持最低费率门槛（0.5%），低于此平仓
FUNDING_ARB_RSI4H_EXTREME       = 0.01    # 极端费率门槛（1%），RSI4H 放宽到 70/30
FUNDING_ARB_RSI4H_HIGH          = 0.005   # 较高费率门槛（0.5%），RSI4H 放宽到 60/40


# ═══════════════════════════════════════════════════════════════════════════════
# 第十组：每日归档（archiver）
# ═══════════════════════════════════════════════════════════════════════════════
ARCHIVE_ENABLED      = True
ARCHIVE_GIT_PUSH     = True                            # 是否自动 push（需要 SSH key）
ARCHIVE_DIR          = _FX_ROOT / "archive" / "trades"

# ═══════════════════════════════════════════════════════════════════════════════
# 第十三组：持仓中反向信号主动平仓（exit_signal）
# ═══════════════════════════════════════════════════════════════════════════════
EXIT_SIGNAL_ENABLED           = True
EXIT_SIGNAL_RSI_LONG_THRESH   = 28    # 做空持仓：RSI1H < 28 触发反向信号检查
EXIT_SIGNAL_RSI_SHORT_THRESH  = 72    # 做多持仓：RSI1H > 72 触发反向信号检查
EXIT_SIGNAL_MIN_PROFIT_RATIO  = 0.5   # v3.40: 0.8→0.5，价格走到TP1一半时允许反向信号触发平仓（0.8几乎永不触发）
EXIT_SIGNAL_MIN_HOLD_MIN      = 30    # 持仓至少 30 分钟后才检查

# ═══════════════════════════════════════════════════════════════════════════════
# 第十四组：脉冲大阳保护（pulse_candle）— 拦截庄家拉盘后做空
# ═══════════════════════════════════════════════════════════════════════════════
PULSE_CANDLE_VOL_RATIO_1H  = 3.0   # 1H 成交量超过前5根均量的倍数门槛
PULSE_CANDLE_VOL_RATIO_4H  = 4.0   # 4H 成交量超过前5根均量的倍数门槛
PULSE_CANDLE_BODY_CHG_1H   = 5.0   # 1H K线实体涨幅门槛（%）
PULSE_CANDLE_BODY_CHG_4H   = 8.0   # 4H K线实体涨幅门槛（%）
PULSE_CANDLE_BODY_RATIO    = 0.5   # 实体占振幅最低比例（排除插针型）
PULSE_CANDLE_LOOKBACK_1H   = 2     # 1H 向前看几根已闭合K线
PULSE_CANDLE_LOOKBACK_4H   = 3     # 4H 向前看几根已闭合K线

# ═══════════════════════════════════════════════════════════════════════════════
# 第十五组：pullback SHORT 弱信号过滤（v3.19）
# 目的：拦截横盘中信号强度不足的空单（如 BTW chg24=-1.3% vol_ratio=0.85 这类）
# 仅用于 pullback SHORT，reversal/breakout 有自己的信号强度校验
# ═══════════════════════════════════════════════════════════════════════════════
PULLBACK_SHORT_CHG24_MIN   = 3.0   # SHORT pullback 要求 24H 涨幅至少 +3%（标的需有过拉升才值得做空回踩）
PULLBACK_SHORT_VOL_RATIO_MIN = 0.8  # SHORT pullback 量比最低门槛（量比过低说明市场无兴趣）

# ═══════════════════════════════════════════════════════════════════════════════
# 第十一组：挂单健康检查（pending health）
# ═══════════════════════════════════════════════════════════════════════════════
PENDING_HEALTH_MIN_HOLD_MIN        = 10    # 挂单多久后才开始检查
PENDING_HEALTH_ENTRY_ADJUST_ATR    = 0.2   # 入场价变化超过几个ATR才重挂
PENDING_HEALTH_MAX_ADJUST_COUNT    = 3     # 最多调整几次（超出直接撤单）
PENDING_HEALTH_LOG  = _FX_ROOT / "logs" / "pending_adjust_log.jsonl"

# ═══════════════════════════════════════════════════════════════════════════════
# 第十二组：AI 参数顾问（ai_advisor）
# ═══════════════════════════════════════════════════════════════════════════════
AI_ADVISOR_ENABLED          = True
AI_ADVISOR_LOOKBACK_DAYS    = 7        # 读取最近 N 天的 archive 数据
AI_ADVISOR_LOG              = _FX_ROOT / "logs" / "advisor_log.jsonl"

# Telegram 推送配置（复用主配置里的 bot/chat）
# 如无独立配置，ai_advisor 直接使用 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID

# 当前 config 参数快照（供 AI 参考，手动同步关键参数名）
AI_ADVISOR_PARAM_KEYS = [
    "ATR_RATIO_MIN", "SL_MULT_LOW", "SL_MULT_MID", "SL_MULT_HIGH",
    "SL_MAX_PCT", "SL_MAX_PCT_LOW", "SL_MAX_PCT_MID", "SL_MAX_PCT_HIGH",
    "TP1_RR_MIN", "TP1_MAX_PCT", "TP1_MAX_PCT_HIGH", "MAX_SL_PCT",
    "PULLBACK_OI_MULTIPLIER_THRESHOLD", "BREAKOUT_OI_CHANGE_MIN",
    "REVERSAL_CHG24_SHORT_MIN", "REVERSAL_RSI4H_SHORT_MIN",
    "ORDER_EXPIRE_PULLBACK_MIN", "ORDER_EXPIRE_BREAKOUT_MIN",
    "ORDER_MAINTAIN_LIQ_ATR_MULT",
]


# ═══════════════════════════════════════════════════════════════════════════════
# v3.41 LLM 辅助判断开关（fx/shared/llm_judge.py）
# ═══════════════════════════════════════════════════════════════════════════════
LLM_JUDGE_ENABLED          = True   # 总开关：False 时所有 LLM 调用全部跳过
LLM_JUDGE_EXPLOSIVE_COIN   = False  # 场景1：妖币识别（3日涨>100% 但 24H 横盘）  # [v3.87 STEP2-4] 按评审关闭：该分支会把已通过的信号强制改成 range_bounce LONG（SHORT→LONG 翻转），无历史胜率支撑且依赖外部大模型实时调用；有数据后再评估是否重新开启
LLM_JUDGE_BORDERLINE       = False   # 场景2：规则边界信号（final_score 45~55）
LLM_JUDGE_NEWS             = True   # 场景3：新闻 caution 语义深度解读
LLM_JUDGE_MIN_CONFIDENCE   = 0.65   # LLM 最低置信度门槛，低于此视为不确定→跳过

# 妖币识别阈值
LLM_EXPLOSIVE_GAIN_3D_MIN  = 100.0  # 3日涨幅超过此值（%）才触发妖币检测
LLM_EXPLOSIVE_GAIN_24H_MAX = 10.0   # 24H涨幅绝对值低于此值（%）才认为是横盘

# 边界信号阈值（工作日门槛 MIN_SIGNAL_SCORE_WEEKDAY=60）——区间同步上移 10 分：55~65
# 注意：LLM_JUDGE_BORDERLINE 默认 False，此时 55~59 分直接拒绝，不走 LLM 复核
LLM_BORDERLINE_SCORE_MIN   = 55     # 边界信号分数下界
LLM_BORDERLINE_SCORE_MAX   = 65     # 边界信号分数上界

# NovaAPI 配置（OpenAI 兼容中转站，路径A优先使用）
# key 通过环境变量注入，不硬编码；env 优先，config 兜底

# 模型选择说明（v3.41.3，基于 NovaAPI 实测 2026-10-18）：
#   测试了 GPT-4o / GPT-4o-mini / GPT-4.1 / GPT-4.1-mini /
#          Claude Sonnet 4.6 / Opus 4.7 Med / Opus 4.8 Med / gpt-5-mini
#
#   关键发现：GPT-4o 在「负费率」场景有特定误判——
#     无注释 prompt → d=open（把负费率当多头支撑）❌
#     手动加「(空头拥挤)」注释 → d=skip ✅
#     Claude Opus 4.8 Med 不依赖注释，原始数字就能判对 ✅
#
#   结论：分场景配置更稳健
#   - 妖币识别：claude-opus-4-8-medium（GPT系负费率场景有固有误判）
#   - 边界信号：claude-opus-4-8-medium（最关键场景，保守稳健）
#   - 新闻语义：claude-sonnet-4-6（逻辑简单，Sonnet够用，速度稍快）
#   - gpt-5-mini：NovaAPI 上 reasoning_tokens 全内部消耗，output 为空，不可用
LLM_API_MODEL              = "claude-opus-4-8-medium"  # 默认（妖币+边界信号）
LLM_API_MODEL_FAST         = "claude-sonnet-4-6"        # 新闻语义（逻辑简单，够用）



# ── 熊市空单费率门槛（v3.41.1）─────────────────────────────────────────────────
# 熊市顺势空单：费率 >= 此值才允许。负值=空头付钱给多头（空头过拥挤警告）
# -0.0005 (-0.05%) 过严：-0.05% 空头每天付 0.15%，7天 1.05%，不算极端
# -0.0015 (-0.15%) 合理：-0.15% 空头每天付 0.45%，7天 3.15%，开始影响盈亏
# 真正极端 (-0.3%+) 才应该完全禁止，日报会记录被拦截的具体标的供人工复查
BTC_BEAR_SHORT_MIN_FUNDING = -0.0015   # 熊市顺势空单最低费率门槛（默认 -0.15%）


# ── 动态波动率分档参数（v3.46：替代固定 MAJOR_COINS 列表）────────────────────
# 完全数据驱动，无需维护标的列表，ATR ratio 实时决定当前所在档位
ATR_TIER_HIGH_MIN            = 2.5    # 高波动档下限（meme / 山寨大涨期）
ATR_TIER_MID_MIN             = 1.0    # 中波动档下限（主流 / 新兴币）
EMA_TREND_ATR_MIN           = 0.8    # ema_trend 专用 ATR 比例下限(%)；不再共用 ATR_TIER_MID_MIN(1.0)，其他策略不受影响

ATR_TIER_MID_SCORE_ADJ       = -5     # 中波动档评分惩罚（不确定性更高）

# reversal chg24 触发门槛（各档）
ATR_TIER_MID_REVERSAL_CHG24  =  5.0   # 中波动档：SOL 跌 5% 就是大跌

# pullback SHORT 最低涨幅要求（各档）
ATR_TIER_MID_PULLBACK_CHG24  =  2.0   # 中波动档：主流币涨 2% 就有意义

# 周末各档 ATR 门槛
ATR_TIER_HIGH_WEEKEND        =  1.5   # 高波动档周末放宽：现有逻辑不变
ATR_TIER_MID_WEEKEND         =  0.8   # 中波动档周末放宽

# ═══════════════════════════════════════════════════════════════════════════════
# v3.49 Stop Hunt Reversal 策略参数
# ═══════════════════════════════════════════════════════════════════════════════

# 前高/前低识别窗口（1H 已闭合 K 线数）
STOP_HUNT_LOOKBACK_1H        = 20      # 取最近 N 根 1H 闭合 K 线寻找前高/前低
STOP_HUNT_CONFIRM_BARS       = 3       # 极值点左右各需至少 N 根 K 线确认（不是边缘点）

# 刺穿条件
STOP_HUNT_PIERCE_PCT         = 0.003   # 刺穿距离：超过前高/前低的 0.3%
STOP_HUNT_VOL_RATIO_MIN      = 1.5     # 工作日：刺穿量能 ≥ 均量的 N 倍
STOP_HUNT_VOL_RATIO_MIN_WKND = 1.2    # 周末：刺穿量能门槛适当放宽
STOP_HUNT_VOL_LOOKBACK_15M   = 20      # 计算均量用的 15M K 线数

# 收回确认条件
STOP_HUNT_RECOVER_BARS       = 3       # 工作日：刺穿后 N 根 15M 以内收回
STOP_HUNT_RECOVER_BARS_WKND  = 4       # 周末：收回时间窗口宽松

# 入场时机检查：当前价离前高/前低不能超过 1%
STOP_HUNT_MAX_DIST_PCT       = 0.01    # 超过则信号时机已过

# 强度分级量能门槛
STOP_HUNT_VOL_STRONG         = 2.5     # 量能 ≥ 此倍 + 收回 ≤ 2 根 → strong
STOP_HUNT_RECOVER_STRONG     = 2       # strong 要求收回根数 ≤ N

# 止损缓冲
STOP_HUNT_SL_BUFFER          = 0.003   # SL = 刺穿极端点 ± 0.3% 缓冲

# 入场参数（挂限价单）
STOP_HUNT_EXPIRE_MIN_WKND    = 20      # 周末信号有效期（分钟，假突破多）

# 评分参数
STOP_HUNT_BASE_STRONG        = 70      # strong 信号 base score（与 reversal 对齐）
STOP_HUNT_BASE_MEDIUM        = 60      # medium 信号 base score
STOP_HUNT_MIN_SCORE_WEEKEND  = 65      # 周末最低评分门槛  # [v3.87 DOC] 仅回测脚本 backtest_btw.py 使用；实盘代码不读此常量（撤销此前 DOC-2 误标）

# 信号优先级（加入 SIGNAL_PRIORITY 时用）

# chg24 上限：超过此值应走 reversal，不走 stop_hunt
STOP_HUNT_CHG24_MAX          = 15.0

STOP_HUNT_ENABLED            = False           # v3.64: 胜率≈9%、期望值为负，暂停直至实盘验证胜率>35%
STOP_HUNT_EXCLUDED_SYMBOLS   = ["XRPUSDT"]  # 纯形态胜率<40%的标的，跳过stop_hunt走pullback/reversal路由

# ── stop_hunt 专属参数（v3.51）───────────────────────────────────────────
STOP_HUNT_CONSOLIDATION_BARS = 8      # 横盘积累检查：刺穿前这多根 1H 全部在 pivot 附近
STOP_HUNT_CONSOLIDATION_PCT  = 0.02   # 横盘范围：pivot ±2%
STOP_HUNT_FUNDING_THRESHOLD  = 0.0001 # 资金费率门槛 0.01%
STOP_HUNT_RETEST_EXPIRE_MIN  = 90     # 二探等待有效期（分钟，默认値 / low tier）
STOP_HUNT_RETEST_EXPIRE_HIGH = 30     # high tier 二探有效期（高波动币二探快，超时说明未真正二探）
STOP_HUNT_RETEST_EXPIRE_MID  = 60     # mid tier 二探有效期
RR_MIN_STOP_HUNT             = 0.8    # stop_hunt 专属 RR 要求（入场在 pivot 附近，胜率高，放宽 RR）

# ── tier 差异化参数预留（基于历史统计 2026-06-04 ~ 2026-10-18）───────────────
# 统计方法：15M K线刺穿前高/前低且收回事件，三层共 2556 个事件
#
# 高波动（BTW+RAVE）： 刺穿中位=6.28%   收回中位=1根  量能中位=2.20x  收回率=47%
# 中波动（SOL+HYPE）：刺穿中位=0.96%  收回中位=2根  量能中位=2.21x  收回率=35%
# 低波动（BTC+ETH）： 刺穿中位=0.72%  收回中位=2根  量能中位=2.58x  收回率=33%
#
# 后续走势正确率 49~53%（接近随机）——纯形态无方向预测能力，强平量验证是必要条件。
#
# 待实盘强平量验证跑30笔信号后，若方向正确率 > 55%，再按以下方向调整：
#   高波动 tier： STOP_HUNT_PIERCE_PCT=0.02   STOP_HUNT_RECOVER_BARS=2  STOP_HUNT_VOL_RATIO_MIN=2.0
#   中波动 tier： STOP_HUNT_PIERCE_PCT=0.005  STOP_HUNT_RECOVER_BARS=3  STOP_HUNT_VOL_RATIO_MIN=1.5
#   低波动 tier： STOP_HUNT_PIERCE_PCT=0.003  STOP_HUNT_RECOVER_BARS=4  STOP_HUNT_VOL_RATIO_MIN=2.5
# ────────────────────────────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════════════════
# OI Divergence 策略参数（v3.60）
# 逻辑：价格创新高/低，OI 方向背离 → 聪明钱在反向布局，即将逆转
# LONG：价格创 24H 新低 + OI 不降反升（空头拥挤，逼空风险）+ taker 开始回暖
# SHORT：价格创 24H 新高 + OI 不升反降（多头出逃）+ taker 开始转弱
# ═══════════════════════════════════════════════════════════════════════════════

# ── 触发门槛 ──────────────────────────────────────────────────────────────────
OI_DIV_PRICE_NEAR_LOW_PCT    = 1.03   # 价格 <= low_24h * 1.03 视为接近新低（3%内）
OI_DIV_PRICE_NEAR_HIGH_PCT   = 0.97   # 价格 >= high_24h * 0.97 视为接近新高（3%内）
OI_DIV_OI_RISE_PCT           = 0.10   # LONG：OI 相比 1H 前上升 >= 10% 视为异常拥挤
OI_DIV_OI_DROP_PCT           = 0.10   # SHORT：OI 相比 1H 前下降 >= 10% 视为多头出逃
OI_DIV_TAKER_RECOVERY_MIN    = 0.48   # LONG：taker_buy_ratio_now >= 此值视为买方回暖（从极度悲观恢复）
OI_DIV_TAKER_WEAK_MAX        = 0.52   # SHORT：taker_buy_ratio_now <= 此值视为买方转弱

# ── RSI 过滤 ─────────────────────────────────────────────────────────────────
OI_DIV_RSI1H_LONG_MAX        = 38  # 45→38：与 judge 原写死的有效门槛一致（原 45 被代码 38 覆盖）     # LONG：RSI1H 不能太高（> 45 说明还没超卖）
OI_DIV_RSI1H_SHORT_MIN       = 62  # 55→62：与 judge 原写死的有效门槛一致（原 55 被代码 62 覆盖）     # SHORT：RSI1H 不能太低（< 55 说明还没超买）

# ── 波动率门槛（与 pullback 共用 ATR 分层，不单独设参数）─────────────────────

# ── 评分参数 ──────────────────────────────────────────────────────────────────
OI_DIV_BASE_STRONG           = 68     # strong 信号 base score（OI 背离 + RSI 极端 + taker 回暖）
OI_DIV_BASE_MEDIUM           = 58     # medium 信号 base score（OI 背离 + taker 回暖，RSI 未极端）v3.65: 55→58，配合cg_bonus clip收窄后确保中等质量信号能过50分门槛

# ── OI 历史比较窗口 ───────────────────────────────────────────────────────────

# ── 清算图方向性验证（v3.65）────────────────────────────────────────────────
# 逻辑：信号方向的清算量必须占优，否则说明市场更可能朝反方向扫（如 SKUU 案例）
#   SHORT：下方多头清算量 / 上方空头待清算量 >= OI_DIV_LIQ_RATIO_MIN
#   LONG ：上方空头清算量 / 下方多头待清算量 >= OI_DIV_LIQ_RATIO_MIN
# 绝对量下限防止双方都接近 0 时比值失真（如冷门山寨币数据为空）
OI_DIV_LIQ_RATIO_MIN         = 1.5    # 方向清算量比值最低要求（信号方向 / 反向 >= 1.5）
OI_DIV_LIQ_ABS_MIN           = 300_000 # 信号方向绝对清算量下限（USD），低于此直接拒绝
OI_DIV_ORDER_EXPIRE_MIN      = 60      # oi_divergence 挂单有效期（分钟），GTD 模式

# ═══════════════════════════════════════════════════════════════════════════════
# EMA Trend Follow 策略参数（v3.61）
# 逻辑：4H EMA 多头排列趋势持续 + RSI 处于健康区间（不超买）→ 顺势做多回踩
# 触发：EMA20 > EMA50 > MA99（三线排列）+ 1H RSI 50~65 + 价格在 EMA20 附近回踩
# ═══════════════════════════════════════════════════════════════════════════════

# ── 触发门槛 ──────────────────────────────────────────────────────────────────
EMA_TREND_EMA_GAP_MIN_PCT     = 0.005  # EMA20 > EMA50 至少差距 0.5%（防止刚刚交叉的假多头；影子数据：排列不明确被拒信号 EV 为负，0.3% 已回滚）
EMA_TREND_RSI1H_MIN           = 48     # 1H RSI 下限（太低说明已经超卖，走 reversal 更合适）
EMA_TREND_RSI1H_MAX           = 68     # 1H RSI 上限（太高说明已超买，不是好的回踩入场点）
EMA_TREND_RSI1H_SHORT_MIN     = 32     # 做空 RSI1H 下限（原沿用做多 48）
EMA_TREND_RSI1H_SHORT_MAX     = 52     # 做空 RSI1H 上限（原写死 70）
EMA_TREND_RSI_GOLDEN_SHORT_MIN = 38    # 做空 rsi_golden 下限（做多为 52）
EMA_TREND_RSI_GOLDEN_SHORT_MAX = 48    # 做空 rsi_golden 上限（做多为 62）
EMA_TREND_RSI4H_MIN           = 45     # 4H RSI 下限（趋势结构支撑）
EMA_TREND_RSI4H_MAX           = 72     # 4H RSI 上限（超买区域放弃）

# ── 回踩深度：价格必须在 EMA20 附近（回踩确认） ───────────────────────────────
EMA_TREND_PULLBACK_ABOVE_EMA  = 0.02   # 价格最多在 EMA20 上方 2%（过高说明没回踩，追高）
EMA_TREND_PULLBACK_BELOW_EMA  = 0.015  # 价格最多在 EMA20 下方 1.5%（过低说明已破结构）
EMA_TREND_PULLBACK_BELOW_EMA_LONG = 0.02  # 做多专用：价格最多在 EMA20 下方 2.0%（做空仍用上面的 1.5%）

# ── BTC 协同：只在 BTC 不处于明显熊市时允许做多 ─────────────────────────────
# 由 scanner 的 btc_trend 处理，不在 judge 层重复检查

# ── 量能要求 ─────────────────────────────────────────────────────────────────
EMA_TREND_VOL_RATIO_MIN       = 0.5    # 量比最低门槛（比 pullback 宽松：趋势跟随不需要量能爆发）

# ── 评分参数 ──────────────────────────────────────────────────────────────────
EMA_TREND_BASE_STRONG         = 65     # strong：三线排列 + RSI 健康 + 量能配合
EMA_TREND_BASE_MEDIUM         = 52     # medium：两线排列 + RSI 可以

# ── CHG24 范围约束（防止追极端行情）──────────────────────────────────────────
EMA_TREND_CHG24_MAX           = 12.0   # chg24 > 12% 说明当日已有大涨，不追高
EMA_TREND_CHG24_MIN           = -8.0   # chg24 < -8% 说明当日大跌，应走 reversal 不走 trend
EMA_TREND_MAX_HOLD_HOURS     = 16     # ema_trend 时间止损软上限（小时）
                                      # 4H 趋势追踪，比 pullback 的 8H 宽松，比 reversal 的 12H 宽松

# ── Signal Analytics（信号分析系统）─────────────────────────────────────────
SIGNAL_ANALYTICS_FILE         = "logs/signal_analytics.jsonl"   # 相对 fx/ 目录
SIGNAL_ANALYTICS_TRACK_AFTER_H = 4    # 信号产生后 4H 才做事后追踪
SIGNAL_ANALYTICS_MAX_SIZE_MB  = 10    # 超过 10MB 触发轮转
SIGNAL_ANALYTICS_MIN_SAMPLE   = 30    # Kelly 调整需要的最小样本数

# ── Kelly 仓位调整 ──────────────────────────────────────────────────────────
KELLY_ENABLED    = False   # 初始关闭，数据足够后开启
KELLY_FRACTION   = 0.25    # 1/4 Kelly 保守系数
KELLY_MIN_CAP    = 0.15
KELLY_MAX_CAP    = 0.75

# ── 账户状态感知仓位调整 ─────────────────────────────────────────────────────
ACCOUNT_STATE_LOSS_SCALE_THRESH  = 0.10   # 当日亏损 > 10% 权益触发降仓（原3%干扰过多，积累阶段放宽）
ACCOUNT_STATE_LOSS_SCALE_FACTOR  = 0.60   # 降至 60%
ACCOUNT_STATE_CONSEC_LOSS_DAYS   = 3      # 连续亏损天数门槛
ACCOUNT_STATE_CONSEC_LOSS_FACTOR = 0.60   # 连续亏损时降至 60%

# ── 挂单随机化（反猎杀）────────────────────────────────────────────────────
ORDER_ENTRY_RANDOM_ATR    = 0.15    # 入场价随机偏移幅度（ATR 倍数）
ORDER_ENTRY_DELAY_MIN_S   = 15      # 挂单随机延迟最小秒数
ORDER_ENTRY_DELAY_MAX_S   = 90      # 挂单随机延迟最大秒数
ORDER_ENTRY_RANDOMIZE     = False   # 关闭：当前账户规模（100~300U名义）不需要反猎杀，随机延迟反而影响成交率

# ── 全市场资金费率情绪感知 ──────────────────────────────────────────────────
MARKET_FUNDING_LONG_THRESH   = 0.70   # 70% 正费率 = 多头拥挤
MARKET_FUNDING_SHORT_THRESH  = 0.30   # 30% 正费率 = 空头拥挤
MARKET_FUNDING_SCALE         = 0.70   # 拥挤时拥挤方向仓位降至 70%

# ── 系统性风险检测 ──────────────────────────────────────────────────────────
SYSTEMIC_USDT_DEPEG_THRESH        = 0.005   # USDT 偏离 0.5% 触发告警
SYSTEMIC_API_FAIL_RATE_THRESH     = 0.50    # API 失败率 50% 触发告警
SYSTEMIC_FUNDING_EXTREME_THRESH   = 0.010   # 持仓资金费率 1% 触发告警

# ── 组合风险告警 ────────────────────────────────────────────────────────────
PORTFOLIO_BETA_WARN_THRESH        = 2.0    # Beta > 2 告警
PORTFOLIO_SINGLE_POS_MAX_PCT      = 0.50   # 单仓最大权重 50%

# ── 滑点告警 ────────────────────────────────────────────────────────────────
SLIPPAGE_SL_WARN_PCT              = 0.5    # 单次止损滑点 > 0.5% 触发 Jarvis 告警

# ── TP2 动态收紧（时间自适应出场，v3.72）────────────────────────────────────────
TP2_DYNAMIC_TIGHTEN_ENABLED       = True   # 总开关
TP2_TIGHTEN_4H_MULT               = 1.5    # 持仓 4~8H：TP2 = 当前浮盈 × 1.5
TP2_TIGHTEN_8H_MULT               = 1.2    # 持仓 > 8H：TP2 = 当前浮盈 × 1.2
TP2_TIGHTEN_CHECK_INTERVAL_MIN    = 30     # 每30分钟检查一次（与 _adjust_sl_tp_once 同频）

# ── 信号时机优化：价格位移超过 N ATR 时触发重挂（v3.72）───────────────────────
SIGNAL_PRICE_SHIFT_REORDER_ATR    = 1.0    # 价格偏离原入场价 > 1 ATR 时重算并重挂
SIGNAL_PRICE_SHIFT_REORDER_TYPES  = ("reversal", "breakout")  # 只对这两类信号生效

# ── 加仓逻辑（Pyramiding，v3.72）────────────────────────────────────────────────
PYRAMID_ENABLED                   = False  # 总开关。暂停加仓，等积累数据后决定是否重启或改用影子模式（代码全部保留）
PYRAMID_FLOAT_PROFIT_ATR          = 1.5    # 浮盈 >= 1.5 ATR 才允许加仓（原 1.0，价格走得更充分再加，减少刚加仓就被打出）
PYRAMID_SIZE_RATIO                = 0.5    # 加仓仓位 = 原仓位 × 0.5
PYRAMID_MAX_ADDUP                 = 1      # 每个方向每个标的最多加一次
PYRAMID_RSI_LONG_MAX              = 75     # LONG 加仓时 RSI1H 上限（避免超买追入）
PYRAMID_RSI_SHORT_MIN             = 25     # SHORT 加仓时 RSI1H 下限

# ── 资金费率 + 主策略协同（v3.72）────────────────────────────────────────────────
FUNDING_SYNERGY_BONUS             = 1.15   # 方向与费率一致时仓位上浮系数（1.25→1.15，原值加成过激）
FUNDING_SYNERGY_PENALTY           = 0.80   # 方向与费率相反时仓位下调系数
FUNDING_SYNERGY_THRESH            = 0.0005 # 费率绝对值 > 0.05% 才触发协同逻辑



# ═══════════════════════════════════════════════════════════════════════════════
# v3.84 优化开关（第一轮）——每项对应 OPTIMIZATION_CHANGELOG.md 中的一条，False 即回到原行为
# ═══════════════════════════════════════════════════════════════════════════════
# 1) 提前滞涨平仓的浮亏阈值随止损距离缩放（原固定 2%）
OPT_STAGNATION_SCALE_BY_SL = False  # [v3.87 合并决策] 默认 False：OPT-1 按评审：先关闭，看 early_stagnation 频率再决定
EARLY_STAGNATION_LOSS_FRAC = 0.6    # 阈值 = max(下限, 0.6 × 止损距离)
EARLY_STAGNATION_LOSS_MIN  = 0.02   # 下限 2%（与原固定值一致）
# 2) TP1 成交后价格跌穿入场价：直接平掉剩余仓位，而不是把止损放到错误一侧
OPT_TP1_PROTECT_CLOSE = False  # [v3.87 合并决策] 默认 False：OPT-2 按评审：BUG-1/2 跑稳约 1 周后再开
# 3) 回踩入场（无清算区降级）多空对称
OPT_PULLBACK_ENTRY_SYMMETRIC = False  # [v3.87 合并决策] 默认 False：OPT-3 按评审：暂缓，空头 0.8ATR 为有意设计，待数据
PULLBACK_DEGRADED_ATR = 0.5
PULLBACK_MIN_DIST_ATR = 0.3
PULLBACK_MAX_DIST_ATR = 2.0

# ═══════════════════════════════════════════════════════════════════════════════
# v3.85 第二轮开关——False 即回到原行为
# ═══════════════════════════════════════════════════════════════════════════════
OPT_BREAKOUT_CLOSED_BAR       = True    # OPT-5 突破只用已闭合 1H K 线  # [v3.88 用户决定开启] 未闭合K线可被重绘成假突破；代价：突破信号晚约一根K线。设 False 恢复
OPT_FUNDING_WINDOW_PER_SYMBOL = True    # OPT-6 资金费窗口按标的自身结算时间，不再整轮停扫  # [v3.88 用户决定开启] 1H/4H 结算合约也有保护，不再白白停扫；代价：扫描频率增加，需观察限速。设 False 恢复
OPT_PYRAMID_FAIL_CLOSED       = True   # FIX-2 加仓所需 RSI 拿不到时本轮不加仓

# v3.86 第三轮开关——False 即回到原行为
OPT_REVERSAL_SHORT_TIER_RSI = False   # OPT-7 反转做空的 RSI4H 门槛按波动层区分（高波动 70）  # [v3.87 合并决策] 默认 False：OPT-7 按评审：暂缓，待对比高波动做空历史胜率

# ═══════════════════════════════════════════════════════════════════════════════
# v3.87 开仓场景重构（第一批）——False 即回到原行为
# ═══════════════════════════════════════════════════════════════════════════════
OPT_PULLBACK_ENTRY_AT_CONFIRM = False   # OPT-8 回踩在确认处入场（mark∓0.1ATR）  # [v3.87 合并决策] 默认 False：OPT-8 按评审：舍弃，会绕过清算区加权入场价；代码保留但永久关闭
OPT_INVALIDATION_SL           = False   # OPT-9 回踩/EMA趋势/反转用失效位止损（1H 结构点外 0.3ATR，1~3ATR）  # [v3.87 合并决策] 默认 False：OPT-9 按评审：舍弃，会绕过清算区止损；代码保留但永久关闭
OPT_OI_DIV_EXHAUSTION_CONFIRM = False   # OPT-10 OI 背离需不创新低(高) + 主动买卖比确认  # [v3.87 合并决策] 默认 False：OPT-10 按评审：暂缓，待 oi_divergence 累计 20+ 笔再定
MFE_DRAWBACK_USE_PROGRESS   = True    # mfe_drawback_50pct 改用「进度」判断(峰值进度>=60%、回撤>=50%、当前进度<30%、仍盈利);False=回退旧逻辑(峰值>0.5%且回撤>=50%)
OPT_EMA_TREND_TRIGGER_BAR     = False   # OPT-11 EMA 趋势需 1H/15M 触发 K 线  # [v3.87 合并决策] 默认 False：OPT-11 评审未列入：保守起见默认关闭，待数据

# v3.87 第二批
OPT_BREAKOUT_RSI_STRONG_RELAX = False   # OPT-12 放量(量比≥强量阈值)突破放宽 RSI 限制  # [v3.87 合并决策] 默认 False：OPT-12 按评审：暂缓，待验证 RSI>65 突破单质量
BREAKOUT_RSI_MAX_LONG_STRONG  = 75     # 强量突破做多 RSI1H 上限
BREAKOUT_RSI_MIN_SHORT_STRONG = 25     # 强量突破做空 RSI1H 下限
OPT_BREAKOUT_MOMENTUM_ENTRY   = False   # OPT-13 距突破位≤0.5ATR 时延续入场，不再强制等回踩  # [v3.87 合并决策] 默认 False：OPT-13 按评审：暂缓，待对比成交率与止损率

# ═══════════════════════════════════════════════════════════════════════════════
# v3.88 OPT-14 清算区相对强度（清算区金额 / 24H 成交额）——False 即回到原行为
# ═══════════════════════════════════════════════════════════════════════════════
OPT_LIQ_RELATIVE_STRENGTH = False  # 总开关：回踩入场清算区权重 + TP1 缓冲按相对强度动态化
LIQ_RELATIVE_STRONG   = 0.20   # >= 20% → 强
LIQ_RELATIVE_MID      = 0.05   # 5%~20% → 中；< 5% → 弱
LIQ_WEIGHT_REF_RATIO  = 0.10   # 10% 相对强度 = 权重 2.0（原固定权重）
LIQ_WEIGHT_MIN        = 0.5
LIQ_WEIGHT_MAX        = 3.0
TP1_LIQ_BUFFER_STRONG = 0.20   # TP1 距清算区前沿缓冲（ATR 倍数）
TP1_LIQ_BUFFER_MID    = 0.35
TP1_LIQ_BUFFER_WEAK   = 0.50
# [v3.88 OPT-14d] 相对强度分母：vol24（24H 成交额）/ oi（持仓名义价值）/ concentration（同方向清算图集中度）。
#   三种分母的数值都会写进 params / 持仓 / trade_log 的 liq_metrics 字段，积累数据后再决定用哪个、阈值定多少。
#   注意 concentration 在只有一个清算区时恒为 1.0，且随清算图拉取范围变化。
LIQ_REL_DENOM = "vol24"

# [v3.88 OPT-14f] 分歧度门控（受 OPT_LIQ_RELATIVE_STRENGTH 控制，不单独开关）：
#   清算区锚点与 EMA20 偏离超过 OPT_DIVERGENCE_GATE_ATR × ATR 时，动态权重上限压制到 OPT_DIVERGENCE_GATE_WEIGHT_CAP。
OPT_DIVERGENCE_GATE_ATR = 1.5
OPT_DIVERGENCE_GATE_WEIGHT_CAP = 2.0
# [v3.88 OPT-15] oi_divergence 近5根全阳（SHORT）/全阴（LONG）时直接拒绝信号；默认开启（与其他 OPT_* 不同）。
#   关闭则恢复 mark*0.992 / mark*1.008 兜底。
OPT_OID_REJECT_ALL_BULL = True

# ── 阶段3：影子触发器（只观测，不下单、不发 REST）──────────────────────────────
TRIGGER_DETECTOR_ENABLED = True  # True：每秒轮询 hub 内存，写 logs/trigger_log.jsonl 并与 logs/scan_signals.jsonl 对账
TRIGGER_CONFIG_VERSION   = "phase3.1"   # 写入 trigger_log 的 config_version 前缀（后接 config.py sha1 前 8 位）

# 触发阈值（比例均为小数；chg24 0.10 = 10%）
TRIGGER_PULLBACK_ATR_MULT        = 1.5     # LONG [EMA20-1.5ATR, EMA20+0.3ATR]，SHORT 对称
TRIGGER_REVERSAL_CHG24_ON        = 0.10
TRIGGER_REVERSAL_CHG24_OFF       = 0.08
TRIGGER_REVERSAL_FUNDING_THRESH  = 0.0005
TRIGGER_REVERSAL_FUNDING_OFF     = 0.0004
TRIGGER_REVERSAL_RSI_LOW_ON      = 32
TRIGGER_REVERSAL_RSI_LOW_OFF     = 36
TRIGGER_REVERSAL_RSI_HIGH_ON     = 68
TRIGGER_REVERSAL_RSI_HIGH_OFF    = 64
TRIGGER_BREAKOUT_CHG24           = 0.03
TRIGGER_BREAKOUT_ATR_DIST        = 0.3
TRIGGER_FUNDING_THRESH           = 0.0005
TRIGGER_RANGE_BOUNCE_LOOKBACK    = 24
TRIGGER_RANGE_BOUNCE_GAIN        = 0.10
TRIGGER_RANGE_BOUNCE_ATR_MIN     = 0.015
TRIGGER_RANGE_BOUNCE_ZONE        = 0.60    # 横盘振幅 <= 前期涨幅 × 0.6
TRIGGER_STOP_HUNT_VOL_RATIO      = 1.2
TRIGGER_STOP_HUNT_PIERCE_ATR     = 0.1
TRIGGER_STOP_HUNT_PIERCE_PCT     = 0.0015
TRIGGER_OI_DIV_CHANGE_1H         = 0.03
TRIGGER_OI_DIV_CHANGE_4H         = 0.05
TRIGGER_OI_DIV_PRICE_DIST        = 0.035
TRIGGER_OI_DIV_PRICE_DIST_MAX    = 0.05
TRIGGER_OI_DIV_ATR_MULT          = 1.5     # 暂不使用
TRIGGER_OI_DIV_ATR_MIN           = 0.005
TRIGGER_OI_DIV_CHG24_MIN         = 5.0     # |chg24| 达到此值才做OI背离检查
TRIGGER_OI_DIV_RSI_HIGH          = 70.0    # RSI 超买门槛
TRIGGER_OI_DIV_RSI_LOW           = 30.0    # RSI 超卖门槛
TRIGGER_EMA_TREND_EMA_THRESH     = 0.995   # 多头 EMA20 >= EMA50×0.995；空头 EMA20 <= EMA50×(2-0.995)
TRIGGER_EMA_TREND_PRICE_PCT      = 0.04    # 暂不使用
TRIGGER_EMA_TREND_ATR_MULT       = 1.2     # 暂不使用

# 冷却（秒）
TRIGGER_COOL_PULLBACK     = 900
TRIGGER_COOL_REVERSAL     = 900
TRIGGER_COOL_BREAKOUT     = 1800
TRIGGER_COOL_FUNDING_ARB  = 3600
TRIGGER_COOL_RANGE_BOUNCE = 3600
TRIGGER_COOL_STOP_HUNT    = 900
TRIGGER_COOL_OI_DIV       = 1200
TRIGGER_COOL_EMA_TREND    = 14400

# 令牌桶（每分钟 RATE 个，容量 CAPACITY；排队超过 TTL 秒记 expired；单策略 TTL 窗口内最多占容量的 40%）
TRIGGER_TOKEN_RATE             = 20
TRIGGER_TOKEN_CAPACITY         = 60
TRIGGER_TOKEN_TTL              = 300
TRIGGER_TOKEN_STRATEGY_MAX_PCT = 0.40

# ── 阶段5：触发器驱动真实分析（会真实下单）──────────────────────────────────────
TRIGGER_DISPATCH_ENABLED = True  # 需同时 TRIGGER_DETECTOR_ENABLED=True；True 时触发 → 预取 → analyze_one → 第四步 → 注册下单
SCAN_INTERVAL_PASSIVE    = 900   # 两个开关都为 True 时的兜底扫描间隔（秒）；否则保持 300s / 周末 900s
TRIGGER_DISPATCH_TIMEOUT = 30     # 单个分析任务超时（预取 + analyze_one + 第四步）
TRIGGER_DISPATCH_WORKERS = 1      # 分析协程数
TRIGGER_HUB_WAIT_SEC = 30         # hub 快照未就绪（K线/ticker 缺失）时，触发分析最多等待的秒数；到点仍未就绪则单次 REST 补拉后再分析（0=不等待，直接按旧逻辑报 hub_snapshot_incomplete）
TRIGGER_HUB_WAIT_POLL_SEC = 2.0   # 等待期间检查 hub 是否就绪的间隔（秒）
TRIGGER_DISPATCH_MAX_WORKERS = 1  # 触发器派发并发上限，过高会GIL争抢饿死事件循环

# [fix #48] 已删除的失效常量（代码从未引用，保留名单便于追溯）：
#   UNIVERSE_REFRESH_INTERVAL, SCANNER_INTERVAL, KLINE_LIMIT_15M, KLINE_LIMIT_1H
#   KLINE_LIMIT_4H, SCANNER_MAX_WORKERS, BTC_SHOCK_WINDOW, PENDING_CONFIRM_TIMEOUT
#   ATR_TIER_LOW_MAX, ATR_TIER_MID_MAX, PULLBACK_UPPER_BUFFER, PULLBACK_LIQ_ZONE_MIN
#   PULLBACK_LIQ_ZONE_MAX, PULLBACK_LIQ_ZONE_MAX_HIGH, BREAKOUT_REF_BARS, BREAKOUT_MIN_MARGIN
#   BREAKOUT_MIN_MARGIN_HIGH, BREAKOUT_VOLUME_RATIO_LOW, BREAKOUT_VOLUME_RATIO_HIGH, BREAKOUT_OI_RISE_LOW
#   BREAKOUT_OI_RISE_HIGH, BREAKOUT_CONFIRM_VOLUME_MIN, REVERSAL_DROP_24H_LONG, REVERSAL_DROP_4H_LONG
#   REVERSAL_RISE_24H_SHORT, REVERSAL_RISE_4H_SHORT, REVERSAL_FUNDING_MIN_LONG, REVERSAL_FUNDING_STRONG_LONG
#   REVERSAL_FUNDING_MIN_SHORT, REVERSAL_FUNDING_STRONG_SHORT, REVERSAL_RSI4H_STRONG_LONG, REVERSAL_RSI4H_STRONG_SHORT
#   REVERSAL_CRASH_ACCEL_BARS, REVERSAL_CRASH_DROP_PER_BAR, REVERSAL_CRASH_OI_DROP, REVERSAL_LIQ_NORMAL_MIN
#   REVERSAL_LIQ_PENDING_MIN, HIGH_VOL_EXTREME_CANDLE, HIGH_VOL_REVERSAL_RSI4H_LONG, HIGH_VOL_REVERSAL_RSI4H_SHORT
#   HIGH_VOL_REVERSAL_FUNDING_LONG, HIGH_VOL_REVERSAL_FUNDING_SHORT, CIRCUIT_BREAKER_RESET_HOUR, MARGIN_TYPE
#   ORDER_TIMEOUT_BREAKOUT, ORDER_TIMEOUT_NORMAL, HEALTH_EMA_WEIGHT, HEALTH_RSI_WEIGHT
#   HEALTH_FUNDING_WEIGHT, HEALTH_FUNDING_WARN_LONG, HEALTH_FUNDING_DANGER_LONG, HEALTH_FUNDING_WARN_SHORT
#   HEALTH_FUNDING_DANGER_SHORT, HEALTH_RSI_WARN_LONG, HEALTH_RSI_DANGER_LONG, HEALTH_RSI_WARN_SHORT
#   HEALTH_RSI_DANGER_SHORT, PENDING_SCORE_STRENGTH_MAX, PENDING_SCORE_TYPE_MAX, PENDING_SCORE_TIME_MAX
#   PENDING_SCORE_LIQ_MAX, STOP_LOSS_COOLDOWN, REVERSAL_COOLDOWN_STOP, REVERSAL_COOLDOWN_CRASH
#   TRAILING_DEBOUNCE, PATROL_INTERVAL, PATROL_OFFSET, CIRCUIT_BREAKER_FILE
#   COOLDOWN_STATE_FILE, OI_CACHE_FILE, LEVERAGE_CACHE_FILE, SYSTEM_LOG_FILE
#   FUNDING_ARB_SCAN_INTERVAL_MIN, FUNDING_ARB_CANCEL_BEFORE_MIN, FUNDING_ARB_COOLDOWN_SETTLE_MIN, PENDING_HEALTH_CHECK_INTERVAL_MIN
#   AI_ADVISOR_MIN_TRADES, LLM_API_BASE_URL, ATR_TIER_LOW_CUTOFF, ATR_TIER_HIGH_REVERSAL_CHG24
#   ATR_TIER_HIGH_PULLBACK_CHG24, STOP_HUNT_ENTRY_BUFFER_PCT, STOP_HUNT_EXPIRE_MIN, STOP_HUNT_MIN_SCORE_WEEKDAY
#   STOP_HUNT_PRIORITY, OI_DIV_OI_HIST_HOURS, SIGNAL_ANALYTICS_MAX_DAYS

# ── 股票类代币交易时段过滤（只影响开仓扫描，不影响持仓维护）──
STOCK_TOKEN_FILTER_ENABLED = True   # 总开关；False 时完全不生效

# 美股映射代币（只在美股交易时段开仓）
# 允许开仓：UTC 12:00-21:00（美股开盘前1小时到盘后1小时）
STOCK_TOKENS_US = {
    "TSLAUSDT","INTCUSDT","HOODUSDT","MSTRUSDT","AMZNUSDT",
    "CRCLUSDT","COINUSDT","PLTRUSDT","EWYUSDT","EWJUSDT",
    "PAYPUSDT","METAUSDT","NVDAUSDT","GOOGLUSDT","QQQUSDT",
    "SPYUSDT","AAPLUSDT","TSMUSDT","MUUSDT","SNDKUSDT",
    "MSFTUSDT","AVGOUSDT","BABAUSDT","AMDUSDT","QCOMUSDT",
    "USARUSDT","LITEUSDT","ORCLUSDT","DISUSDT","UBERUSDT",
    "CSCOUSDT","HDUSDT","SOXLUSDT","MRVLUSDT","CRWVUSDT",
    "WMTUSDT","JPMUSDT","VUSDT","BRKBUSDT","FLNCUSDT",
    "DRAMUSDT","RKLBUSDT","CBRSUSDT","SPCXUSDT","NBISUSDT",
    "WDCUSDT","ARMUSDT","BEUSDT","COHRUSDT","QNTXUSDT",
    "LLYUSDT","NVOUSDT","BBXUSDT","NOKUSDT","EWTUSDT",
    "ASTSUSDT","DELLUSDT","IBMUSDT","NOWUSDT","CRMUSDT",
    "IRENUSDT","ONDSUSDT","BXUSDT","HPEUSDT","AMATUSDT",
    "CRWDUSDT","CRDOUSDT","AAOIUSDT","IWMUSDT","AXTIUSDT",
    "NFLXUSDT","COSTUSDT","URNMUSDT","HIMSUSDT","EBAYUSDT",
    "ZMUSDT","DKNGUSDT","RIVNUSDT","GMEUSDT","XLEUSDT",
    "EWZUSDT","BMNRUSDT","UVXYUSDT","ADBEUSDT","GLWUSDT",
    "STXXUSDT","ASMLUSDT","LRCXUSDT","KLACUSDT","ALABUSDT",
    "SMCIUSDT","CIENUSDT","KORUUSDT","SONYUSDT","MVLLUSDT",
    "TQQQUSDT","SQQQUSDT","STRCUSDT","CATUSDT","TXNUSDT",
    "FLEXUSDT","TERUSDT","TTWOUSDT","KSTRUSDT","BSPUSDT",
    "BOTUSDT","WENUSDT","INTWUSDT","SNXXUSDT","XBIUSDT",
    "BNCUSDT","FWDIUSDT","GEVUSDT","VRTUSDT","SNOWUSDT",
    "APPUSDT","SKHYUSDT","MUUUSDT","SOXSUSDT","TZAUSDT",
    "SPCXUSD1","SHAZUSDT","SOFIUSDT","PANWUSDT","PENGUSDT",
    "TMFUSDT","TBTUSDT","BITOUSDT","PYPLUSDT","GSUSDT",
    "SMHUSDT","KOUSDT","RDDTUSDT","GDXUSDT","NETUSDT",
    "VSTUSDT","SHOPUSDT","LYTEUSDT","SKUUUSDT","SKDDUSDT",
    "RAMUSDT","DJTUSDT","MRNAUSDT","TEMUSDT","MRKUSDT",
    "IONQUSDT","MARAUSDT","PDDUSDT","NVDLUSDT","TSLLUSDT",
    "DDOGUSDT","TEAMUSDT","MDBUSDT","ZSUSDT","GTLBUSDT",
    "GPROUSDT","PATHUSDT","AMCUSDT","CYPHUSDT","ANETUSDT",
    "HUTUSDT","APLDUSDT","AGPUUSDT","OKLOUSDT","TWSTUSDT",
    "CVNAUSDT","RUMUSDT","XOMUSDT","CRMLUSDT","BWETUSDT",
    "ACNUSDT","MPUSDT","SECZUSDT","UNHUSDT","NKEUSDT",
    "USDEXUSDT","VKTXUSDT","MCDUSDT","AKAMUSDT","ZCSHUSDT",
    "MSTUUSDT","SNPSUSDT","RDWUSDT","CEGUSDT","BACUSDT",
    "IBKRUSDT","MDTUSDT",
}
STOCK_TOKENS_US_HOURS_UTC = (12, 21)  # 允许开仓的UTC小时范围 [12, 21)

# 港股映射代币（港股交易时段 UTC 01:30-08:00）
STOCK_TOKENS_HK = {
    "MINIMAXUSDT","ZHIPUUSDT","HK0700USDT","HK1810USDT","TENCENTUSDT",
    "POPMARTUSDT","GIGADEVUSDT","KUAISHOUUSDT","MEITUANUSDT","CSOPSKHYNIX2LUSDT",
    "CSOPSAMSUNG2LUSDT","ZHONGJIUSDT","HK0625USDT","BYDUSDT","HK0992USDT",
}
STOCK_TOKENS_HK_HOURS_UTC = (1, 8)  # 允许开仓的UTC小时范围 [1, 8)

# 韩股映射代币（韩股交易时段 UTC 00:00-06:30）
STOCK_TOKENS_KR = {
    "SKHYNIXUSDT","SAMSUNGUSDT","HYUNDAIUSDT","SAMSUNGEMUSDT","HANMIUSDT",
    "LGELECTRONICSUSDT","NAVERUSDT","KODEX200USDT",
}
STOCK_TOKENS_KR_HOURS_UTC = (0, 7)  # 允许开仓的UTC小时范围 [0, 7)

# A股映射代币（A股交易时段 UTC 01:30-07:00）
STOCK_TOKENS_CN = {
    "CXMTUSDT","UNITREEUSDT",
}
STOCK_TOKENS_CN_HOURS_UTC = (1, 7)  # 允许开仓的UTC小时范围 [1, 7)
