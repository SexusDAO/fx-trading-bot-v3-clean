"""
fx/main.py — 异步入口（V3.0）

职责：
  1. 启动 WsOrderListener（userData + markPrice）
  2. 初始化 PositionManager，接入 ws_listener
  3. 定时调度：
       T1: 每 5 分钟   → scanner.run_scan()
       T2: 每 5 分钟   → position_manager.patrol_once()（与 T1 错开 30 秒）
       T3: 每 30 分钟  → scanner.refresh_pool()
       T4: 每天 UTC 00:00 → risk_manager.reset_circuit()
  4. 优雅退出：捕获 SIGINT / SIGTERM，停止所有任务再退出
"""

import asyncio
import logging
import os
import signal
import sys
import shutil
import subprocess
import time
import faulthandler
import threading
import io
from pathlib import Path

# ── 启动依赖检查 ─────────────────────────────────────────────────────────────
def _check_dependencies():
    """验证关键依赖包是否可用，缺失时立即报错退出。"""
    required = [
        ("python_socks", "python-socks[asyncio]"),
        ("aiohttp", "aiohttp"),
        ("websockets", "websockets"),
    ]
    missing = []
    for module, pkg in required:
        try:
            __import__(module)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"[FATAL] 缺少依赖包: {missing}")
        print(f"请运行: pip install {' '.join(missing)}")
        sys.exit(1)

_check_dependencies()

# ── 临时诊断（定位后删除）────────────────────────────────────────────────
# 事件循环看门狗：独立线程检测事件循环阻塞，超过 2 秒 dump 所有线程堆栈
_loop_heartbeat_ts = time.monotonic()

async def _loop_heartbeat():
    """每 0.5 秒更新心跳时间戳，供看门狗线程检测事件循环是否存活。"""
    global _loop_heartbeat_ts
    while True:
        _loop_heartbeat_ts = time.monotonic()
        await asyncio.sleep(0.5)

def _watchdog_thread_fn():
    """后台 daemon 线程：检测事件循环阻塞，超过 2 秒时 dump 所有线程堆栈。
    10 秒冷却防止连续刷屏。"""
    last_dump = 0.0
    while True:
        time.sleep(0.5)
        lag = time.monotonic() - _loop_heartbeat_ts
        if lag > 2.0 and time.monotonic() - last_dump > 10.0:
            # faulthandler 需要真实 fileno，StringIO 会报 UnsupportedOperation，改写磁盘文件
            try:
                _dump_path = Path(__file__).parent / "logs" / "watchdog_stack.txt"
                with open(_dump_path, "w") as _f:
                    faulthandler.dump_traceback(file=_f, all_threads=True)
                _stack = _dump_path.read_text()
            except Exception as _fe:
                _stack = f"堆栈获取失败: {_fe}"
            logger.error(
                f"[watchdog] 事件循环阻塞 {lag:.1f}s，线程堆栈:\n{_stack}"
            )
            last_dump = time.monotonic()

# ── 启动前依赖检查：确保 SOCKS 代理依赖可用 ─────────────────────────────────
def _ensure_deps():
    """检查关键依赖，缺失时自动安装，防止沙箱重建后包丢失。"""
    try:
        import socks  # noqa: F401
    except ImportError:
        print("[startup] PySocks 缺失，自动安装...", flush=True)
        subprocess.run(
            [sys.executable, "-m", "pip", "install",
             "requests[socks]", "PySocks", "-q", "--break-system-packages"],
            check=True,
        )
        print("[startup] PySocks 安装完成，重建代理 Session", flush=True)
        # 重建 binance_api 的 Session，让 SOCKS 代理立即生效
        try:
            ROOT_PATH = Path(__file__).parent
            sys.path.insert(0, str(ROOT_PATH))
            sys.path.insert(0, str(ROOT_PATH / "shared"))
            from shared.binance_api import reset_session
            reset_session()
            print("[startup] Session 重建完成", flush=True)
        except Exception as e:
            print(f"[startup] Session 重建失败（将在下次 import 时自动修复）: {e}", flush=True)

_ensure_deps()
from datetime import datetime, timezone

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent))

# 启动时自动清理 __pycache__，防止旧的 .pyc 导致修复不生效
for _d in ROOT.rglob("__pycache__"):
    shutil.rmtree(_d, ignore_errors=True)

LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── 自写 main.pid，供 daemon_start.py 精确管理 ──────────────────────────────
_MAIN_PID_FILE = LOG_DIR / "main.pid"


def _pid_is_running_main(pid: int) -> bool:
    """pid 对应进程仍存活（Linux 下额外核对 cmdline 含 main.py，避免 pid 复用误判）。"""
    if pid <= 0 or pid == os.getpid():
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as _f:
            return b"main.py" in _f.read()
    except FileNotFoundError:
        return True      # 无 /proc（非 Linux）：进程存在即视为在运行
    except Exception:
        return True


# [fix #6] 单实例保护：pid 文件存在且进程仍在运行 → 直接退出并报错，不覆盖 pid 文件
try:
    _old_pid = int(_MAIN_PID_FILE.read_text().strip())
except Exception:
    _old_pid = 0
if _old_pid and _pid_is_running_main(_old_pid):
    sys.stderr.write(f"[main] 已有 main.py 实例在运行 (pid={_old_pid})，本进程退出，避免双实例重复下单\n")
    sys.exit(1)
try:
    _MAIN_PID_FILE.write_text(str(os.getpid()))
except Exception:
    pass

import atexit
@atexit.register
def _cleanup_main_pid():
    try:
        # 只清理自己写的 pid 文件，不误删其他实例的
        if _MAIN_PID_FILE.exists() and _MAIN_PID_FILE.read_text().strip() == str(os.getpid()):
            _MAIN_PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass

# v3.32: FileHandler → RotatingFileHandler，防止 system.log 无限增长
# 10MB × 5 份 = 最多保留 50MB 历史日志，长期 24/7 运转安全
from logging.handlers import RotatingFileHandler as _RotatingFileHandler
import logging.handlers as _lh
import queue as _queue
# 异步日志：root 只挂 QueueHandler（非阻塞入队），RotatingFileHandler 由 QueueListener 后台线程串行写文件，
# 事件循环线程不再持有文件 handler 锁、不做 shouldRollover 文件 I/O
_file_handler = _RotatingFileHandler(
    str(LOG_DIR / "system.log"),
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=5,
    encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
_log_queue = _queue.SimpleQueue()
_queue_listener = _lh.QueueListener(_log_queue, _file_handler, respect_handler_level=True)
_queue_listener.start()
_queue_handler = _lh.QueueHandler(_log_queue)
# 入队时只保留原始消息（含异常堆栈），时间/级别/名称前缀由 _file_handler 统一加；
# 不设的话 basicConfig 会给它套默认格式，导致前缀重复（"INFO x INFO:x:msg"）
_queue_handler.setFormatter(logging.Formatter("%(message)s"))
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        _queue_handler,
        # v3.14: 去掉 StreamHandler，daemon 已将 stdout 重定向到 system.log，
        # 保留 StreamHandler 会导致每条日志重复写两次
    ],
    force=True,  # v3.14: 强制覆盖已有 handler，防止如果子模块提前初始化了 root logger
)


@atexit.register
def _stop_queue_listener():
    # 退出时停止 listener：先把队列里剩余日志写完再结束后台线程
    try:
        _queue_listener.stop()
    except Exception:
        pass
logger = logging.getLogger("fx.main")
logging.getLogger("websockets").setLevel(logging.WARNING)  # 屏蔽 ws 心跳 DEBUG 日志
logging.getLogger("urllib3").setLevel(logging.WARNING)


# ───────────────────────────────────────────────────────────────────────────────
# v3.33 共享线程池：扫描线程不再每轮新建，避免卡住时线程多次累积
import concurrent.futures as _cfu

# 预导入 analysis_worker：其顶层 import（concurrent.futures.process / multiprocessing.connection）在启动时完成，
# 避免 shutdown 路径在事件循环里懒加载、gVisor 下 _path_stat 阻塞 348s
try:
    from shared import analysis_worker as _aw_stop
except Exception:
    _aw_stop = None

_SCAN_POOL = _cfu.ThreadPoolExecutor(max_workers=1, thread_name_prefix="scan-worker")

# ───────────────────────────────────────────────────────────────────────────────
# 定时任务（异步版）
# ───────────────────────────────────────────────────────────────────────────────

async def _loop_scan(interval: int = 300, signal_watcher=None, ws_listener=None) -> None:
    """每 interval 秒执行一次扫描，扫到信号后注册到 signal_watcher（若提供）。
    v3.42: 周末自动降频至 900s（15 分钟），工作日维持 interval（默认 300s）。
    周末价格几乎不动，5 分钟和 15 分钟的信号捕捉能力完全相同，但资源消耗减少 2/3。
    """
    await asyncio.sleep(5)
    # ── 6小时心跳计数器 ────────────────────────────────────────────────
    import time as _hb_time, datetime as _hb_dt
    _hb_interval   = 6 * 3600          # 6 小时一次
    _hb_last_ts    = _hb_time.time()   # 启动时对齐，第一次心跳在启动 6h 后
    _hb_scans      = 0    # 计数：扫描轮次
    _hb_raw        = 0    # 计数：原始信号
    _hb_watched    = 0    # 计数：进入监控
    _hb_placed     = 0    # 计数：成交开仓
    _scan_future   = None # 上一轮 run_in_executor 返回的 asyncio future（用于防重叠/防排队堆积）
    while True:
        try:
            from scanner import run_scan
            # ── 心跳写入（供 health_check 活跃度检查）───────────────────────
            try:
                import json as _hb_j, time as _hb_t, pathlib as _hb_p
                _hb_f = _hb_p.Path(__file__).parent.parent / "ws_cache" / "fx_main_heartbeat.json"
                _hb_f.write_text(_hb_j.dumps({"ts": _hb_t.time()}))
            except Exception:
                pass
            # [fix] 上一轮线程还没结束（wait_for 超时只放弃等待，线程取消不了）→ 直接跳过本轮，
            #   不再往只有 1 个 worker 的 _SCAN_POOL 里排队，避免僵尸线程结束后连续补跑过期的扫描
            if _scan_future is not None and not _scan_future.done():
                logger.warning("[scanner] 上一轮仍在运行，跳过本轮")
                await asyncio.sleep(interval)
                continue
            loop = asyncio.get_event_loop()
            # v3.33 复用共享线程池（替代每轮 with ThreadPoolExecutor）
            import functools
            _scan_fn = functools.partial(run_scan, signal_watcher=signal_watcher)
            future = loop.run_in_executor(_SCAN_POOL, _scan_fn)
            _scan_future = future
            # 取走（可能晚到的）异常，避免 "exception was never retrieved"
            future.add_done_callback(lambda f: f.cancelled() or f.exception())
            try:
                # shield：wait_for 超时会取消传入的 future，取消后 done() 为 True，会让上面的防重叠检查误判；
                # 用 shield 保证超时只放弃等待，_scan_future 在线程真正结束前一直是未完成状态
                result = await asyncio.wait_for(asyncio.shield(future), timeout=300)
                placed = (result or {}).get("placed", 0)
                # ── 心跳计数器更新 ────────────────────────────────────────────
                _r = result or {}
                _hb_scans   += 1
                _hb_raw     += _r.get("signals_raw", 0)
                _hb_placed  += placed
                # 计入监控的信号数（signals_final 是进入 watcher 的）
                _hb_watched += _r.get("signals_final", 0)
                if placed:
                    logger.info(f"[main] 扫描完成，注册信号/开仓 {placed} 笔")
                else:
                    logger.debug("[main] 扫描完成，无信号")
                # 打印 signal_watcher 当前监控状态
                if signal_watcher is not None:
                    pending = signal_watcher.pending_count()
                    if pending:
                        logger.info(f"[main] SignalWatcher 监控中: {pending} 个信号 {signal_watcher.pending_symbols()}")
            except asyncio.TimeoutError:
                logger.error("[main] task_scan 超时（300s），跳过本轮（扫描线程仍在后台，结束前后续轮次会被跳过）")
            except Exception as e:
                logger.error(f"[main] task_scan 子线程异常: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"[main] task_scan 外层异常: {e}", exc_info=True)
        # v3.42: 周末降频 300s → 900s，工作日保持原 interval
        from scanner import _is_weekend_period as _iwp
        from datetime import datetime, timezone as _tz
        _eff_interval = 900 if _iwp(datetime.now(_tz.utc)) else interval
        # 阶段5：触发器为主路径时（两个开关都 True）扫描降为兜底，其余情况保持原逻辑
        try:
            import config as _cfg_ps
            if getattr(_cfg_ps, "TRIGGER_DETECTOR_ENABLED", False) and getattr(_cfg_ps, "TRIGGER_DISPATCH_ENABLED", False):
                _eff_interval = int(getattr(_cfg_ps, "SCAN_INTERVAL_PASSIVE", 1800))
        except Exception:
            pass
        if _eff_interval != interval:
            logger.debug(f"[main] 周末模式：扫描间隔降频 {interval}s → {_eff_interval}s")
        # ── 6小时心跳推送 ────────────────────────────────────────────
        _now_ts = _hb_time.time()
        if _now_ts - _hb_last_ts >= _hb_interval:
            try:
                from shared.notifier import notify_async as _nb_ntf
                # fix: 直接用传入的 ws_listener 实例，模块级不存在 _ws_instance
                _ws_ok = "✅ 全部连接"
                if ws_listener is not None:
                    _disconn = getattr(ws_listener, "disconnected_streams", None)
                    if callable(_disconn):
                        _bad = _disconn()
                        _ws_ok = f"❌ {','.join(_bad)}断开" if _bad else "✅ 全部连接"
                    elif not getattr(ws_listener, "_running", True):
                        _ws_ok = "❌ WS 未运行"
                _pending_n = signal_watcher.pending_count() if signal_watcher else 0
                _hb_utc    = _hb_dt.datetime.now(_hb_dt.timezone.utc).strftime("%H:%M UTC")
                await _nb_ntf(
                    f"💓 系统心跳 · {_hb_utc}\n"
                    f"扫描正常 | {_pending_n}个标的 | 每5分钟一轮\n"
                    f"过去6小时：原始信号{_hb_raw}个 | 进入监控{_hb_watched}个 | 成交{_hb_placed}笔\n"
                    f"WS：{_ws_ok}"
                )
            except Exception as _hb_e:
                logger.debug(f"[main] 心跳推送失败: {_hb_e}")
            # 重置计数器
            _hb_last_ts = _now_ts
            _hb_scans = _hb_raw = _hb_watched = _hb_placed = 0
        await asyncio.sleep(_eff_interval)


def _classify_loop_latency(actual: float, warn: float = 0.5, err: float = 2.0):
    """事件循环延迟分级：返回 None / "warn" / "error"。"""
    if actual > err:
        return "error"
    if actual > warn:
        return "warn"
    return None


async def _loop_event_latency_monitor(interval: float = 5.0, probe: float = 0.1) -> None:
    """事件循环心跳延迟监控：每 interval 秒一次心跳，相邻两次心跳的实际间隔减去 interval = 事件循环延迟
    （GIL 竞争 / 同步调用占用事件循环线程）。延迟 >1s 打 WARNING，>5s 打 ERROR。
    同时保留原有的 sleep(probe) 探针（>0.5s warn / >2s error），两种口径互补。"""
    import time as _t
    from shared.observability import heartbeat_delay, heartbeat_level
    prev = _t.monotonic()
    while True:
        await asyncio.sleep(interval)
        now = _t.monotonic()
        delay = heartbeat_delay(prev, now, interval)
        prev = now
        hl = heartbeat_level(delay)
        if hl == "error":
            logger.error(f"[loop] 心跳延迟 {delay:.2f}s（事件循环严重阻塞）")
        elif hl == "warn":
            logger.warning(f"[loop] 心跳延迟 {delay:.2f}s")
        t0 = _t.monotonic()
        await asyncio.sleep(probe)
        actual = _t.monotonic() - t0
        lvl = _classify_loop_latency(actual)
        if lvl == "error":
            logger.error(f"[loop] 事件循环严重阻塞 {actual:.2f}s")
        elif lvl == "warn":
            logger.warning(f"[loop] 事件循环延迟 {actual:.2f}s")
        prev = _t.monotonic()          # probe 自身的 0.1s 不计入下一次心跳间隔


async def _loop_patrol(interval: int = 300, offset: int = 30) -> None:
    """每 interval 秒执行一次巡检，与 scan 错开 offset 秒。"""
    await asyncio.sleep(offset)
    while True:
        try:
            from position_manager import patrol_once as _patrol_once
            await _patrol_once()
        except Exception as e:
            logger.error(f"[main] task_patrol 异常: {e}", exc_info=True)
        await asyncio.sleep(interval)


async def _loop_refresh_pool(interval: int = 1800) -> None:
    """每 interval 秒刷新一次候选池。
    v3.42: 工作日缩短至 900s（15 分钟），更快捕捉新进涨跌幅榜的标的。
    周末保持 1800s（30 分钟），候选池变化慢，高频刷新无意义。
    """
    while True:
        try:
            from scanner import refresh_pool
            await asyncio.get_event_loop().run_in_executor(None, refresh_pool)
            logger.info("[main] 候选池已刷新")
        except Exception as e:
            logger.error(f"[main] task_refresh_pool 异常: {e}", exc_info=True)
        # v3.42: 工作日候选池刷新缩短到 15 分钟，周末保持 30 分钟
        from scanner import _is_weekend_period as _iwp
        from datetime import datetime, timezone as _tz
        _eff_pool_interval = interval if _iwp(datetime.now(_tz.utc)) else 900
        if _eff_pool_interval != interval:
            logger.debug(f"[main] 工作日模式：候选池刷新间隔 {interval}s → {_eff_pool_interval}s")
        await asyncio.sleep(_eff_pool_interval)


async def _loop_funding_arb(interval_sec: int = 1800, signal_watcher=None) -> None:
    """每30分钟运行一次 funding_arb 独立扫描"""
    import asyncio as _aio
    await _aio.sleep(120)  # 启动偏移：错开主扫描
    while True:
        try:
            from scanner import run_funding_arb_scan
            loop = _aio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: run_funding_arb_scan(dry_run=False, signal_watcher=signal_watcher)
            )
        except Exception as _e:
            logger.error(f"[main] funding_arb_scan 异常: {_e}", exc_info=True)
        await _aio.sleep(interval_sec)


async def _loop_reset_circuit() -> None:
    """每天 UTC 00:00 重置熔断器。"""
    while True:
        now = datetime.now(timezone.utc)
        # 计算到下一个 00:00 的秒数
        seconds_until_midnight = (
            (23 - now.hour) * 3600
            + (59 - now.minute) * 60
            + (60 - now.second)
        )
        await asyncio.sleep(seconds_until_midnight)
        try:
            from shared.risk_manager import reset_circuit, backup_daily_logs
            reset_circuit()
            try:
                backup_daily_logs()   # [fix #48] 备份与熔断重置分离
            except Exception as _be:
                logger.warning(f"[main] 日志备份失败: {_be}")
            logger.info("[main] 熔断器日重置完成")
        except Exception as e:
            logger.error(f"[main] task_reset_circuit 异常: {e}", exc_info=True)
        await asyncio.sleep(60)  # 防止同秒多次触发


_MAINT_TIMEOUT_SEC = 600   # 单个维护脚本最长运行时间


def _seconds_until_next_utc_midnight(now=None) -> float:
    """到下一个 UTC 00:00 的秒数（至少 1 秒）。"""
    from datetime import datetime, timedelta, timezone
    now = now or datetime.now(timezone.utc)
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max((nxt - now).total_seconds(), 1.0)


def _seconds_until_next_hour(now=None) -> float:
    """到下一个整点（UTC，整点对所有时区一致）的秒数（至少 1 秒）。"""
    from datetime import datetime, timedelta, timezone
    now = now or datetime.now(timezone.utc)
    nxt = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return max((nxt - now).total_seconds(), 1.0)


async def _loop_liq_snapshot() -> None:
    """每整点保存一次候选池的清算区快照（回测数据积累）。失败只记日志，不影响主流程。"""
    while True:
        await asyncio.sleep(_seconds_until_next_hour())
        try:
            from tools import liq_snapshot
            _r = await asyncio.to_thread(liq_snapshot.run)
            logger.info(f"[main] 清算区快照: {_r}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[main] 清算区快照失败（非致命）: {e}")
        await asyncio.sleep(5)   # 防止同秒重复触发


def _seconds_until_next_utc_hour(hour: int, now=None) -> float:
    """到下一个 UTC hour:00 的秒数（至少 1 秒；正好整点则取明天）。"""
    from datetime import datetime, timedelta, timezone
    now = now or datetime.now(timezone.utc)
    nxt = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if nxt <= now:
        nxt += timedelta(days=1)
    return max((nxt - now).total_seconds(), 1.0)


async def _loop_trigger_report(det) -> None:
    """每天 UTC 08:00 发触发器日报到钉钉。统计取 det.stats 与上次快照的差；
    进程内计数器重启清零，所以重启后的第一条是「自启动以来」。失败只记日志。"""
    import time as _t
    prev = None
    while True:
        await asyncio.sleep(_seconds_until_next_utc_hour(8))
        try:
            from tools import daily_dingtalk as _dd
            from shared.notifier import dingtalk_notify
            from shared.trigger_detector import TRIGGER_LOG
            now_ms = int(_t.time() * 1000)
            cnt = await asyncio.to_thread(_dd.count_triggers, TRIGGER_LOG, now_ms - 86_400_000, now_ms)
            snap = dict(det.stats)
            dingtalk_notify(_dd.build_trigger_text(snap, prev, cnt))
            prev = snap
            logger.info("[main] 触发器日报已发送")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[main] 触发器日报失败（非致命）: {e}")
        await asyncio.sleep(60)   # 防止同一小时重复触发


async def _run_maintenance_script(script: str, args: list, log_name: str) -> int:
    """子进程运行 tools 下的维护脚本，标准输出/错误追加写入 logs/<log_name>。不阻塞主事件循环；
    超时则杀掉子进程。返回退出码（超时/启动失败返回 -1）。永不抛异常。"""
    from datetime import datetime, timezone
    log_path = ROOT / "logs" / log_name
    script_path = ROOT / "tools" / script
    header = f"\n===== {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC  {script} {' '.join(args)} =====\n"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "ab") as lf:
            lf.write(header.encode("utf-8"))
            lf.flush()
            proc = await asyncio.create_subprocess_exec(
                sys.executable, str(script_path), *args,
                stdout=lf, stderr=asyncio.subprocess.STDOUT, cwd=str(ROOT),
            )
            try:
                rc = await asyncio.wait_for(proc.wait(), timeout=_MAINT_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                lf.write(f"[超时 {_MAINT_TIMEOUT_SEC}s，已终止]\n".encode("utf-8"))
                logger.warning(f"[main] 维护脚本 {script} 超时被终止")
                return -1
        if rc != 0:
            logger.warning(f"[main] 维护脚本 {script} 退出码={rc}，详见 logs/{log_name}")
        return rc
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"[main] 维护脚本 {script} 运行失败: {e}")
        return -1


async def _loop_daily_maintenance() -> None:
    """每天 UTC 00:00 运行维护脚本：触发对账（--dry-run，只查看不修改）+ rejected_log 清理（保留 7 天）+ shadow_verify 影子信号补标签。
    用子进程执行，避免脚本里的网络请求/文件读写阻塞主事件循环；与 _loop_reset_circuit 相互独立。"""
    while True:
        await asyncio.sleep(_seconds_until_next_utc_midnight())
        try:
            await _run_maintenance_script("reconcile_triggers.py", ["--dry-run"], "trigger_reconcile_auto.log")
            await _run_maintenance_script("cleanup_rejected_log.py", ["--days", "7"], "cleanup_rejected_log_auto.log")
            # 影子信号离线补标签（只读；放在 cleanup 之后，读到的是清理后的 rejected_log；失败只记日志）
            await _run_maintenance_script("shadow_verify.py", [], "shadow_verify_auto.log")
            # UTC 00:00 钉钉摘要（前一个 UTC 日）；只发钉钉，失败只记日志
            await _run_maintenance_script("daily_dingtalk.py", [], "daily_dingtalk_auto.log")
            try:
                from shared.notifier import cleanup_stale_cards
                _closed = await asyncio.to_thread(cleanup_stale_cards, 48 * 3600)
                logger.info(f"[main] 孤儿卡片清理: 关闭 {len(_closed)} 张")
            except Exception as _sce:
                logger.warning(f"[main] 孤儿卡片清理失败（非致命）: {_sce}")
            logger.info("[main] 每日维护任务完成")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[main] daily_maintenance 异常: {e}", exc_info=True)
        await asyncio.sleep(60)  # 防止同秒多次触发


async def _loop_track_outcomes(interval_sec: int = 3600) -> None:
    """每小时运行一次事后追踪，对已通过 judge 的信号填充 outcome。"""
    await asyncio.sleep(600)  # 启动偏移 10 分钟，错开主扫描
    while True:
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: __import__('tools.track_outcomes', fromlist=['run']).run()
            )
        except Exception as _e:
            logger.warning(f"[main] track_outcomes 异常（非致命）: {_e}")
        await asyncio.sleep(interval_sec)


# ═══════════════════════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════════════════════

def _build_orphan_record(sym: str, pos_side: str, rp: dict, algo_orders, open_orders, now_ms: int) -> tuple:
    """孤儿仓位（交易所有、本地无）→ 建档记录。返回 (record, notes)。
    同时从已拉取的交易所挂单里读出保护单，避免记录里 sl_price/sl_order_id 缺失（读取方 float(.get('sl_price',0)) 会得到 0）：
      · 止损：Algo 单按 symbol+positionSide+平仓方向+STOP_MARKET 匹配，恰好 1 个才写 sl_price/sl_order_id；0 个或多个不写；
      · 止盈：普通限价单按离入场近→远排序，第 1 个写 tp1_*，第 2 个写 tp2_*。"""
    from shared.guard_match import find_exchange_sl, sl_fields, find_exchange_tps, tp_fields
    entry = float(rp["entryPrice"])
    qty = abs(float(rp["positionAmt"]))
    ps = {
        "pos_id":             f"rec_{sym}_{pos_side[0]}_{now_ms}",
        "symbol":             sym,
        "direction":          pos_side.lower(),   # 取自交易所 positionSide
        "status":             "open",
        "mode":               "recovered",
        "sig_type":           "recovered",
        "entry_price":        entry,
        "fill_price":         entry,
        "quantity":           qty,
        "quantity_remaining": qty,
        "leverage":           int(float(rp.get("leverage", 0) or 0)),
        "open_time":          now_ms,
        "fill_time":          now_ms,
        "tp1_filled":         False,
        "tp2_filled":         False,
        "source":             "orphan_recovered",
    }
    notes = []
    sl_hits = find_exchange_sl(algo_orders, sym, pos_side)
    if len(sl_hits) == 1:
        ps.update(sl_fields(sl_hits[0]))
        notes.append(f"SL@{ps['sl_price']:.6g}(id={ps['sl_order_id']})")
    elif len(sl_hits) > 1:
        notes.append(f"交易所有{len(sl_hits)}个止损单，歧义，未自动认领SL")
    tps = find_exchange_tps(open_orders, sym, pos_side)
    if tps:
        ps.update(tp_fields(tps))
        notes.append(f"TP×{min(len(tps), 2)}")
    return ps, notes


_sync_lite_busy = False       # 防止 WS 重连回调与定时任务重叠执行


def _claim_quota(ts_list, limits: dict, claimed: dict) -> bool:
    """孤儿仓位认领配额判断。ts_list = [(open_ms, symbol)]，claimed = {"total":n, "long":n, "short":n}（已含本地持仓）。"""
    return (claimed["total"] < limits["total"]
            and claimed[ts_list[0][1]] < limits[ts_list[0][1]])


# recovered：当前代码不会把它写进 status（只写 mode/sig_type），但历史/手工记录可能带；带了就必须算「本地已有」，不能再建一次孤儿
_LOCAL_LIVE_STATUSES = ("open", "pending", "patching", "pending_guard", "recovered")


def _match_local_position(positions, sym: str, pos_side: str):
    """交易所持仓 ↔ 本地 pos_state 记录匹配：(symbol, direction.upper()) + 状态 open/pending/patching/pending_guard。
    多条时取 open_time 最新的一条；没有返回 None（只有 None 才允许建孤儿档案）。"""
    side = str(pos_side).upper()
    hits = [p for p in positions or []
            if isinstance(p, dict) and p.get("symbol") == sym
            and str(p.get("direction", "")).upper() == side
            and p.get("status") in _LOCAL_LIVE_STATUSES]
    if not hits:
        return None

    def _ot(p):
        try:
            return float(p.get("open_time", 0) or 0)
        except (TypeError, ValueError):
            return 0.0
    return max(hits, key=_ot)


async def _check_and_reattach_guards(local: dict, rp: dict, algo_orders=None, open_orders=None,
                                     repatch_tp: bool = True) -> dict:
    """本地已有记录的仓位：只核对/认领保护单，绝不改动 entry_price/sl_price/mode/judge_detail/pos_id 等业务字段。
      · SL：先看本地 sl_order_id 是否在交易所 Algo 单 algoId 集合里（存活）；不在则按 symbol+positionSide+平仓方向+STOP_MARKET
        匹配，恰好 1 个 → 只回写 sl_order_id；0 个 → 缺失，委托 position_manager._补挂保护单 重挂；多个 → 歧义，同样委托（它会告警不盲挂）。
      · TP：先看本地 tp1/tp2_order_id 是否在挂单 orderId 集合里；不在则在 LIMIT+平仓方向+positionSide 一致的挂单里，
        按与本地 tp 价格最接近分配（偏差 ≤3% 才认领），只回写 tp*_order_id。
      · repatch_tp=False（轻量对齐用）：只在 SL 缺失/歧义时委托补挂，TP 缺失不补。
    查询交易所失败 → 什么都不做（失败≠缺失）。返回 {"sl","tp1","tp2","claimed","repatched"}。"""
    from shared import binance_api as _bapi
    from shared.state import update_position
    from shared.guard_match import find_exchange_sl, find_exchange_tps, assign_tps_by_price

    sym = local.get("symbol")
    side = str(local.get("direction") or rp.get("positionSide") or "").upper()
    pid = local.get("pos_id", sym)
    out = {"sl": "unknown", "tp1": "unknown", "tp2": "unknown", "claimed": {}, "repatched": False}
    try:
        if algo_orders is None:
            algo_orders = (await asyncio.to_thread(_bapi.get_algo_open_orders)) or []
        if open_orders is None:
            open_orders = (await asyncio.to_thread(_bapi.get_open_orders)) or []
    except Exception as _qe:
        logger.warning(f"[guard_reattach] {sym}({side}) 查询交易所保护单失败，本次不动: {_qe}")
        out["sl"] = out["tp1"] = out["tp2"] = "query_failed"
        return out

    def _ids(orders, key):
        r = set()
        for o in orders or []:
            try:
                if o.get(key):
                    r.add(int(o[key]))
            except (TypeError, ValueError):
                pass
        return r

    def _alive(v, live):
        try:
            return bool(v) and int(v) in live
        except (TypeError, ValueError):
            return False

    live_ids = _ids(open_orders, "orderId")
    live_algo = _ids(algo_orders, "algoId")
    claims = {}

    # ── SL ──
    if _alive(local.get("sl_order_id"), live_algo):
        out["sl"] = "alive"
    else:
        hits = find_exchange_sl(algo_orders, sym, side)
        if len(hits) == 1:
            claims["sl_order_id"] = int(hits[0]["algoId"])
            out["sl"] = "claimed"
        elif len(hits) > 1:
            out["sl"] = "ambiguous"
        else:
            out["sl"] = "missing"

    # ── TP ──
    single = local.get("tp2_price") is None
    tp1_ok = _alive(local.get("tp1_order_id"), live_ids)
    tp2_ok = True if single else _alive(local.get("tp2_order_id"), live_ids)
    if not (tp1_ok and tp2_ok):
        taken = set()
        if tp1_ok:
            taken.add(int(local["tp1_order_id"]))
        if not single and tp2_ok:
            taken.add(int(local["tp2_order_id"]))
        cands = [o for o in find_exchange_tps(open_orders, sym, side) if int(o["orderId"]) not in taken]

        def _near(o, want):
            try:
                w = float(want or 0)
                return w > 0 and abs(float(o.get("price")) - w) / w <= 0.03
            except (TypeError, ValueError):
                return False
        a1, a2, by_price = assign_tps_by_price(cands,
                                               None if tp1_ok else local.get("tp1_price"),
                                               None if (single or tp2_ok) else local.get("tp2_price"), side)
        if by_price:
            if not tp1_ok and a1 and _near(a1, local.get("tp1_price")):
                claims["tp1_order_id"] = int(a1["orderId"])
                tp1_ok = True
            if not single and not tp2_ok and a2 and _near(a2, local.get("tp2_price")):
                claims["tp2_order_id"] = int(a2["orderId"])
                tp2_ok = True
    out["tp1"] = "alive_or_claimed" if tp1_ok else "missing"
    out["tp2"] = "n/a" if single else ("alive_or_claimed" if tp2_ok else "missing")

    if claims:
        try:
            await asyncio.to_thread(update_position, pid, claims)   # 只写 *_order_id，不碰任何业务字段
            local.update(claims)
            out["claimed"] = dict(claims)
            logger.info(f"[guard_reattach] {sym}({side}) 认领交易所已有保护单（仅回写订单号）: {claims}")
        except Exception as _ue:
            logger.warning(f"[guard_reattach] {sym}({side}) 回写订单号失败: {_ue}")

    sl_bad = out["sl"] in ("missing", "ambiguous")
    tp_bad = repatch_tp and not (tp1_ok and tp2_ok)
    status_bad = repatch_tp and local.get("status") != "open"
    if sl_bad or tp_bad or status_bad:
        from position_manager import _补挂保护单
        logger.info(f"[guard_reattach] {sym}({side}) sl={out['sl']} tp1_ok={tp1_ok} tp2_ok={tp2_ok} "
                    f"status={local.get('status')}，委托 position_manager 补挂")
        entry = float(rp.get("entryPrice") or local.get("entry_price") or 0)
        qty = abs(float(rp.get("positionAmt") or local.get("quantity") or 0))
        await _补挂保护单(local, entry, qty, pid, sym)
        out["repatched"] = True
    return out


_SYNC_LITE_COOLDOWN_MS = 5 * 60 * 1000     # 开仓冷却期：刚开/刚成交不足 5 分钟的仓位，交易所查不到也不删
_SYNC_LITE_MISSING_LIMIT = 2               # 连续多少次 sync_lite 都查不到才清理本地记录


def _local_age_ms(ps: dict, now_ms: int):
    """本地记录自开仓/成交以来的毫秒数；取 open_time 与 fill_time（秒或毫秒）里最新的一个。无法判断返回 None。"""
    ts = []
    for k in ("open_time", "fill_time"):
        try:
            v = float(ps.get(k) or 0)
        except (TypeError, ValueError):
            continue
        if v <= 0:
            continue
        ts.append(v * 1000 if v < 1e12 else v)      # fill_time 部分路径写的是秒
    return (now_ms - max(ts)) if ts else None


def _missing_on_exchange_decision(ps: dict, key: tuple, real: dict, now_ms: int, tag: str) -> str:
    """本地记录 vs 本轮交易所持仓：决定能否清理。sync_lite 与启动对齐共用，保证两处规则一致。
    返回：
      "present"  交易所查到了（若之前有缺失计数则清零）
      "cooldown" 开仓/成交不足 5 分钟，交易所暂未查到 → 不删、不计数
      "wait"     非冷却期第 1 次查不到 → 写 _exchange_missing_count，不删
      "remove"   连续第 2 次查不到 → 调用方执行清理
    调用方须保证本轮交易所持仓查询成功（查询失败时整轮跳过，根本不会走到这里，因此不计数）。"""
    from shared.state import update_position as _upd
    pid = ps.get("pos_id", ps.get("symbol"))
    if key in real:
        if ps.get("_exchange_missing_count"):
            try:
                _upd(pid, {"_exchange_missing_count": 0})
            except Exception:
                pass
        return "present"
    _age = _local_age_ms(ps, now_ms)
    if _age is not None and _age < _SYNC_LITE_COOLDOWN_MS:
        logger.info(f"[{tag}] {key[0]}({key[1]}) 开仓仅 {_age / 1000:.0f}s（冷却期内），交易所暂未查到，"
                    f"不清理不建孤儿 pos_id={pid}")
        return "cooldown"
    try:
        _cnt = int(ps.get("_exchange_missing_count") or 0) + 1
    except (TypeError, ValueError):
        _cnt = 1
    if _cnt < _SYNC_LITE_MISSING_LIMIT:
        try:
            _upd(pid, {"_exchange_missing_count": _cnt})
        except Exception:
            pass
        logger.warning(f"[{tag}] {key[0]}({key[1]}) 交易所未查到持仓（第{_cnt}/{_SYNC_LITE_MISSING_LIMIT}次），"
                       f"暂不清理 pos_id={pid}")
        return "wait"
    logger.warning(f"[{tag}] {key[0]}({key[1]}) 交易所连续{_cnt}次查不到持仓，清理本地记录 pos_id={pid} "
                   f"原因=missing_on_exchange")
    return "remove"


async def _startup_sync_lite() -> None:
    """轻量对齐（WS 重连后 / 每 30 分钟）：不补挂 TP、不推送卡片，只做两件事：
      1. 交易所有、本地无 → 孤儿建档 + 补挂保护单；超出当前市场模式持仓上限的，按开仓时间从新到旧只认领能填满上限的，
         其余直接市价平仓（不让无人管理的裸仓继续存在）。
      2. 交易所无、本地有 → 标记 closed 并删除记录。
    任何异常只记 warning，不影响主流程。"""
    global _sync_lite_busy
    if _sync_lite_busy:
        logger.info("[sync_lite] 上一轮仍在执行，跳过本次")
        return
    _sync_lite_busy = True
    added, removed, force_closed = 0, 0, 0
    try:
        from shared import binance_api as _bapi
        from shared.state import get_all_positions, add_position
        from shared.risk_manager import get_market_mode, _get_position_limits

        # 1) 交易所真实持仓；查询失败直接放弃本轮（失败≠无持仓，绝不清理）
        try:
            pos_risk = await asyncio.to_thread(_bapi.get_position, None)
            open_orders = await asyncio.to_thread(_bapi.get_open_orders)
            algo_orders = await asyncio.to_thread(_bapi.get_algo_open_orders)
        except Exception as _qe:
            logger.warning(f"[sync_lite] 查询交易所持仓/挂单失败，本轮跳过: {_qe}")
            return
        real = {(p["symbol"], p.get("positionSide")): p for p in (pos_risk or [])
                if p.get("positionSide") in ("LONG", "SHORT")
                and abs(float(p.get("positionAmt", 0) or 0)) > 0}

        positions = [p for p in get_all_positions() if isinstance(p, dict)]
        live_status = ("open", "pending_guard", "patching")

        # 2) 交易所无、本地有 → 清理（共用 _missing_on_exchange_decision：冷却期内不动；非冷却期需连续 2 次都查不到才删）
        _now_ms0 = int(time.time() * 1000)
        for ps in positions:
            if ps.get("status") not in live_status:
                continue
            key = (ps.get("symbol"), str(ps.get("direction", "")).upper())
            pid = ps.get("pos_id", ps.get("symbol"))
            if _missing_on_exchange_decision(ps, key, real, _now_ms0, "sync_lite") != "remove":
                continue
            try:
                from shared.state import update_position as _upd
                _upd(pid, {"status": "closed", "close_reason": "sync_lite_missing_on_exchange"})
            except Exception:
                pass
            from position_manager import _remove_position_checked
            await asyncio.to_thread(_remove_position_checked, pid, ps.get("symbol", ""))
            removed += 1

        # 3) 交易所有、本地无 → 孤儿（先按配额筛选）
        orphans = [(k, rp) for k, rp in real.items() if _match_local_position(positions, k[0], k[1]) is None]
        # 本地已有记录的仓位：原样保留，只核对保护单（SL 缺失才补挂；lite 不补 TP）
        for (k_sym, k_side), k_rp in real.items():
            _loc = _match_local_position(positions, k_sym, k_side)
            if _loc is None or _loc.get("status") != "open":
                continue
            try:
                await _check_and_reattach_guards(_loc, k_rp, algo_orders, open_orders, repatch_tp=False)
            except Exception as _ge:
                logger.warning(f"[sync_lite] {k_sym}({k_side}) 保护单核对失败: {_ge}")
        claimed = {"total": 0, "long": 0, "short": 0}
        for p in positions:
            if p.get("status") in _LOCAL_LIVE_STATUSES:
                claimed["total"] += 1
                d = str(p.get("direction", "")).upper()
                if d == "LONG":
                    claimed["long"] += 1
                elif d == "SHORT":
                    claimed["short"] += 1
        limits = _get_position_limits(get_market_mode())
        # 开仓时间：交易所持仓没有开仓时间字段，用 updateTime 倒序（越新越靠前）；缺失时排最后
        orphans.sort(key=lambda kv: float(kv[1].get("updateTime", 0) or 0), reverse=True)

        from position_manager import _补挂保护单
        for (sym, pos_side), rp in orphans:
            # 建档/配额平仓之前复核（交易所快照与本地读取之间隔着多次 await，仓位可能已被本地处理或已不存在）：
            #   ① 重读 pos_state，匹配到本地记录 → 不建孤儿；② 单独复查交易所该仓位仍存在，查询失败/已无 → 跳过（失败≠有仓）。
            try:
                _fresh = [p for p in get_all_positions() if isinstance(p, dict)]
                if _match_local_position(_fresh, sym, pos_side) is not None:
                    logger.info(f"[sync_lite] {sym}({pos_side}) 复核时已有本地记录，不建孤儿")
                    continue
                _chk = await asyncio.to_thread(_bapi.get_position, sym)
                if not any(c.get("symbol") == sym and c.get("positionSide") == pos_side
                           and abs(float(c.get("positionAmt", 0) or 0)) > 0 for c in (_chk or [])):
                    logger.info(f"[sync_lite] {sym}({pos_side}) 复核时交易所已无该持仓，不建孤儿")
                    continue
            except Exception as _re:
                logger.warning(f"[sync_lite] {sym}({pos_side}) 建孤儿前复核失败，本轮跳过: {_re}")
                continue
            _side = pos_side.lower()
            wants = {"total": 1, "long": 1 if _side == "long" else 0, "short": 1 if _side == "short" else 0}
            # (open_ms, symbol) 语义：这里用 0 占位，配额只看方向
            if not _claim_quota([(0, _side)], limits, claimed):
                qty = abs(float(rp.get("positionAmt", 0) or 0))
                try:
                    _close_side = "SELL" if pos_side == "LONG" else "BUY"
                    # 对冲模式：显式 positionSide 平仓（reduceOnly 语义由交易所按 positionSide 处理）
                    await asyncio.to_thread(_bapi.place_order, sym, _close_side, "MARKET", qty,
                                            position_side=pos_side)
                    force_closed += 1
                    logger.warning(f"[sync_lite] {sym}({pos_side}) 超出持仓上限，超出部分市价平仓 qty={qty}")
                except Exception as _ce:
                    logger.warning(f"[sync_lite] {sym}({pos_side}) 超出上限平仓失败（保留由人工处理）: {_ce}")
                continue
            try:   # [sync_lite-forensic] 只加日志：建孤儿前打印本地该标的全部记录（含所有 status）
                _ex = [p for p in get_all_positions() if isinstance(p, dict) and p.get("symbol") == sym]
                logger.warning(
                    f"[sync_lite-forensic] 准备建孤儿 {sym}({pos_side})，"
                    f"当前本地该标的全部记录(含所有status): "
                    f"{[{k: p.get(k) for k in ('pos_id', 'status', 'mode', 'source')} for p in _ex]}"
                )
            except Exception:
                pass
            _now_ms = int(time.time() * 1000)
            ps, _notes = _build_orphan_record(sym, pos_side, rp, algo_orders, open_orders, _now_ms)
            ps["source"] = "orphan_recovered_sync_lite"
            if not add_position(ps):
                logger.warning(f"[sync_lite] {sym}({pos_side}) 孤儿建档失败")
                continue
            added += 1
            claimed["total"] += wants["total"]
            claimed["long"] += wants["long"]
            claimed["short"] += wants["short"]
            logger.warning(f"[sync_lite] {sym}({pos_side}) 新增孤儿仓位: qty={ps['quantity']} entry={ps['entry_price']} "
                           f"{', '.join(_notes) if _notes else '交易所未读到保护单'}")
            try:
                await _补挂保护单(ps, ps["entry_price"], ps["quantity"], ps["pos_id"], sym)
            except Exception as _ge:
                logger.warning(f"[sync_lite] {sym}({pos_side}) 补挂保护单失败: {_ge}")

        logger.info(f"[sync_lite] 完成: 新增{added}笔 清理{removed}笔 超限平仓{force_closed}笔")
    except Exception as e:
        logger.warning(f"[sync_lite] 对齐异常: {e}")
    finally:
        _sync_lite_busy = False


async def _periodic_sync_loop(interval_sec: int = 1800) -> None:
    """每 30 分钟轻量对齐一次（首次错开一个周期，避免与启动对齐撞车）。"""
    await asyncio.sleep(interval_sec)
    while True:
        try:
            await _startup_sync_lite()
        except Exception as e:
            logger.warning(f"[periodic_sync] 定时对齐异常: {e}")
        await asyncio.sleep(interval_sec)


async def _sync_positions_on_startup() -> None:
    """
    启动时对齐持仓状态：
    1. 拉取交易所真实持仓和开放挂单
    2. 清除 pos_state 中已无持仓的僵尸记录
    3. 检查有持仓但缺 tp1/tp2 挂单的仓位，自动补挂
    4. 推送对齐摘要到 Jarvis
    """
    import math
    import time as _time

    try:
        sys.path.insert(0, str(ROOT))
        sys.path.insert(0, str(ROOT / "shared"))
        from shared import binance_api as _bapi
        from shared.state import get_all_positions, update_position, remove_position, cleanup_orphan_patching
        from executor import _place_guards_async, _get_precision
    except Exception as e:
        logger.error(f"[startup_sync] 模块导入失败: {e}")
        return

    # ── 圆山 孤儿 patching 清理（问题34）——进程崩溃/网络异常可能导致 patching 永远不恢复，
    # 造成该标的无法再开仓。开机时一次性强制清理。
    try:
        # 删除记录之前先关闭对应信号卡片（否则卡片永远悬在“补挂中”）
        try:
            from shared.state import list_orphan_patching
            from shared.notifier import card_append as _ca, card_remove as _cr, _hhmm as _hm
            for _op in list_orphan_patching():
                _sid = _op.get("_sig_id", "")
                if _sid:
                    try:
                        await asyncio.to_thread(_ca, _sid, f"\n⚠️ {_hm()} 开机自检：清除孤儿记录，监控结束")
                    except Exception:
                        pass
                    _cr(_sid)
        except Exception as _ce:
            logger.warning(f"[startup_sync] 孤儿记录关闭卡片失败（非致命）: {_ce}")
        orphans = cleanup_orphan_patching()
        if orphans:
            logger.warning(f"[startup_sync] 广清孤儿 patching: {orphans}")
            try:
                from shared.notifier import notify_async
                await notify_async(
                    f"⚠️ 开机自检：广清孤儿 patching 记录\n"
                    f"局标的: {', '.join(orphans)}\n"
                    f"已删除，该标的下轮扫描可正常开仓"
                )
            except Exception:
                pass
    except Exception as _op_e:
        logger.error(f"[startup_sync] orphan patching 清理失败（非致命）: {_op_e}")

    try:
        # ── 拉取真实持仓 ──────────────────────────────────────────────────────
        # [fix #2] 查询失败（ApiQueryError）≠ 交易所无持仓：直接中止对齐，不做任何清理
        try:
            pos_risk = await asyncio.to_thread(_bapi.get_position)  # 走 binance_api 封装：_retry + Session + key 轮询
            open_orders = await asyncio.to_thread(_bapi.get_open_orders)
        except Exception as _qe:
            logger.error(f"[startup_sync] 查询交易所持仓/挂单失败，本次对齐中止（不清理、不补挂）: {_qe}")
            try:
                from shared.notifier import notify_async
                await notify_async(f"⚠️ 持仓对齐中止：交易所持仓/挂单查询失败，未做任何清理\n{str(_qe)[:120]}")
            except Exception:
                pass
            return
        # HEDGE 模式：多单 positionSide=LONG，空单 positionSide=SHORT，两者都要对齐
        # 用 (symbol, positionSide) 作为 key，避免同一标的多空各一条记录时互相覆盖
        real = {(p["symbol"], p["positionSide"]): p for p in pos_risk
                if p.get("positionSide") in ("LONG", "SHORT")
                and abs(float(p.get("positionAmt", 0))) > 0}

        # ── 拉取开放挂单（普通限价单：TP）──────────────────────────────────────
        live_ids = {int(o["orderId"]) for o in open_orders}
        order_by_sym = {}
        for o in open_orders:
            order_by_sym.setdefault(o["symbol"], []).append(o)

        # ── v3.56: 拉取 Algo 止损单（SL），检查 SL 是否仍在交易所 ──────────────
        try:
            _algo_orders = (await asyncio.to_thread(_bapi.get_algo_open_orders)) or []
        except Exception as _ae:
            logger.warning(f"[startup_sync] 拉取 algo 止损单失败: {_ae}")
            _algo_orders = []
        live_algo_ids = {int(o.get("algoId", 0)) for o in _algo_orders if o.get("algoId")}

        positions = get_all_positions()
        ps_map = {p["symbol"]: p for p in positions}

        removed,補挂, errors = [], [], []
        orphans_rebuilt = []

        # ── 1. 清僵尸记录（仅在上面持仓/挂单查询都成功时才会走到这里）────────────
        # 与 sync_lite 共用 _missing_on_exchange_decision：开仓/成交 <5 分钟不删；其余需连续 2 次（含 sync_lite 的计数）查不到才删。
        _live_statuses = ("open", "pending_guard")
        _now_ms_z = int(_time.time() * 1000)
        for ps in list(positions):
            if ps.get("status") not in _live_statuses:
                continue
            sym = ps.get("symbol")
            ps_side = str(ps.get("direction") or "long").upper()  # pos_state 里存的是 direction
            if _missing_on_exchange_decision(ps, (sym, ps_side), real, _now_ms_z, "startup_sync") == "remove":
                # 交易所连续两次查不到此方向持仓，用 state.py 的加锁原子接口清除
                pid = ps.get("pos_id", sym)
                remove_position(pid)
                removed.append(sym)
                logger.info(f"[startup_sync] 清除僵尸记录: {sym} ({ps_side}) pos_id={pid}")

        # ── 2. 对齐每个真实持仓：孤儿建档 + 统一走 position_manager._补挂保护单 ──────
        # [fix #3] 孤儿仓位（交易所有、本地无）：用交易所数据重建完整记录并 add_position，
        #          方向取 positionSide，不使用默认值。
        # [fix #4/#13] 保护单补挂只走一条路径：position_manager._补挂保护单
        #          （SL 存活不重复挂；缺失先撤旧单；新 SL id 写回 pos_state）。
        from position_manager import _补挂保护单
        from shared.state import add_position
        _local_all = get_all_positions()

        for (sym, pos_side), rp in real.items():
            entry = float(rp["entryPrice"])
            qty   = abs(float(rp["positionAmt"]))
            # 先按 (symbol, direction) 匹配本地记录（open/pending/patching/pending_guard）：有则原样保留，只核对保护单；
            # 只有真正无记录才建孤儿档案。
            ps    = _match_local_position(_local_all, sym, pos_side)

            if ps is None:
                _now_ms = int(_time.time() * 1000)
                ps, _orphan_notes = _build_orphan_record(sym, pos_side, rp, _algo_orders, open_orders, _now_ms)
                if len(_orphan_notes) and any("歧义" in _n for _n in _orphan_notes):
                    logger.warning(f"[startup_sync] {sym}({pos_side}) {'; '.join(_orphan_notes)}")
                if not add_position(ps):
                    errors.append(f"{sym}({pos_side}) 孤儿仓位建档失败")
                    logger.error(f"[startup_sync] {sym}({pos_side}) 孤儿仓位建档失败，请人工核查")
                    continue
                orphans_rebuilt.append(f"{sym}({pos_side})")
                logger.warning(f"[startup_sync] {sym}({pos_side}) 孤儿仓位已按交易所数据建档: qty={qty} entry={entry} "
                               f"{'已读取保护单: ' + ', '.join(_orphan_notes) if _orphan_notes else '交易所未读到保护单'}")

            pid = ps.get("pos_id", sym)
            try:
                _r = await _check_and_reattach_guards(ps, rp, _algo_orders, open_orders)
                if _r.get("repatched"):
                    補挂.append(f"{sym}({pos_side})")
                elif _r.get("sl") == "alive" and _r.get("tp1") != "missing" and _r.get("tp2") != "missing":
                    logger.info(f"[startup_sync] {sym}: SL✅ TP1✅ {'(单TP)' if _r.get('tp2') == 'n/a' else 'TP2✅'} 无需补挂"
                                f"{'（孤儿仓位已建档并认领保护单 pos_id=' + str(pid) + '）' if ps.get('source') == 'orphan_recovered' else ''}")
            except Exception as e:
                errors.append(f"{sym}: {e}")
                logger.error(f"[startup_sync] {sym} 补挂失败: {e}")

        ps_map = {p["symbol"]: p for p in get_all_positions()}

        # ── 3. 推送摘要 ───────────────────────────────────────────────────────
        lines = [f"🔄 重启持仓对齐完成"]
        lines.append(f"  真实持仓: {len(real)} 笔 | pos_state: {len(positions)} 条")
        for (sym, pos_side), rp in real.items():
            entry = float(rp["entryPrice"])
            qty   = abs(float(rp["positionAmt"]))
            pnl   = float(rp["unRealizedProfit"])
            ps    = ps_map.get(sym, {})
            tp1_id = ps.get("tp1_order_id")
            tp2_id = ps.get("tp2_order_id")
            sl_p   = ps.get("sl_price")
            sl_str = f"{float(sl_p):.6g}" if sl_p is not None else "?"
            t1_ok  = "✅" if (tp1_id and int(tp1_id) in live_ids) else "❌"
            t2_ok  = "✅" if (tp2_id and int(tp2_id) in live_ids) else "❌"
            lines.append(f"  {sym}({pos_side}): entry={entry:.6g} qty={qty} PnL={pnl:+.2f}U")
            lines.append(f"    SL={sl_str}(patrol) TP1{t1_ok} TP2{t2_ok}")
        if orphans_rebuilt:
            lines.append(f"  🧩 孤儿仓位已建档: {', '.join(orphans_rebuilt)}")
        if removed:
            lines.append(f"  🗑 清除僵尸: {', '.join(removed)}")
        if 補挂:
            lines.append(f"  🔧 补挂TP: {', '.join(補挂)}")
        if errors:
            lines.append(f"  ⚠️ 补挂失败: {', '.join(errors)}")

        msg = "\n".join(lines)
        logger.info(f"[startup_sync] 摘要: {msg}")

        # 只在补挂失败（errors）时才推送，正常对齐和补挂全部静默
        if not errors:
            return
        try:
            from shared.notifier import notify_async
            err_msg = "⚠️ 重启对齐异常\n" + "\n".join(f"  {e}" for e in errors)
            await notify_async(err_msg)
        except Exception as e:
            logger.warning(f"[startup_sync] Jarvis 推送失败: {e}")

    except Exception as e:
        logger.error(f"[startup_sync] 对齐失败: {e}", exc_info=True)


async def _verify_hedge_mode_or_exit() -> None:
    """[fix #16] 启动时向交易所确认对冲模式；确认非对冲 → 拒绝启动；无法确认 → 告警后继续。
    （代码按对冲模式编写：positionSide=LONG/SHORT，单向模式下对账与保护单会全部错位。）"""
    from shared import binance_api as _b
    loop = asyncio.get_running_loop()
    try:
        dual = await loop.run_in_executor(None, _b.get_position_mode)
    except Exception as e:
        # [fix #16] 网络异常等无法确认时允许继续启动，仅告警；确认为单向模式才拒绝
        msg = f"⚠️ 暂时无法确认账户持仓模式，继续启动（请人工核实为对冲模式）：{str(e)[:120]}"
        logger.warning(f"[main] {msg}")
        try:
            from shared.notifier import notify_async
            await notify_async(msg)
        except Exception:
            pass
        return
    if not dual:
        msg = "🚨 账户当前为单向持仓模式，本系统要求对冲模式(Hedge Mode)，拒绝启动。请在币安合约设置中切换后重启。"
        logger.critical(f"[main] {msg}")
        try:
            from shared.notifier import notify_async
            await notify_async(msg)
        except Exception:
            pass
        raise SystemExit(2)
    logger.info("[main] 持仓模式校验通过：对冲模式")


async def _startup_p1_validate_account() -> None:
    """[启动分批 P1] 校验 API key：get_account 只调用一次。
    失败（含限频/网络抖动/返回空）只告警并继续，不阻断启动（持仓模式检查另有独立策略）。"""
    from shared import binance_api as _b
    loop = asyncio.get_running_loop()
    try:
        acc = await loop.run_in_executor(None, _b.get_account)
    except Exception as e:
        logger.warning(f"[main] P1 API key 校验异常（告警后继续）: {str(e)[:120]}")
        return
    if not acc:
        logger.warning("[main] P1 API key 校验：get_account 返回空（可能限频/网络/key 无权限），告警后继续")
        return
    logger.info("[main] P1 API key 校验通过")


async def _startup_wait(batch: str, cfg_name: str, default: float) -> float:
    """[启动分批] 批次间等待；返回实际等待秒数。配置缺失/非法时用默认值，<=0 不等待。"""
    try:
        import config as _c
        sec = float(getattr(_c, cfg_name, default))
    except Exception:
        sec = float(default)
    if sec > 0:
        logger.info(f"[main] 启动分批：等待 {sec:g}s 后进入 {batch}（{cfg_name}）")
        await asyncio.sleep(sec)
    return max(sec, 0.0)


def _startup_p3_warm() -> None:
    """[启动分批 P3] 预热 OI 缓存（读盘建立内存快照，不额外发 REST）并做首次系统性风险检查。
    任何异常只记日志，不影响启动（扫描阶段仍会再次检查）。"""
    try:
        from shared import state as _st
        _oi = _st._load_oi_cache()
        logger.info(f"[main] P3 OI 缓存已加载: {len(_oi or {})} 个标的")
    except Exception as e:
        logger.warning(f"[main] P3 OI 缓存预热失败（忽略）: {e}")
    try:
        from shared.risk_manager import check_systemic_risk
        ok, reason = check_systemic_risk()
        if ok:
            logger.info("[main] P3 系统性风险检查通过")
        else:
            logger.warning(f"[main] P3 系统性风险检查未通过：{reason}（扫描阶段将跳过开仓）")
    except Exception as e:
        logger.warning(f"[main] P3 系统性风险检查异常（忽略）: {e}")


async def main() -> None:
    logger.info("[main] fx 交易系统 V3.0 启动")
    logger.info(f"CPU核心数: {os.cpu_count()}")
    logger.info(f"[main] 事件循环: {type(asyncio.get_event_loop_policy()).__name__} / "
                f"loop={type(asyncio.get_running_loop()).__name__}")
    import config as _cfg_req
    _cfg_req.require_env()   # [fix #39] 必填项缺失直接报错退出
    # ── 启动分批 P1（立即）：API key 校验 + 持仓模式 ────────────────────────
    #   P2/P3/P4 见下文；批次间延迟见 config.STARTUP_BATCH_DELAY_P2/P3/P4
    await _startup_p1_validate_account()
    await _verify_hedge_mode_or_exit()
    # hub 先启动（建 WS 连接），但推迟首次 K 线订阅（订阅即触发历史 K 线 REST 加载），P4 再放开
    try:
        import scanner as _scn_hold
        _scn_hold.set_hub_subscribe_hold(True)
    except Exception as _he:
        logger.warning(f"[main] 设置 hub 订阅延迟失败（忽略）: {_he}")

    # ── 临时诊断（定位后删除）────────────────────────────────────────────────
    # 启动事件循环看门狗线程：检测阻塞 > 2s 时 dump 所有线程堆栈
    _wd = threading.Thread(target=_watchdog_thread_fn, daemon=True, name="watchdog")
    _wd.start()
    logger.info("[main] 事件循环看门狗线程已启动（阻塞 > 2s dump 堆栈）")

    # ── 1. 初始化 WebSocket 监听器 ──────────────────────────────────────────
    from shared.ws_listener import WsOrderListener
    ws = WsOrderListener()
    await ws.start()

    # ── 1.4 公共市场数据 hub（默认关闭；与 ws_listener 的 userData/forceOrder 连接完全隔离）──
    hub = None
    try:
        from config import MARKET_DATA_HUB_ENABLED as _HUB_ON
    except ImportError:
        _HUB_ON = False
    if _HUB_ON:
        try:
            from shared.market_data_hub import MarketDataHub
            hub = MarketDataHub()
            hub.start()
            for _ in range(100):               # 最多等 10 秒，不阻塞启动
                if hub.is_ready():
                    break
                await asyncio.sleep(0.1)
            if hub.is_ready():
                logger.info("[main] market_data_hub 已就绪")
            else:
                logger.warning("[main] market_data_hub 10 秒内未就绪，scanner 将回落 REST（不阻塞启动）")
        except Exception as e:
            logger.warning(f"[main] market_data_hub 启动失败（忽略，回落 REST）: {e}")

            hub = None

    # 阶段3：影子触发器（默认关闭；只读 hub 内存，不下单）
    try:
        from config import TRIGGER_DETECTOR_ENABLED as _TD_ON
    except ImportError:
        _TD_ON = False
    _trigger_det = None
    if _TD_ON and hub is not None:
        try:
            from shared import trigger_detector as _td_mod
            _trigger_det = _td_mod.create(hub=hub)
        except Exception as e:
            logger.warning(f"[main] trigger_detector 创建失败（忽略）: {e}")

    # ── 分析进程池预热（SCAN_USE_PROCESS_POOL=True 时；失败不影响启动，扫描时会回落线程池）──
    try:
        import config as _cfg_pp
        if getattr(_cfg_pp, "SCAN_USE_PROCESS_POOL", False):
            from shared import analysis_worker as _aw
            _ok = await asyncio.get_running_loop().run_in_executor(None, _aw.warmup)
            logger.info(f"[main] 分析进程池预热{'完成' if _ok else '失败（扫描时回落线程池）'}")
    except Exception as e:
        logger.warning(f"[main] 分析进程池预热异常（忽略）: {e}")

    # ── 启动分批 P2（P1 之后等待 STARTUP_BATCH_DELAY_P2）：持仓/挂单对齐 ───────
    await _startup_wait("P2", "STARTUP_BATCH_DELAY_P2", 5)
    # ── 1.5 启动时对齐持仓状态 ─────────────────────────────────────────────
    await _sync_positions_on_startup()

    # v3.33 注册 WS 重连回调：断线期间可能漏掉 TP/SL 成交事件，重连后主动对齐
    async def _on_ws_reconnect():
        logger.info("[main] WS 重连，主动触发持仓对齐...")
        await _sync_positions_on_startup()
    ws.on_reconnect(_on_ws_reconnect)

    # [fix #36] 注册断线补偿回调：userData 断线期间由 ws_listener 的有界补偿任务周期性 REST 对齐
    #   （此前 on_disconnect 从未注册，补偿任务永远不启动）。对齐函数查询失败时会整体中止，不会误清仓。
    async def _on_ws_disconnect():
        logger.info("[main] WS 断线补偿：REST 对齐持仓/保护单...")
        await _sync_positions_on_startup()
    ws.on_disconnect(_on_ws_disconnect)

    # ── 2. 初始化 PositionManager，注入 ws ────────────────────────────────
    from position_manager import PositionManager, _set_manager, _adjust_sl_tp_loop_static
    pm = PositionManager(ws_listener=ws)
    _set_manager(pm)
    await pm.start()

    # ── 2.5. market_data_hub WS 重连成功 → 轻量对齐（运行期 WS 断连期间成交的仓位靠这里兜底）──
    if hub is not None:
        try:
            _main_loop = asyncio.get_running_loop()
            _pending = {"task": None}

            def _on_ws_hub_reconnect():
                # hub 回调在其后台线程/task 中执行，转发回主事件循环
                def _spawn():
                    t = _pending["task"]
                    if t is not None and not t.done():
                        logger.info("[sync_lite] 上一次重连对齐仍未结束，合并本次")
                        return
                    logger.info("[main] market_data_hub WS 已恢复，触发轻量对齐...")
                    _pending["task"] = asyncio.ensure_future(_startup_sync_lite())
                try:
                    _main_loop.call_soon_threadsafe(_spawn)
                except Exception as _re:
                    logger.warning(f"[main] hub 重连回调调度失败（非致命）: {_re}")
            hub.set_reconnect_callback(_on_ws_hub_reconnect)
            logger.info("[main] market_data_hub 重连回调已注册")
        except Exception as e:
            logger.warning(f"[main] hub 重连回调注册失败（非致命）: {e}")

    # ── 3. 注册 executor 使用相同的 ws 实例 ───────────────────────────────
    #    executor 的 WsOrderListener 引用通过模块级 _LISTENER 变量注入
    try:
        import executor as _executor
        _executor._LISTENER = ws
        logger.info("[main] executor ws_listener 注入完成")
    except Exception as e:
        logger.warning(f"[main] executor ws_listener 注入失败（非致命）: {e}")

    # ── 3.5. 初始化 SignalWatcher（信号入场价格监控）─────────────────────────
    from shared.signal_watcher import SignalWatcher
    signal_watcher = SignalWatcher()
    signal_watcher._main_loop   = asyncio.get_event_loop()   # 主 loop，供子线程 run_coroutine_threadsafe 用
    signal_watcher._ws_listener = ws                          # 供 cancel_all 的 unsubscribe 用
    # 注册价格回调（WS 模式）同时也启动 REST 轮询（WS 不透传时作为保底）
    signal_watcher.register_to_ws(ws)
    await signal_watcher.start_gc()           # 后台 GC
    await signal_watcher.start_polling(1.5)   # REST 轮询（1.5s间隔，与 WS 互补）
    await signal_watcher.start_recheck(60)   # 每60s 复核 pending 挂单指标
    await signal_watcher.start_btc_guard(300) # 每5min 检查 BTC 大跌护卫
    logger.info("[main] SignalWatcher 初始化完成（WS + REST + recheck + btc_guard）")

    # ── 持仓维护任务（P2 内启动：止损/止盈/巡检；自带首次 sleep）──
    tasks = [
        # ── 临时诊断（定位后删除）────────────────────────────────────────────
        asyncio.create_task(_loop_heartbeat(),                                  name="heartbeat"),
        # ── 优先级 1：持仓维护（止损/止盈/巡检） ───────────────────────────────
        asyncio.create_task(_adjust_sl_tp_loop_static(),                       name="adjust_sl_tp"),
        asyncio.create_task(_loop_patrol(300, 30),                             name="patrol"),
    ]

    # ── 启动分批 P3（P2 之后等待 STARTUP_BATCH_DELAY_P3）：OI 缓存 + 系统性风险 ──
    await _startup_wait("P3", "STARTUP_BATCH_DELAY_P3", 8)
    await asyncio.get_running_loop().run_in_executor(None, _startup_p3_warm)

    # ── 启动分批 P4（P3 之后等待 STARTUP_BATCH_DELAY_P4）：hub K 线订阅 / 清算区预热 / 扫描等 ──
    await _startup_wait("P4", "STARTUP_BATCH_DELAY_P4", 12)
    try:
        import scanner as _scn_rel
        _scn_rel.set_hub_subscribe_hold(False)
        logger.info("[main] P4 已放开 hub K 线订阅（历史 K 线开始加载）")
    except Exception as _he:
        logger.warning(f"[main] 放开 hub 订阅失败（忽略）: {_he}")

    # ── 3.5 启动 liq_cache 后台刷新线程（每15分钟批量预热清算区缓存）──────────────
    try:
        from shared.liq_cache import start_bg_refresh
        import scanner as _scanner_mod
        import json as _json
        from pathlib import Path as _Path
        _pool_file = _Path(__file__).parent / "logs" / "candidate_pool.json"
        def _get_pool():
            try:
                return _json.loads(_pool_file.read_text()).get("pool", [])
            except Exception:
                return []
        start_bg_refresh(_get_pool)
        logger.info("[main] liq_cache 后台刷新线程已启动")
    except Exception as _lce:
        logger.warning(f"[main] liq_cache 启动失败（非致命）: {_lce}")

    tasks += [
        # ── 优先级 3：市场信号扫描 ────────────────────────────────────────────────
        asyncio.create_task(_loop_scan(300, signal_watcher=signal_watcher, ws_listener=ws), name="scan"),
        asyncio.create_task(_loop_funding_arb(1800, signal_watcher=signal_watcher), name="funding_arb"),
        # ── 基础支撑 ─────────────────────────────────────────────────────────────
        asyncio.create_task(_loop_refresh_pool(1800),                          name="refresh_pool"),
        asyncio.create_task(_loop_reset_circuit(),                             name="reset_circuit"),
        asyncio.create_task(_loop_daily_maintenance(),                         name="daily_maintenance"),
        asyncio.create_task(_loop_liq_snapshot(),                              name="liq_snapshot"),
        asyncio.create_task(_loop_event_latency_monitor(),                     name="loop_latency"),
        asyncio.create_task(_loop_track_outcomes(3600),                        name="track_outcomes"),
        asyncio.create_task(_periodic_sync_loop(1800),                         name="periodic_sync"),
    ]
    if _trigger_det is not None:
        _trigger_det._watcher = signal_watcher
        tasks.append(asyncio.create_task(_trigger_det.run(), name="trigger_detector"))
        tasks.append(asyncio.create_task(_loop_trigger_report(_trigger_det), name="trigger_report"))
        logger.info("[main] trigger_detector 已启动（影子模式，asyncio task）")

    # ── 5. 优雅退出 ─────────────────────────────────────────────────────────
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("[main] 收到退出信号，准备停止...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # Windows 不支持 add_signal_handler
            pass

    # SIGUSR1: 手动触发诊断快照
    def _handle_sigusr1():
        logger.info("[main] 收到 SIGUSR1，手动触发诊断快照...")
        import threading
        def _gen_snapshot():
            try:
                from archiver import archive_daily
                archive_daily(trigger="manual")
            except Exception as e:
                logger.error(f"[main] SIGUSR1 快照生成失败: {e}")
        threading.Thread(target=_gen_snapshot, daemon=True, name="sigusr1_snapshot").start()

    try:
        loop.add_signal_handler(signal.SIGUSR1, _handle_sigusr1)
    except (NotImplementedError, AttributeError):
        pass  # Windows 不支持

    # 捕获所有未处理的协程异常，防止 loop 意外退出
    def _handle_task_exception(loop, context):
        msg = context.get("exception", context["message"])
        logger.error(f"[main] 未捕获的协程异常: {msg}")

    loop.set_exception_handler(_handle_task_exception)

    logger.info("[main] 所有任务已启动，等待退出信号...")
    # 监控所有 task，任意一个提前结束就记录
    # [fix #37] 把 position_manager / signal_watcher / ws_listener 的内部后台 task 纳入监控：
    #   关键 task（持仓巡检、userData/keepalive/markPrice、挂单轮询）意外结束 → 与主 task 同样处理（触发退出由守护重启）；
    #   非关键 task（gc/health/forceOrder）只记录错误并推送一次。
    def _internal_tasks():
        crit, noncrit = [], []
        try:
            crit.extend(getattr(pm, "_tasks", []) or [])
        except Exception:
            pass
        for _name in ("_polling_task", "_recheck_task", "_btc_guard_task"):
            _t = getattr(signal_watcher, _name, None)
            if _t is not None:
                crit.append(_t)
        _g = getattr(signal_watcher, "_gc_task", None)
        if _g is not None:
            noncrit.append(_g)
        for _name in ("_user_data_task", "_mark_price_task", "_keepalive_task"):
            _t = getattr(ws, _name, None)
            if _t is not None:
                crit.append(_t)
        for _name in ("_force_order_task", "_health_task"):
            _t = getattr(ws, _name, None)
            if _t is not None:
                noncrit.append(_t)
        return crit, noncrit

    async def _watchdog():
        _reported = set()
        while True:
            await asyncio.sleep(10)
            done = [t for t in tasks if t.done()]
            _crit, _noncrit = _internal_tasks()
            done += [t for t in _crit if t.done()]
            for t in _noncrit:
                if t.done() and id(t) not in _reported:
                    _reported.add(id(t))
                    _exc = t.exception() if not t.cancelled() else None
                    logger.error(f"[main] WATCHDOG: 非关键内部 task {t.get_name()} 已结束 "
                                 f"cancelled={t.cancelled()} exc={_exc}")
            if done:
                for t in done:
                    exc = t.exception() if not t.cancelled() else None
                    logger.error(f"[main] WATCHDOG: task {t.get_name()} 意外结束 "
                                 f"cancelled={t.cancelled()} exc={exc}")
                stop_event.set()
                return
    watchdog_task = asyncio.create_task(_watchdog(), name="watchdog")

    try:
        await stop_event.wait()
        logger.info("[main] stop_event 已触发，开始停止流程")
    except asyncio.CancelledError:
        logger.info("[main] stop_event.wait() 被取消")
    finally:
        watchdog_task.cancel()

    # 停止
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    await pm.stop()
    try:
        await signal_watcher.stop_gc()
        await signal_watcher.stop_polling()
        await signal_watcher.stop_recheck()
        await signal_watcher.stop_btc_guard()
    except Exception:
        pass
    await ws.stop()
    if hub is not None:
        try:
            hub.stop()
        except Exception as e:
            logger.warning(f"[main] market_data_hub 停止异常: {e}")
        if _aw_stop is not None:
            try:
                _aw_stop.shutdown()
            except Exception:
                pass
    logger.info("[main] fx 系统已安全停止")


if __name__ == "__main__":
    import sys, traceback as _tb
    # ── 事件循环：可用时启用 uvloop。必须在 asyncio.run() 之前、logger 定义之后设置；
    #    不可用时明确告警（原先静默跳过，无法判断是否生效）──────────────────────
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        logger.warning("[main] uvloop 不可用（未安装），使用默认事件循环")
    except Exception as _ue:
        logger.warning(f"[main] uvloop 启用失败，使用默认事件循环: {_ue}")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[main] KeyboardInterrupt，正常退出")
    except SystemExit as e:
        logger.info(f"[main] SystemExit({e.code})")
    except Exception as e:
        logger.critical(f"[main] 未捕获异常导致退出: {e}", exc_info=True)
        sys.exit(1)
    logger.info("[main] asyncio.run 正常返回，进程退出")
