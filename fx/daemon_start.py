#!/usr/bin/env python3
"""
daemon_start.py — fx trading daemon 管理器 (v2.0 精确PID版)

核心改进（一次性永绝旧进程/旧数据问题）：
  1. 双 PID 文件：daemon.pid（守护进程）+ main.pid（main.py 工作进程）
     - 精确 kill，不用 pkill 模糊匹配
  2. 启动前强制清场：检查两个 PID 文件，任意活着的进程都先杀掉
  3. 竞态保护：等待旧进程完全退出（最多 15s）再启动新进程
  4. log rotate：system.log 超过 MAX_LOG_MB 自动归档，防无限膨胀
  5. _daemon_loop 在 Popen 后立即写 main.pid，stop 时精确杀
  6. cmd_stop 先按 main.pid 杀 main.py，再按 daemon.pid 杀 daemon，
     最后兜底 pkill（防止 PID 文件丢失）

用法：
  python3 daemon_start.py start    # 启动
  python3 daemon_start.py stop     # 停止
  python3 daemon_start.py restart  # 重启
  python3 daemon_start.py status   # 查状态
  python3 daemon_start.py clean    # 清理僵尸 PID 文件（不杀进程）
"""

from __future__ import annotations
import os, sys, time, signal, subprocess, json
from pathlib import Path
from datetime import datetime, timezone


def _load_dot_env() -> None:
    """启动前将 fx/.env（或上级 .env）注入 os.environ。
    优先级：已有系统环境变量 > .env 文件（不覆盖已设置的值）。
    不依赖任何第三方库，与 shared/binance_api.py 中的实现保持一致。
    """
    _fx_root = Path(__file__).parent
    for _env_path in [_fx_root / ".env", _fx_root.parent / ".env"]:
        if _env_path.exists():
            try:
                with open(_env_path) as _f:
                    for _line in _f:
                        _line = _line.strip()
                        if not _line or _line.startswith("#") or "=" not in _line:
                            continue
                        _k, _, _v = _line.partition("=")
                        _k = _k.strip()
                        _v = _v.strip().strip('"').strip("'")
                        if _k and _k not in os.environ:
                            os.environ[_k] = _v
            except Exception:
                pass
    # 仅在第一个可读的 .env 文件处停止


FX_DIR        = Path(__file__).parent
DAEMON_PID    = FX_DIR / "logs" / "daemon.pid"
MAIN_PID      = FX_DIR / "logs" / "main.pid"
LOG_FILE      = FX_DIR / "logs" / "daemon.log"
SYSTEM_LOG    = FX_DIR / "logs" / "system.log"
MAIN_PY       = FX_DIR / "main.py"

RESTART_DELAY  = 15     # 重启间隔（秒）
MAX_RESTARTS   = 500    # 最大重启次数
STOP_WAIT_SEC  = 15     # 等旧进程退出的最长秒数
MAX_LOG_MB     = 50     # system.log 超过此大小自动 rotate（MB）


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def log(msg: str):
    line = f"[{ts()}] {msg}\n"
    sys.stdout.write(line)
    sys.stdout.flush()
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line)
    except Exception:
        pass

def notify_jarvis(msg: str):
    try:
        subprocess.run(
            ["openclaw", "message", "send", "--channel", "jarvis", "--message", msg],
            timeout=10, capture_output=True,
        )
    except Exception:
        pass

def notify(msg: str):
    log(msg)
    notify_jarvis(msg)

def _read_pid(pid_file: Path) -> int | None:
    """读 PID 文件，失败返回 None。"""
    try:
        return int(pid_file.read_text().strip())
    except Exception:
        return None

# ── PID 身份校验（[v3.89 #104]）─────────────────────────────────────────────
# 原写法只用 os.kill(pid, 0)：PID 被系统复用后会把无关进程当成目标并发信号；
# PermissionError（进程存在但无权限）还被当成「不存在」。
# 现在：PID 文件旁边写 .meta（启动时间令牌 + 命令行特征），校验时同时核对；
# 严格区分 absent / alive / alive_no_permission / mismatch。

def _proc_start_token(pid: int) -> str | None:
    """进程启动时间令牌：Linux 取 /proc/<pid>/stat 第 22 字段，其他系统用 ps lstart。取不到返回 None。"""
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        return raw[raw.rindex(")") + 2:].split()[19]
    except Exception:
        pass
    try:
        r = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, timeout=5)
        out = r.stdout.strip()
        return out or None
    except Exception:
        return None

def _proc_cmdline(pid: int) -> str | None:
    """进程命令行（空格拼接）。取不到返回 None。"""
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").decode(errors="replace").strip() or None
    except Exception:
        pass
    try:
        r = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True, timeout=5)
        return r.stdout.strip() or None
    except Exception:
        return None

def _meta_path(pid_file: Path) -> Path:
    return Path(str(pid_file) + ".meta")

def _write_pid_file(pid_file: Path, pid: int, cmd_hint: str) -> None:
    """写 PID 文件，并在旁边写身份文件（启动时间令牌 + 命令行特征）。"""
    pid_file.write_text(str(pid))
    try:
        _meta_path(pid_file).write_text(json.dumps(
            {"pid": pid, "start": _proc_start_token(pid), "cmd": cmd_hint}))
    except Exception as e:
        log(f"[pid] 写身份文件失败（降级为仅命令行特征校验）: {e}")

def _expected_identity(pid_file: Path, default_cmd: str) -> dict:
    """读取期望身份；没有身份文件（旧版本写的 PID 文件）时只用默认命令行特征。"""
    try:
        m = json.loads(_meta_path(pid_file).read_text())
        return {"start": m.get("start"), "cmd": m.get("cmd") or default_cmd, "pid": m.get("pid")}
    except Exception:
        return {"start": None, "cmd": default_cmd, "pid": None}

def _pid_status(pid: int | None, expect: dict | None = None) -> str:
    """
    返回 absent / alive / alive_no_permission / mismatch。
      absent              进程不存在
      mismatch            PID 存在但启动时间或命令行与期望不符（PID 已被复用）
      alive_no_permission 进程存在但无权限发信号（不是「不存在」）
      alive               进程存在且身份相符（或没有可校验的期望）
    """
    if pid is None:
        return "absent"
    no_perm = False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "absent"
    except PermissionError:
        no_perm = True
    if expect:
        if expect.get("pid") not in (None, pid):
            return "mismatch"
        want_start = expect.get("start")
        if want_start:
            cur = _proc_start_token(pid)
            if cur is not None and cur != want_start:
                return "mismatch"
        want_cmd = expect.get("cmd")
        if want_cmd:
            cmd = _proc_cmdline(pid)
            if cmd is not None and want_cmd not in cmd:
                return "mismatch"
    return "alive_no_permission" if no_perm else "alive"

def _is_alive(pid: int | None, expect: dict | None = None) -> bool:
    """目标进程是否存活。PermissionError（存在但无权限）算存活；PID 被无关进程复用（mismatch）算不存在。"""
    return _pid_status(pid, expect) in ("alive", "alive_no_permission")

def _kill_pid(pid: int, label: str = "", wait_sec: int = STOP_WAIT_SEC, expect: dict | None = None) -> bool:
    """发 SIGTERM，等待退出，超时则 SIGKILL。返回是否成功停止。身份不符的 PID 不会被发送信号。"""
    st = _pid_status(pid, expect)
    if st == "absent":
        return True
    if st == "mismatch":
        log(f"  {label} (PID={pid}) 已不是目标进程（PID 被复用），不发送信号，视为已退出")
        return True
    if st == "alive_no_permission":
        log(f"  ❌ {label} (PID={pid}) 存在但无权限发送信号，未能停止")
        return False
    log(f"正在停止 {label} (PID={pid})...")
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except PermissionError:
        log(f"  ❌ {label} (PID={pid}) 无权限发送 SIGTERM，未能停止")
        return False

    deadline = time.time() + wait_sec
    while time.time() < deadline:
        time.sleep(0.5)
        if not _is_alive(pid, expect):
            log(f"  {label} (PID={pid}) 已优雅退出")
            return True

    # 超时，强杀（再次核对身份，避免等待期间 PID 被复用）
    if _pid_status(pid, expect) != "alive":
        return not _is_alive(pid, expect)
    log(f"  {label} (PID={pid}) 未在 {wait_sec}s 内退出，发送 SIGKILL")
    try:
        os.kill(pid, signal.SIGKILL)
        time.sleep(1)
    except ProcessLookupError:
        pass
    except PermissionError:
        log(f"  ❌ {label} (PID={pid}) 无权限发送 SIGKILL")
        return False
    return not _is_alive(pid, expect)

def _rotate_log():
    """system.log 超过 MAX_LOG_MB 时归档，防止无限膨胀。"""
    try:
        if not SYSTEM_LOG.exists():
            return
        size_mb = SYSTEM_LOG.stat().st_size / (1024 * 1024)
        if size_mb < MAX_LOG_MB:
            return
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        archive = SYSTEM_LOG.parent / "archive" / f"system_{stamp}.log"
        archive.parent.mkdir(parents=True, exist_ok=True)
        SYSTEM_LOG.rename(archive)
        log(f"[rotate] system.log ({size_mb:.1f}MB) 已归档为 {archive.name}")
    except Exception as e:
        log(f"[rotate] log rotate 失败: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 强制清场：杀掉所有旧进程
# ─────────────────────────────────────────────────────────────────────────────

def _stop_all_existing(verbose: bool = True) -> bool:
    """
    精确停止所有已运行的 daemon + main.py 进程。
    返回 True 表示清场成功（没有残留进程）。
    """
    stopped_all = True

    # 1. 按 main.pid 精确停 main.py
    main_pid = _read_pid(MAIN_PID)
    _main_exp = _expected_identity(MAIN_PID, "main.py")
    if _is_alive(main_pid, _main_exp):
        ok = _kill_pid(main_pid, "main.py", wait_sec=STOP_WAIT_SEC, expect=_main_exp)
        if not ok:
            if verbose: log(f"  ⚠️ main.py PID={main_pid} 未能停止")
            stopped_all = False
    MAIN_PID.unlink(missing_ok=True)
    _meta_path(MAIN_PID).unlink(missing_ok=True)

    # 2. 按 daemon.pid 精确停 daemon
    daemon_pid = _read_pid(DAEMON_PID)
    _dm_exp = _expected_identity(DAEMON_PID, "daemon_start.py")
    if _is_alive(daemon_pid, _dm_exp):
        ok = _kill_pid(daemon_pid, "daemon", wait_sec=5, expect=_dm_exp)
        if not ok:
            if verbose: log(f"  ⚠️ daemon PID={daemon_pid} 未能停止")
            stopped_all = False
    DAEMON_PID.unlink(missing_ok=True)
    _meta_path(DAEMON_PID).unlink(missing_ok=True)

    # 3. 兜底：pkill 扫描残余的 main.py 进程（防止 PID 文件丢失的情形）
    r = subprocess.run(
        ["pgrep", "-f", str(MAIN_PY)],
        capture_output=True, text=True,
    )
    leftover_pids = [int(p) for p in r.stdout.strip().split() if p.isdigit()]
    own_pid = os.getpid()
    for pid in leftover_pids:
        if pid == own_pid:
            continue
        if verbose: log(f"  兜底: 发现残余 main.py 进程 PID={pid}，强制清理")
        _kill_pid(pid, f"残余main.py({pid})", wait_sec=5, expect={"cmd": "main.py"})

    # 4. 同理兜底扫描 daemon_start.py 残余
    r2 = subprocess.run(
        ["pgrep", "-f", "daemon_start.py"],
        capture_output=True, text=True,
    )
    for pid_str in r2.stdout.strip().split():
        if not pid_str.isdigit():
            continue
        pid = int(pid_str)
        if pid == own_pid or pid == os.getppid():
            continue
        if verbose: log(f"  兜底: 发现残余 daemon 进程 PID={pid}，强制清理")
        _kill_pid(pid, f"残余daemon({pid})", wait_sec=3, expect={"cmd": "daemon_start.py"})

    return stopped_all


# ─────────────────────────────────────────────────────────────────────────────
# 守护进程主循环
# ─────────────────────────────────────────────────────────────────────────────

def _daemon_loop():
    """main.py 崩溃就重启，推送通知。"""
    DAEMON_PID.parent.mkdir(parents=True, exist_ok=True)
    _write_pid_file(DAEMON_PID, os.getpid(), "daemon_start.py")

    notify(f"✅ fx trading daemon 启动 (PID={os.getpid()})")

    restarts = 0
    while restarts < MAX_RESTARTS:
        # log rotate：每次启动前检查
        _rotate_log()

        start = time.time()
        log(f"启动 main.py (第{restarts+1}次)")

        SYSTEM_LOG.parent.mkdir(parents=True, exist_ok=True)
        # v3.43: 注入 LLM API Key（NovaAPI 兼容 OpenAI 格式），供 llm_judge 路径A使用
        _child_env = os.environ.copy()
        _child_env.setdefault("OPENAI_API_KEY",  os.environ.get("OPENAI_API_KEY", ""))
        _child_env.setdefault("OPENAI_BASE_URL", "https://novaapi.ai")
        # Binance API Key + 代理 — 从父进程环境变量继承，不在代码里硬编码
        # 启动前请确保已 export BINANCE_API_KEY / BINANCE_API_SECRET / BINANCE_PROXY
        for _k in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "BINANCE_PROXY", "PROXY_URL"):
            if _k not in _child_env and os.environ.get(_k):
                _child_env[_k] = os.environ[_k]
        with open(SYSTEM_LOG, "a") as out:
            proc = subprocess.Popen(
                [sys.executable, str(MAIN_PY)],
                cwd=str(FX_DIR),
                env=_child_env,
                stdout=subprocess.DEVNULL,   # main.py 只用 FileHandler 写日志，stdout 弃用
                stderr=out,                   # 仅 stderr（崩溃堆栈）追加到 system.log
                start_new_session=True,
            )

        # 写 main.pid，供精确 kill 使用
        _write_pid_file(MAIN_PID, proc.pid, "main.py")
        log(f"main.py 已启动 (PID={proc.pid})")

        # v3.82: 内存守护 —— 每 60s 检查一次 RSS，超过阈值主动发 SIGTERM 优雅重启
        # 目的：避免内存耗尽被 OOM killer SIGKILL（-9），改为主动优雅重启（exit=0）
        MEM_RESTART_MB = 3000   # 超过 3GB 主动重启（系统总内存 7.6GB，留 4.6GB 给其他进程）
        mem_restart_triggered = False
        while proc.poll() is None:
            time.sleep(60)
            if proc.poll() is not None:
                break
            try:
                rss_kb = int(open(f"/proc/{proc.pid}/status").read().split("VmRSS:")[1].split()[0])
                rss_mb = rss_kb // 1024
                if rss_mb >= MEM_RESTART_MB:
                    notify(f"⚠️ main.py 内存 {rss_mb}MB 超过 {MEM_RESTART_MB}MB 阈值，主动优雅重启...")
                    log(f"[mem-guard] RSS={rss_mb}MB >= {MEM_RESTART_MB}MB，发 SIGTERM")
                    import signal as _sig
                    proc.send_signal(_sig.SIGTERM)
                    mem_restart_triggered = True
                    break
            except Exception:
                pass
        proc.wait()
        elapsed = int(time.time() - start)
        exit_code = proc.returncode

        # 清理 main.pid（进程已退出）
        MAIN_PID.unlink(missing_ok=True)

        if mem_restart_triggered:
            notify(
                f"♻️ main.py 内存守护重启 (exit={exit_code}, 运行{elapsed}s)，"
                f"{RESTART_DELAY}s 后重启..."
            )
        elif elapsed < 30:
            notify(
                f"⚠️ main.py 异常退出！exit={exit_code}，运行仅 {elapsed}s。"
                f"{RESTART_DELAY}s 后第{restarts+2}次重启..."
            )
        else:
            notify(
                f"🔄 main.py 退出 (exit={exit_code}, 运行{elapsed}s)，"
                f"{RESTART_DELAY}s 后重启..."
            )

        time.sleep(RESTART_DELAY)
        restarts += 1

    notify(f"❌ 达到最大重启次数 {MAX_RESTARTS}，daemon 停止，需人工介入！")
    DAEMON_PID.unlink(missing_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Unix daemon 化
# ─────────────────────────────────────────────────────────────────────────────

def _daemonize() -> bool:
    """双重 fork，彻底脱离控制终端。返回 True 表示当前是 daemon 进程。"""
    pid = os.fork()
    if pid > 0:
        time.sleep(2)
        return False

    os.setsid()
    pid = os.fork()
    if pid > 0:
        os._exit(0)

    os.chdir(str(FX_DIR))
    os.umask(0)

    devnull = os.open("/dev/null", os.O_RDWR)
    os.dup2(devnull, 0)   # stdin  → /dev/null
    os.dup2(devnull, 1)   # stdout → /dev/null（main.py 不再用 stdout 写日志）
    os.dup2(devnull, 2)   # stderr → /dev/null（main.py 的 stderr 由 Popen 重定向）

    return True


# ─────────────────────────────────────────────────────────────────────────────
# 控制命令
# ─────────────────────────────────────────────────────────────────────────────

def cmd_start():
    # 检查是否已有活跃进程
    daemon_pid = _read_pid(DAEMON_PID)
    main_pid_val = _read_pid(MAIN_PID)
    if (_is_alive(daemon_pid, _expected_identity(DAEMON_PID, "daemon_start.py"))
            or _is_alive(main_pid_val, _expected_identity(MAIN_PID, "main.py"))):
        print(f"⚠️ 已有进程在运行 (daemon={daemon_pid}, main={main_pid_val})，先执行 stop 再 start")
        return

    # 清理失效 PID 文件
    DAEMON_PID.unlink(missing_ok=True)
    MAIN_PID.unlink(missing_ok=True)

    print("正在启动 fx trading daemon...")
    _load_dot_env()   # 确保 .env 已注入 os.environ，daemon fork 后子进程自动继承
    is_daemon = _daemonize()
    if is_daemon:
        _daemon_loop()
        sys.exit(0)
    else:
        for _ in range(20):
            if DAEMON_PID.exists():
                pid = DAEMON_PID.read_text().strip()
                print(f"✅ daemon 已启动 (PID={pid})")
                return
            time.sleep(0.5)
        print("⚠️ daemon 启动超时，检查 logs/daemon.log")


def cmd_stop():
    print("正在停止所有 fx 进程...")
    _stop_all_existing(verbose=True)
    print("✅ 所有进程已停止")


def cmd_restart():
    print("=== restart: 先 stop ===")
    _stop_all_existing(verbose=True)
    # 等待确认清场
    time.sleep(2)
    # 二次确认没有残余
    leftover = subprocess.run(
        ["pgrep", "-f", str(MAIN_PY)], capture_output=True, text=True
    ).stdout.strip()
    if leftover:
        print(f"⚠️ 仍有残余进程 {leftover}，强制清理后继续")
        for pid_str in leftover.split():
            if pid_str.isdigit():
                try: os.kill(int(pid_str), signal.SIGKILL)
                except: pass
        time.sleep(1)
    print("=== restart: 启动新进程 ===")
    cmd_start()


def cmd_status():
    daemon_pid = _read_pid(DAEMON_PID)
    main_pid_val = _read_pid(MAIN_PID)

    daemon_alive = _is_alive(daemon_pid, _expected_identity(DAEMON_PID, "daemon_start.py"))
    main_alive   = _is_alive(main_pid_val, _expected_identity(MAIN_PID, "main.py"))

    print(f"daemon : {'✅ 运行中' if daemon_alive else '❌ 未运行'} (PID={daemon_pid})")
    print(f"main.py: {'✅ 运行中' if main_alive  else '❌ 未运行'} (PID={main_pid_val})")

    if SYSTEM_LOG.exists():
        size_mb = SYSTEM_LOG.stat().st_size / (1024 * 1024)
        print(f"system.log: {size_mb:.1f}MB (rotate 阈值 {MAX_LOG_MB}MB)")

    if not daemon_alive and not main_alive:
        print("ℹ️  系统未运行，执行 start 启动")
    elif daemon_alive and not main_alive:
        print("⚠️  daemon 在跑但 main.py 不在，可能正在重启中（等 15s 后再检查）")
    elif not daemon_alive and main_alive:
        print("⚠️  main.py 在跑但 daemon 不在（孤儿进程），建议 restart")


def cmd_clean():
    """清理失效的 PID 文件（不杀进程）。"""
    for f, label in [(DAEMON_PID, "daemon.pid"), (MAIN_PID, "main.pid")]:
        if not f.exists():
            continue
        pid = _read_pid(f)
        if not _is_alive(pid, _expected_identity(f, "daemon_start.py" if f == DAEMON_PID else "main.py")):
            f.unlink(missing_ok=True)
            _meta_path(f).unlink(missing_ok=True)
            print(f"  清理失效 {label} (PID={pid})")
        else:
            print(f"  {label} 有效 (PID={pid} 在运行中)")


# ─────────────────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    cmd = sys.argv[1] if len(sys.argv) > 1 else "start"
    {
        "start":   cmd_start,
        "stop":    cmd_stop,
        "status":  cmd_status,
        "restart": cmd_restart,
        "clean":   cmd_clean,
    }.get(cmd, lambda: print(f"未知命令: {cmd}，可用: start/stop/restart/status/clean"))()