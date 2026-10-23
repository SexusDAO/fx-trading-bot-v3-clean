#!/bin/bash
# fx 交易系统守护脚本 + 崩溃通知
# 用法：bash watchdog.sh &

FX_DIR="/root/.openclaw/workspace/fx"
LOG="$FX_DIR/logs/watchdog.log"
LOCK="$FX_DIR/watchdog.lock"
MAX_RESTARTS=200
RESTART_DELAY=15      # 重启等待（秒）
NOTIFY_URL=""         # Telegram 通知（下面自动从 config 读）

mkdir -p "$FX_DIR/logs"

# 防重复
if [ -f "$LOCK" ]; then
    OLD_PID=$(cat "$LOCK")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "[watchdog] 已有实例运行 (PID=$OLD_PID)，退出"
        exit 1
    fi
fi
echo $$ > "$LOCK"
trap "rm -f '$LOCK'" EXIT

ts() { date -u '+%Y-%m-%d %H:%M UTC'; }

# 读 Telegram 配置
read_cfg() {
    python3 -c "
import sys; sys.path.insert(0,'$FX_DIR')
try:
    import config as c
    print(getattr(c,'TELEGRAM_BOT_TOKEN',''), getattr(c,'TELEGRAM_CHAT_ID',''))
except: print('','')
" 2>/dev/null
}

send_telegram() {
    local msg="$1"
    local cfg
    cfg=$(read_cfg)
    local token chat_id
    token=$(echo "$cfg" | awk '{print $1}')
    chat_id=$(echo "$cfg" | awk '{print $2}')
    [ -z "$token" ] || [ -z "$chat_id" ] && return
    # [fix #39] 令牌与消息经环境变量传入，不拼进命令行（避免 ps 可见与引号注入）
    TG_TOKEN="$token" TG_CHAT_ID="$chat_id" TG_MSG="$msg" python3 -c "
import urllib.request, json, os
token=os.environ['TG_TOKEN']; chat_id=os.environ['TG_CHAT_ID']
msg=os.environ['TG_MSG']
payload=json.dumps({'chat_id':chat_id,'text':msg}).encode()
req=urllib.request.Request(
    f'https://api.telegram.org/bot{token}/sendMessage',
    data=payload, headers={'Content-Type':'application/json'})
try:
    urllib.request.urlopen(req, timeout=10)
except Exception as e:
    pass
" 2>/dev/null
}

# ── OpenClaw Jarvis 通知（直接写消息到用户会话）────────────────────────────
notify_jarvis() {
    local msg="$1"
    openclaw message send --channel jarvis --message "$msg" 2>/dev/null || true
}

notify() {
    local msg="$1"
    echo "[$(ts)] $msg" | tee -a "$LOG"
    notify_jarvis "$msg"
    send_telegram "🚨 fx watchdog: $msg"
}

notify "watchdog 启动 (PID=$$)"

restarts=0
while [ $restarts -lt $MAX_RESTARTS ]; do
    START_TS=$(date +%s)
    cd "$FX_DIR"
    echo "[$(ts)] 启动 main.py (第$((restarts+1))次)" | tee -a "$LOG"

    # v3.41.2: NovaAPI 直连（OpenAI 兼容中转站，速度 <1s，绕过 openclaw agent 慢路径）
    # 模型对比结果：gpt-4o-mini 有妖币识别+新闻语义两处误判，换 gpt-4o（速度持平，判断准确率 3/3）
    export OPENAI_API_KEY="${OPENAI_API_KEY:?请在运行环境中设置 OPENAI_API_KEY}"
    export OPENAI_BASE_URL="https://novaapi.ai"
    export OPENAI_MODEL="claude-opus-4-8-medium"  # 默认（妖币+边界），新闻语义见 LLM_API_MODEL_FAST

    # Binance API Key + 代理 — 必须在环境变量中提前设置，不在脚本里硬编码
    # export BINANCE_API_KEY="your_key"
    # export BINANCE_API_SECRET="your_secret"
    # export BINANCE_PROXY="socks5://user:pass@host:port"
    : "${BINANCE_API_KEY:?请设置 BINANCE_API_KEY 环境变量}"
    : "${BINANCE_API_SECRET:?请设置 BINANCE_API_SECRET 环境变量}"
    : "${BINANCE_PROXY:?请设置 BINANCE_PROXY 环境变量}"
    export PROXY_URL="${BINANCE_PROXY}"

    python3 main.py >> "$LOG" 2>&1
    EXIT_CODE=$?

    END_TS=$(date +%s)
    UPTIME=$(( END_TS - START_TS ))

    if [ $UPTIME -lt 30 ]; then
        # 进程启动不足30秒就退了，可能是严重错误
        notify "⚠️ main.py 异常退出 exit=$EXIT_CODE，运行仅 ${UPTIME}s，${RESTART_DELAY}s 后第$((restarts+2))次重启"
    else
        notify "🔄 main.py 退出 exit=$EXIT_CODE (运行${UPTIME}s)，${RESTART_DELAY}s 后重启..."
    fi

    sleep $RESTART_DELAY
    restarts=$((restarts + 1))
done

notify "❌ 达到最大重启次数 $MAX_RESTARTS，watchdog 停止，需人工介入！"
rm -f "$LOCK"
