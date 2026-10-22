"""多模型专家面板：配置。

所有“固定不变”的内容集中在这里：API 地址、模型列表、系统提示、目录。改这里即全局生效。
密钥只从环境变量 PANEL_API_KEY 读取，不写进代码、不写进结果文件。
"""
import os
from pathlib import Path

_HERE = Path(__file__).resolve().parent

PANEL_BASE_URL = os.environ.get("PANEL_BASE_URL", "https://api.novaapi.ai/v1")


def get_api_key() -> str:
    """每次调用时读取环境变量，缺失时报错（不提供默认值）。"""
    key = os.environ.get("PANEL_API_KEY", "").strip()
    if not key:
        raise RuntimeError("未设置环境变量 PANEL_API_KEY")
    return key


# 每个模型：name / max_tokens / token_param（max_tokens 或 max_completion_tokens；缺省则按名字自动判断）。
# 推理型模型（o 系列、glm-5.x）会先消耗大量思考 token，max_tokens 要给足，否则会返回空回答且照常计费。
PANEL_MODELS = [
    {"name": "claude-opus-5",   "max_tokens": 3000},
    {"name": "gpt-6.1-sol",     "max_tokens": 3000, "token_param": "max_completion_tokens"},
    {"name": "glm-5.2",         "max_tokens": 4000},
    {"name": "claude-sonnet-4-6", "max_tokens": 3000},
    # o4-mini 已移除：13题里 9 次被标注回答偏简略/论证薄弱/质量偏低，参考价値不高
]

# 汇总分析使用的模型（analyzer 会调用一次）
# [说明] 原用 claude-sonnet-5，但在长问题上试跑出现“输出顶满上限却返回空回答 / 请求长时间不返回”，改用 claude-sonnet-4-6（价格略高，约 15 美元/百万输出）。
PANEL_ANALYZER_MODEL = {"name": "claude-sonnet-4-6", "max_tokens": 3000}

PANEL_SYSTEM_PROMPT = (
    "你是一位量化交易系统设计顾问，专注于加密合约策略的参数设计与风险评估。\n\n"
    "回答规则（必须遵守）：\n"
    "1. 按题目编号 1/2/3/4 逐题作答，每题独立一段\n"
    "2. 每题回答不超过 150 字\n"
    "3. 结论前置，理由简短跟随\n"
    "4. 不要重复题目内容\n"
    "5. 不要写开场白、总结段、过渡语\n"
    "6. 不确定的地方直接写\"不确定\"，不要用模糊措辞掩盖\n"
    "7. 禁止使用二级小标题\n"
    "8. 只给出你认为最重要的判断，不要列举所有可能性"
)

PANEL_RESULTS_DIR = Path(os.environ.get("PANEL_RESULTS_DIR", _HERE / "results"))
PANEL_QUESTIONS_DIR = Path(os.environ.get("PANEL_QUESTIONS_DIR", _HERE / "questions"))

PANEL_REQUEST_TIMEOUT = 120   # 单次请求秒数
PANEL_MAX_RETRIES = 2         # 失败最多重试次数
PANEL_RETRY_DELAY = 3         # 重试间隔秒数
PANEL_MAX_CONCURRENCY = 5     # 同时在途的请求数上限，避免中转站限速（429）
