"""结果存取：每道题一个 JSON，按日期分目录，支持增量写入与多次运行合并。"""
import datetime
import json
from pathlib import Path


def today_dir(base: Path, date: str = None) -> Path:
    d = Path(base) / (date or datetime.date.today().isoformat())
    d.mkdir(parents=True, exist_ok=True)
    return d


def result_path(directory: Path, question_id: str) -> Path:
    return Path(directory) / f"{question_id}.json"


def load(directory: Path, question_id: str) -> dict:
    p = result_path(directory, question_id)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}


def is_done(response: dict) -> bool:
    """已有成功结果：有回答且没有错误。"""
    return bool(response) and bool(response.get("answer")) and not response.get("error")


def models_to_run(existing: dict, model_names: list, force: bool = False) -> list:
    """增量逻辑：已有成功结果的模型跳过，除非 force。"""
    if force:
        return list(model_names)
    done = (existing or {}).get("responses", {})
    return [m for m in model_names if not is_done(done.get(m))]


def save(directory: Path, question_id: str, question_text: str, new_responses: dict) -> dict:
    """合并写入：同一问题的多次运行合并到同一文件；同模型以最新一次结果覆盖。"""
    data = load(directory, question_id) or {"question_id": question_id, "responses": {}}
    data["question_text"] = question_text
    data["run_at"] = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    for name, resp in new_responses.items():
        resp = dict(resp)
        resp["run_at"] = data["run_at"]
        data["responses"][name] = resp
    result_path(directory, question_id).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def load_all(directory: Path) -> dict:
    """读取目录下全部题目结果，{question_id: data}。analysis 子目录与非 JSON 忽略。"""
    out = {}
    for p in sorted(Path(directory).glob("*.json")):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            out[d.get("question_id", p.stem)] = d
        except Exception:
            continue
    return out
