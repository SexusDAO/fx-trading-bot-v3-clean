"""多模型专家面板入口。

  python -m fx.tools.ai_panel.panel --run --questions "questions/A1*.txt"
  python -m fx.tools.ai_panel.panel --analyze --results results/2026-10-20/
"""
import argparse
import asyncio
import glob
import sys
from pathlib import Path

from . import analyzer, config, runner, storage


def _resolve_questions(patterns: list) -> list:
    files = []
    for pat in patterns:
        hits = glob.glob(pat)
        if not hits:
            hits = glob.glob(str(config.PANEL_QUESTIONS_DIR / pat))
        files.extend(sorted(hits))
    return [Path(f) for f in dict.fromkeys(files) if Path(f).is_file() and f.endswith(".txt")]


async def run_questions(files: list, models: list, directory: Path, force: bool = False, post=None) -> dict:
    summary = {}
    for f in files:                      # 题与题之间顺序执行（便于控制费用）；同一题内多模型并发
        qid, text = f.stem, f.read_text(encoding="utf-8").strip()
        todo_names = storage.models_to_run(storage.load(directory, qid), [m["name"] for m in models], force)
        if not todo_names:
            summary[qid] = "全部已有结果，跳过"
            continue
        todo = [m for m in models if m["name"] in todo_names]
        res = await runner.run_question(text, todo, config.PANEL_SYSTEM_PROMPT, post=post)
        storage.save(directory, qid, text, res)
        summary[qid] = {m: analyzer.quality_label(r) for m, r in res.items()}
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="多模型专家面板")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--run", action="store_true", help="调用 API 并保存结果")
    mode.add_argument("--analyze", action="store_true", help="读取本地结果做汇总分析")
    ap.add_argument("--questions", nargs="+", help="问题文件（glob 或路径，可写相对 questions/ 的路径）")
    ap.add_argument("--results", help="分析模式下的结果目录")
    ap.add_argument("--output", help="分析报告输出目录")
    ap.add_argument("--models", nargs="+", help="只跑这些模型（默认 config.PANEL_MODELS 全部）")
    ap.add_argument("--force", action="store_true", help="已有成功结果也重新调用")
    ap.add_argument("--date", help="结果日期目录（默认今天）")
    args = ap.parse_args(argv)

    if args.run:
        if not args.questions:
            ap.error("--run 需要 --questions")
        files = _resolve_questions(args.questions)
        if not files:
            print("没有找到问题文件", file=sys.stderr)
            return 2
        models = [m for m in config.PANEL_MODELS if not args.models or m["name"] in args.models]
        directory = storage.today_dir(config.PANEL_RESULTS_DIR, args.date)
        print(asyncio.run(run_questions(files, models, directory, args.force)))
        return 0

    if not args.results:
        ap.error("--analyze 需要 --results")
    print(asyncio.run(analyzer.analyze_dir(Path(args.results), Path(args.output) if args.output else None)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
