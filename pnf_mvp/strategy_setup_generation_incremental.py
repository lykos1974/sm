from __future__ import annotations

import argparse

from setup_generation_runner import run_incremental_setup_generation


def str_to_bool(value: str) -> bool:
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase 1 research setup generation with checkpoint/resume")
    parser.add_argument("--settings", default="settings.research_clean.json")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols exactly as they appear in settings")
    parser.add_argument("--warmup-start", required=True, help="UTC date YYYY-MM-DD or ISO timestamp")
    parser.add_argument("--analysis-start", required=True, help="UTC date YYYY-MM-DD or ISO timestamp")
    parser.add_argument("--analysis-end", required=True, help="UTC date YYYY-MM-DD or ISO timestamp")
    parser.add_argument("--output-csv", default="exports/generated_setups_incremental.csv")
    parser.add_argument("--checkpoint-root", required=True)
    parser.add_argument("--force-rebuild", action="store_true")
    parser.add_argument("--include-rejects", type=str_to_bool, default=True)
    parser.add_argument("--include-watch", type=str_to_bool, default=True)
    parser.add_argument("--include-candidates", type=str_to_bool, default=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    run_incremental_setup_generation(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
