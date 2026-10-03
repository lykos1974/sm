"""Offline research job entry point. Operational settings and databases are not inputs."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


SCHEMA = "research-backtest-job-v1"
PERIOD_SCHEMA = "research-backtest-job-v2"
FIELDS = {"schema", "strategy_id", "columns_csv", "columns_sha256",
          "candles_csv", "candles_sha256", "minimum_entry_ts", "output_root"}
PERIOD_FIELDS = FIELDS | {"maximum_entry_ts"}
EXECUTABLE = frozenset({"causal_long_pole"})
BTC_2024_COLUMNS_SHA = "ed263ac77c3b2ed7169006d669ee1c9d21382ac02a2b37f107f2bcb635e47a45"
BTC_2024_CANDLES_SHA = "8045aa135a611d4b4fc2ca0cde9a8fa68905ad4480054399f33ed77ab8f6851f"
BTC_2024_MINIMUM_ENTRY_TS = 1704240000000
CSV_HEADERS = {
    "columns": frozenset({"symbol", "profile_name", "idx", "kind", "top", "bottom", "start_ts", "end_ts"}),
    "candles": frozenset({"close_time", "open", "high", "low", "close"}),
}


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _check_csv_header(path: Path, kind: str) -> None:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        fields = next(csv.reader(stream), [])
    if len(fields) != len(set(fields)) or not CSV_HEADERS[kind].issubset(fields):
        raise ValueError(f"wrong {kind} CSV format")


def suggest_output_directory(parent: Path) -> Path:
    if not parent.is_dir():
        raise ValueError("existing output parent required")
    for _ in range(10):
        name = datetime.now(timezone.utc).strftime("BTC_2024_backtest_%Y%m%d_%H%M%S_%f")
        candidate = parent / f"{name}_{uuid4().hex[:8]}"
        if not candidate.exists() and not candidate.with_suffix(".job.json").exists():
            return candidate
    raise FileExistsError("could not allocate output name")


def load_job(path: Path) -> dict:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate job key")
            result[key] = value
        return result

    with path.open(encoding="utf-8") as stream:
        job = json.load(stream, object_pairs_hook=unique)
    if (type(job) is not dict or
            (job.get("schema"), set(job)) not in
            ((SCHEMA, FIELDS), (PERIOD_SCHEMA, PERIOD_FIELDS))):
        raise ValueError("unsupported job schema")
    if job["strategy_id"] not in EXECUTABLE:
        raise ValueError("strategy has no reviewed execution adapter")
    for key in ("columns_csv", "candles_csv", "output_root"):
        value = job[key]
        if type(value) is not str or not value or "\x00" in value or not Path(value).is_absolute():
            raise ValueError("absolute research path required")
    for key in ("columns_sha256", "candles_sha256"):
        value = job[key]
        if type(value) is not str or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("canonical SHA-256 required")
    if type(job["minimum_entry_ts"]) is not int or job["minimum_entry_ts"] < 10**12:
        raise ValueError("invalid warm-up boundary")
    if job["schema"] == PERIOD_SCHEMA and (type(job["maximum_entry_ts"]) is not int
            or job["maximum_entry_ts"] <= job["minimum_entry_ts"]):
        raise ValueError("invalid entry period")
    return job


def create_job(columns: Path, candles: Path, minimum_entry_ts: int,
               output: Path, job_file: Path, maximum_entry_ts: int | None = None) -> dict:
    """Freeze user-selected inputs into an exclusive, portable job record."""
    columns, candles, output, job_file = (p.resolve() for p in (columns, candles, output, job_file))
    if not columns.is_file() or not candles.is_file() or columns == candles:
        raise ValueError("distinct existing research CSV inputs required")
    if columns.suffix.lower() != ".csv" or candles.suffix.lower() != ".csv":
        raise ValueError("CSV research inputs required")
    _check_csv_header(columns, "columns")
    _check_csv_header(candles, "candles")
    if type(minimum_entry_ts) is not int or minimum_entry_ts < 10**12:
        raise ValueError("invalid warm-up boundary")
    if maximum_entry_ts is not None and (type(maximum_entry_ts) is not int
                                          or maximum_entry_ts <= minimum_entry_ts):
        raise ValueError("invalid entry period")
    if output.exists() or job_file.exists() or not output.parent.is_dir() or not job_file.parent.is_dir():
        raise ValueError("new output and job paths required")
    job = {"schema": PERIOD_SCHEMA if maximum_entry_ts is not None else SCHEMA,
           "strategy_id": "causal_long_pole",
           "columns_csv": str(columns), "columns_sha256": _sha(columns),
           "candles_csv": str(candles), "candles_sha256": _sha(candles),
           "minimum_entry_ts": minimum_entry_ts, "output_root": str(output)}
    if maximum_entry_ts is not None:
        job["maximum_entry_ts"] = maximum_entry_ts
    with job_file.open("x", encoding="utf-8") as stream:
        json.dump(job, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return job


def prepare_btc_2024_job(dataset: Path, start_date: str | None = None,
                         end_date: str | None = None) -> Path:
    """One-folder GUI workflow for the hash-pinned BTC 2024 reference."""
    if not dataset.is_dir():
        raise ValueError("choose the frozen BTC 2024 results folder")
    columns = dataset / "columns.csv"
    candles = dataset / "candles_1m.csv"
    if not columns.is_file() or not candles.is_file():
        raise ValueError("BTC 2024 frozen files missing in selected folder")
    _check_csv_header(columns, "columns")
    _check_csv_header(candles, "candles")
    if _sha(columns) != BTC_2024_COLUMNS_SHA or _sha(candles) != BTC_2024_CANDLES_SHA:
        raise ValueError("BTC 2024 frozen input hash mismatch")
    if (start_date is None) != (end_date is None):
        raise ValueError("both UTC dates required")
    minimum = BTC_2024_MINIMUM_ENTRY_TS
    maximum = None
    if start_date is not None:
        from datetime import date, timedelta
        def parse_date(value):
            if type(value) is not str or len(value) != 10:
                raise ValueError("UTC dates must be YYYY-MM-DD")
            parsed = date.fromisoformat(value)
            if parsed.isoformat() != value:
                raise ValueError("UTC dates must be YYYY-MM-DD")
            return parsed
        first, last = parse_date(start_date), parse_date(end_date)
        if not (date(2024, 1, 3) <= first <= last <= date(2024, 12, 31)):
            raise ValueError("period outside frozen BTC 2024 entry window")
        minimum = int(datetime(first.year, first.month, first.day, tzinfo=timezone.utc).timestamp() * 1000)
        maximum_date = last + timedelta(days=1)
        maximum = int(datetime(maximum_date.year, maximum_date.month, maximum_date.day,
                               tzinfo=timezone.utc).timestamp() * 1000)
    output = suggest_output_directory(dataset.parent.parent)
    job_file = output.with_suffix(".job.json")
    create_job(columns, candles, minimum, output, job_file, maximum)
    return job_file


def run_job(path: Path) -> dict:
    job = load_job(path)
    columns = Path(job["columns_csv"]).resolve(strict=True)
    candles = Path(job["candles_csv"]).resolve(strict=True)
    output = Path(job["output_root"]).resolve()
    if columns == candles or not columns.is_file() or not candles.is_file():
        raise ValueError("distinct frozen CSV files required")
    if output.exists() or output == columns or output == candles:
        raise FileExistsError("new research output directory required")
    if _sha(columns) != job["columns_sha256"] or _sha(candles) != job["candles_sha256"]:
        raise ValueError("frozen input hash mismatch")
    _check_csv_header(columns, "columns")
    _check_csv_header(candles, "candles")

    # Import only after validation. This explicit dispatch is not a dynamic
    # import of an unreviewed strategy from the inventory JSON.
    from research_v2.patterns.pole_causal_long_research import run

    result = run(columns, candles, output, job["minimum_entry_ts"],
                 job["columns_sha256"], job["candles_sha256"],
                 maximum_entry_ts=job.get("maximum_entry_ts"))
    if _sha(columns) != job["columns_sha256"] or _sha(candles) != job["candles_sha256"]:
        raise ValueError("frozen input changed during run")
    provenance = {"schema": job["schema"], "strategy_id": job["strategy_id"],
                  "job_sha256": _sha(path), "columns_sha256": job["columns_sha256"],
                  "candles_sha256": job["candles_sha256"],
                  "minimum_entry_ts": job["minimum_entry_ts"],
                  "research_only": True, "result_manifest": "causal_manifest.json"}
    if job["schema"] == PERIOD_SCHEMA:
        provenance["maximum_entry_ts"] = job["maximum_entry_ts"]
        provenance["entry_cohort_policy"] = "UTC start inclusive, end exclusive; later exits allowed"
    with (output / "research_job_manifest.json").open("x", encoding="utf-8") as stream:
        json.dump(provenance, stream, sort_keys=True, indent=2)
        stream.write("\n")
    return {"output_root": str(output), "job": provenance, "result": result}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_job(args.job), indent=2))


if __name__ == "__main__":
    main()
