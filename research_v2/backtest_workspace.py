"""Offline research job entry point. Operational settings and databases are not inputs."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


SCHEMA = "research-backtest-job-v1"
FIELDS = {"schema", "strategy_id", "columns_csv", "columns_sha256",
          "candles_csv", "candles_sha256", "minimum_entry_ts", "output_root"}
EXECUTABLE = frozenset({"causal_long_pole"})


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
    if type(job) is not dict or set(job) != FIELDS or job["schema"] != SCHEMA:
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
    return job


def create_job(columns: Path, candles: Path, minimum_entry_ts: int,
               output: Path, job_file: Path) -> dict:
    """Freeze user-selected inputs into an exclusive, portable job record."""
    columns, candles, output, job_file = (p.resolve() for p in (columns, candles, output, job_file))
    if not columns.is_file() or not candles.is_file() or columns == candles:
        raise ValueError("distinct existing research CSV inputs required")
    if columns.suffix.lower() != ".csv" or candles.suffix.lower() != ".csv":
        raise ValueError("CSV research inputs required")
    if type(minimum_entry_ts) is not int or minimum_entry_ts < 10**12:
        raise ValueError("invalid warm-up boundary")
    if output.exists() or job_file.exists() or not output.parent.is_dir() or not job_file.parent.is_dir():
        raise ValueError("new output and job paths required")
    job = {"schema": SCHEMA, "strategy_id": "causal_long_pole",
           "columns_csv": str(columns), "columns_sha256": _sha(columns),
           "candles_csv": str(candles), "candles_sha256": _sha(candles),
           "minimum_entry_ts": minimum_entry_ts, "output_root": str(output)}
    with job_file.open("x", encoding="utf-8") as stream:
        json.dump(job, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return job


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

    # Import only after validation. This explicit dispatch is not a dynamic
    # import of an unreviewed strategy from the inventory JSON.
    from research_v2.patterns.pole_causal_long_research import run

    result = run(columns, candles, output, job["minimum_entry_ts"],
                 job["columns_sha256"], job["candles_sha256"])
    if _sha(columns) != job["columns_sha256"] or _sha(candles) != job["candles_sha256"]:
        raise ValueError("frozen input changed during run")
    provenance = {"schema": SCHEMA, "strategy_id": job["strategy_id"],
                  "job_sha256": _sha(path), "columns_sha256": job["columns_sha256"],
                  "candles_sha256": job["candles_sha256"],
                  "minimum_entry_ts": job["minimum_entry_ts"],
                  "research_only": True, "result_manifest": "causal_manifest.json"}
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
