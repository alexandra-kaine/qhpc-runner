"""Turn a finished experiment into results: raw CSV, summary CSV, Markdown, chart.

The workload reports its own measurement by printing one line such as:

    QHPC_RESULT threads=4 steps=200000000 elapsed_sec=1.234567 value=...

The report groups repeats by their matrix parameters (everything except
`repeat`), takes the median of each group, and, when the experiment varies
one numeric parameter, computes speedup and parallel efficiency against the
smallest value of that parameter.
"""
from __future__ import annotations

from pathlib import Path
from statistics import median
import csv
import json


def _parse_result_line(text: str) -> dict[str, str] | None:
    result, node = None, None
    for line in text.splitlines():
        if line.startswith("QHPC_RUNNER_NODE="):
            node = line.split("=", 1)[1].strip()
        if line.startswith("QHPC_RESULT"):
            result = dict(tok.split("=", 1) for tok in line.split()[1:] if "=" in tok)
    if result is not None and node:
        result["node"] = node
    return result


def collect(db, exp_id: int, metric: str = "elapsed_sec") -> list[dict]:
    rows = []
    for task in db.tasks_for_experiment(exp_id):
        if task["status"] != "COMPLETED":
            continue
        attempt = db.latest_attempt(task["id"])
        if attempt is None or not attempt["slurm_job_id"]:
            continue
        log = Path(attempt["log_path"].replace("%j", str(attempt["slurm_job_id"])))
        if not log.exists():
            continue
        parsed = _parse_result_line(log.read_text(errors="replace"))
        if not parsed or metric not in parsed:
            continue
        rows.append({
            "task": task["task_key"],
            "params": json.loads(task["parameters_json"]),
            "job_id": attempt["slurm_job_id"],
            "node": parsed.get("node", ""),
            "max_rss_mb": attempt["max_rss_mb"],
            metric: float(parsed[metric]),
        })
    return rows


def summarize(rows: list[dict], metric: str = "elapsed_sec") -> tuple[list[dict], str | None]:
    groups: dict[tuple, list[float]] = {}
    for r in rows:
        key = tuple(sorted((k, v) for k, v in r["params"].items() if k != "repeat"))
        groups.setdefault(key, []).append(r[metric])

    summary = []
    for key, values in groups.items():
        summary.append({
            **dict(key),
            "runs": len(values),
            f"median_{metric}": round(median(values), 6),
            f"min_{metric}": round(min(values), 6),
            f"max_{metric}": round(max(values), 6),
            "spread_pct": round(100 * (max(values) - min(values)) / median(values), 1),
        })

    # Speedup only makes sense when exactly one numeric parameter varies.
    varying = [k for k in (summary[0] if summary else {})
               if k not in {"runs"} and not k.startswith(("median_", "min_", "max_", "spread"))]
    axis = varying[0] if len(varying) == 1 and all(
        isinstance(s[varying[0]], (int, float)) for s in summary) else None
    if axis:
        summary.sort(key=lambda s: s[axis])
        base_p = summary[0][axis]
        base_t = summary[0][f"median_{metric}"]
        for s in summary:
            speedup = base_t / s[f"median_{metric}"]
            s["speedup"] = round(speedup, 2)
            s["efficiency"] = round(speedup / (s[axis] / base_p), 2)
    return summary, axis


def write(out_dir: Path, name: str, rows: list[dict], summary: list[dict],
          axis: str | None, metric: str = "elapsed_sec") -> list[str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []

    raw = out_dir / "raw.csv"
    with raw.open("w", newline="") as f:
        w = csv.writer(f)
        param_keys = sorted({k for r in rows for k in r["params"]})
        w.writerow(["task", *param_keys, "job_id", "node", "max_rss_mb", metric])
        for r in rows:
            w.writerow([r["task"], *[r["params"].get(k) for k in param_keys],
                        r["job_id"], r["node"], r["max_rss_mb"], r[metric]])
    written.append(str(raw))

    summ = out_dir / "summary.csv"
    with summ.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    written.append(str(summ))

    md = [f"# {name}", "", f"{len(rows)} completed runs. Medians over repeats.", ""]
    cols = list(summary[0])
    md.append("| " + " | ".join(cols) + " |")
    md.append("|" + "---|" * len(cols))
    for s in summary:
        md.append("| " + " | ".join(str(s[c]) for c in cols) + " |")
    nodes = sorted({r["node"] for r in rows if r["node"]})
    md += ["", f"Nodes used: {', '.join(nodes) if nodes else 'unknown'}"]
    (out_dir / "report.md").write_text("\n".join(md) + "\n")
    written.append(str(out_dir / "report.md"))

    if axis:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:
            written.append("(chart skipped: pip install matplotlib)")
            return written
        xs = [s[axis] for s in summary]
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 4))
        a1.plot(xs, [s["speedup"] for s in summary], "o-", label="measured")
        a1.plot(xs, [x / xs[0] for x in xs], "--", color="grey", label="ideal")
        for ax in (a1, a2):
            ax.set_xticks(xs)
        a1.set_xlabel(axis); a1.set_ylabel("speedup"); a1.legend(); a1.set_title("Speedup")
        a2.plot(xs, [s["efficiency"] for s in summary], "o-")
        a2.axhline(1.0, ls="--", color="grey")
        a2.set_ylim(0, 1.1); a2.set_xlabel(axis); a2.set_ylabel("efficiency")
        a2.set_title("Parallel efficiency")
        fig.suptitle(name)
        fig.tight_layout()
        fig.savefig(out_dir / "scaling.png", dpi=150)
        written.append(str(out_dir / "scaling.png"))
    return written
