#!/usr/bin/env python3
"""Benchmark dupehell across all 41 domains — one-shot per domain, mesure RAM + temps."""

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import psutil

from dupehell import DOMAINS, estimate_difficulty, generate


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark all 41 domains")
    p.add_argument(
        "-n",
        type=int,
        default=20_000_000,
        help="Number of base records per domain (default: 20_000_000)",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Where to write the benchmark markdown report (default: cwd)",
    )
    return p.parse_args()


def fmt_mb(b: int) -> str:
    return f"{b / 1_048_576:.1f}"


def main() -> None:
    args = parse_args()
    size = args.n
    size_label = f"{size//10_000_000}M" if size >= 10_000_000 else f"{size//1000}k"
    report_path = Path(args.output_dir or ".") / f"benchmark-measure-{size_label}.md"

    domains = sorted(DOMAINS)

    # Precompute estimates
    print(f"Estimating difficulty (hell) for {len(domains)} domains @ {size_label}...")
    estimates: dict[str, dict] = {}
    for d in domains:
        try:
            r = estimate_difficulty(d, size, difficulty="hell")
            estimates[d] = {
                "f1_max": round(r.f1_max, 4),
                "precision_max": round(r.precision_max, 4),
                "recall_max": round(r.recall_max, 4),
            }
        except Exception as e:
            print(f"  [SKIP] estimate failed for {d}: {e}")
            estimates[d] = None

    # Run generation
    rows: list[dict] = []
    proc = psutil.Process()
    for idx, d in enumerate(domains, 1):
        label = f"[{idx}/{len(domains)}] {d}"
        est = estimates.get(d)
        if est is None:
            print(f"{label} — SKIP (estimate failed)")
            rows.append({"domain": d, "status": "skip"})
            continue

        out_dir = tempfile.mkdtemp(prefix=f"bench_{d}_")
        try:
            gc_before = proc.memory_info().rss
            t0 = time.perf_counter()
            r = generate(
                d,
                size,
                seed=42,
                difficulty="hell",
                output_format="parquet",
                output_dir=out_dir,
            )
            t1 = time.perf_counter()
            gc_after = proc.memory_info().rss

            wall = round(t1 - t0, 3)
            recs = r.total_records
            thru = round(recs / wall) if wall > 0 else 0
            peak_mb = fmt_mb(gc_after)
            delta_mb = fmt_mb(gc_after - gc_before)

            # Taille fichier dataset
            ds_path = Path(r.dataset)
            ds_size = ds_path.stat().st_size if ds_path.exists() else 0

            rows.append(
                {
                    "domain": d,
                    "status": "ok",
                    "wall_s": wall,
                    "total_records": recs,
                    "recs_per_sec": thru,
                    "peak_rss_mb": peak_mb,
                    "delta_rss_mb": delta_mb,
                    "ds_size_mb": round(ds_size / 1_048_576, 2),
                    "f1_max": est["f1_max"],
                    "precision_max": est["precision_max"],
                    "recall_max": est["recall_max"],
                }
            )
            print(
                f"{label} — {wall}s, {thru} rec/s, "
                f"peak={peak_mb}MB delta={delta_mb}MB, "
                f"f1={est['f1_max']}"
            )
        except Exception as e:
            print(f"{label} — FAIL: {e}")
            rows.append({"domain": d, "status": f"fail: {e}"})
        finally:
            shutil.rmtree(out_dir, ignore_errors=True)

    # Write report
    ok_rows = [r for r in rows if r["status"] == "ok"]
    fail_rows = [r for r in rows if r["status"] != "ok"]
    n_ok = len(ok_rows)
    n_fail = len(fail_rows)

    lines = [
        f"# Benchmark — dupehell @ {size_label} hell (parquet)",
        "",
        f"- **Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- **Size**: {size:,} records per domain",
        f"- **Difficulty**: hell",
        f"- **Format**: parquet",
        f"- **Domains**: {n_ok} OK / {n_fail} fail sur {len(domains)}",
        "",
    ]

    if ok_rows:
        walls = [r["wall_s"] for r in ok_rows]
        thrus = [r["recs_per_sec"] for r in ok_rows]
        peaks = [float(r["peak_rss_mb"]) for r in ok_rows]
        f1s = [r["f1_max"] for r in ok_rows]

        lines += [
            "## Résumé",
            "",
            f"| Métrique | Valeur |",
            f"|----------|--------|",
            f"| Wall total (somme) | {sum(walls):.1f}s |",
            f"| Wall moyen | {sum(walls)/len(walls):.3f}s |",
            f"| Wall min / max | {min(walls):.3f}s / {max(walls):.3f}s |",
            f"| Throughput moyen | {sum(thrus)//len(thrus):,} rec/s |",
            f"| Throughput min / max | {min(thrus):,} / {max(thrus):,} rec/s |",
            f"| F1 max moyen | {sum(f1s)/len(f1s):.4f} |",
            f"| F1 max min / max | {min(f1s):.4f} / {max(f1s):.4f} |",
            f"| Peak RSS moyen | {sum(peaks)/len(peaks):.1f} MB |",
            f"| Peak RSS max | {max(peaks):.1f} MB |",
            "",
        ]

        lines += [
            "## Détail par domaine",
            "",
            "| Domaine | Wall (s) | Rec/s | Records | Dataset (MB) | Peak RSS (MB) | ΔRSS (MB) | F1 max | P max | R max |",
            "|---------|----------|-------|---------|-------------|---------------|-----------|--------|-------|-------|",
        ]
        for r in ok_rows:
            lines.append(
                f"| {r['domain']} | {r['wall_s']} | {r['recs_per_sec']:,} "
                f"| {r['total_records']:,} | {r['ds_size_mb']} | {r['peak_rss_mb']} "
                f"| {r['delta_rss_mb']} | {r['f1_max']} | {r['precision_max']} | {r['recall_max']} |"
            )
        lines.append("")

    if fail_rows:
        lines += ["## Échecs", "", "| Domaine | Raison |", "|---------|--------|"]
        for r in fail_rows:
            lines.append(f"| {r['domain']} | {r['status']} |")
        lines.append("")

    report = "\n".join(lines) + "\n"
    report_path.write_text(report, encoding="utf-8")
    print(f"\nReport written to {report_path}")


if __name__ == "__main__":
    main()
