"""Genere un dataset pour chaque (domaine x difficulte) et verifie l'absence
de regression sur trois axes :

1. Aucune generation ne crashe (40 domaines x {light, medium, hell}).
2. La distribution par entite reste dans les clous : aucune entite
   "identite" (celle dont le nom termine le schema, cf. schemas/*.json)
   ne depasse un seuil configurable de la population resolue (defaut 55%,
   marge au-dessus du 50% cible pour ne pas flagger du bruit de mesure).
3. La monotonicite light >= medium >= hell tient sur f1_max
   (`estimate_difficulty`), tolerance 1e-6 pour le bruit flottant.

Usage :
    python scripts/validate_all_domains.py [--size 20000] [--seed 42]
                                            [--threshold 0.55]

Sort avec un code non nul si une generation a echoue ou si une violation
de monotonicite est detectee (utilisable en CI / pre-release check).
"""

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

import polars as pl
from dupehell import estimate_difficulty, generate, list_domains

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = REPO_ROOT / "schemas"
POOLS_DIR = REPO_ROOT / "assets" / "pools"
DIFFICULTIES = ["light", "medium", "hell"]


def run_one(domain: str, difficulty: str, size: int, seed: int, out_dir: Path):
    result = generate(
        domain=domain,
        size=size,
        seed=seed,
        difficulty=difficulty,
        output_format="ipc",
        output_dir=str(out_dir),
        schemas_dir=str(SCHEMAS_DIR),
        pools_dir=str(POOLS_DIR),
    )
    ds = pl.read_ipc(result.dataset)
    gt = pl.read_ipc(result.ground_truth)

    total_rows = ds.height
    entity_counts = ds["entity_type"].value_counts()
    entity_share = {
        row["entity_type"]: row["count"] / total_rows
        for row in entity_counts.to_dicts()
    }

    # Population resolue par entite = nb de master_id distincts pour ce
    # entity_type (equivalent au "entity_count apres filtrage" du sweep Linkars).
    resolved_counts = (
        ds.group_by("entity_type")
        .agg(pl.col("master_id").n_unique().alias("n_resolved"))
        .to_dicts()
    )
    total_resolved = sum(r["n_resolved"] for r in resolved_counts)
    resolved_share = {
        r["entity_type"]: r["n_resolved"] / total_resolved
        for r in resolved_counts
    }

    match_counts = gt["match_type"].value_counts().to_dicts()
    match_dist = {r["match_type"]: r["count"] for r in match_counts}

    est = estimate_difficulty(
        domain=domain,
        size=size,
        seed=seed,
        difficulty=difficulty,
        schemas_dir=str(SCHEMAS_DIR),
    )

    return {
        "domain": domain,
        "difficulty": difficulty,
        "total_rows": total_rows,
        "entity_share": entity_share,
        "resolved_share": resolved_share,
        "match_dist": match_dist,
        "f1_max": est.f1_max,
        "precision_max": est.precision_max,
        "recall_max": est.recall_max,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--threshold", type=float, default=0.55)
    parser.add_argument("--keep-output", action="store_true")
    args = parser.parse_args()

    domains = sorted(list_domains(schemas_dir=str(SCHEMAS_DIR)))
    out_dir = Path(tempfile.mkdtemp(prefix="dupehell_validate_"))
    print(f"[info] {len(domains)} domaines, sortie temporaire: {out_dir}")

    results = []
    failures = []
    for i, domain in enumerate(domains):
        for difficulty in DIFFICULTIES:
            try:
                r = run_one(domain, difficulty, args.size, args.seed, out_dir)
                results.append(r)
                print(
                    f"[{i + 1}/{len(domains)}] {domain:20s} {difficulty:7s} "
                    f"f1_max={r['f1_max']:.4f}",
                    flush=True,
                )
            except Exception as e:  # noqa: BLE001 - report every failure, don't abort the sweep
                failures.append({"domain": domain, "difficulty": difficulty, "error": str(e)})
                print(
                    f"[{i + 1}/{len(domains)}] {domain:20s} {difficulty:7s} ECHEC -- {e}",
                    flush=True,
                )

    if not args.keep_output:
        shutil.rmtree(out_dir, ignore_errors=True)

    # --- Check 1: distribution par entite -----------------------------------
    dist_violations = []
    for r in results:
        for entity, share in r["resolved_share"].items():
            if share > args.threshold:
                dist_violations.append(
                    {
                        "domain": r["domain"],
                        "difficulty": r["difficulty"],
                        "entity": entity,
                        "share": round(share, 4),
                    }
                )

    # --- Check 2: monotonicite light >= medium >= hell ----------------------
    by_domain = {}
    for r in results:
        by_domain.setdefault(r["domain"], {})[r["difficulty"]] = r["f1_max"]

    mono_violations = []
    eps = 1e-6
    for domain, f1s in by_domain.items():
        if not all(d in f1s for d in DIFFICULTIES):
            continue  # domaine avec un echec de generation, deja liste dans failures
        light, medium, hell = f1s["light"], f1s["medium"], f1s["hell"]
        if medium > light + eps:
            mono_violations.append((domain, "medium > light", medium, light))
        if hell > medium + eps:
            mono_violations.append((domain, "hell > medium", hell, medium))

    # --- Rapport --------------------------------------------------------------
    print("\n" + "=" * 70)
    print(f"Generations : {len(results)}/{len(domains) * len(DIFFICULTIES)} OK, {len(failures)} echec(s)")
    if failures:
        for f in failures:
            print(f"  ECHEC {f['domain']}/{f['difficulty']}: {f['error']}")

    print(f"\nDistribution (seuil {args.threshold:.0%}) : {len(dist_violations)} violation(s)")
    for v in dist_violations:
        print(f"  {v['domain']:20s} {v['difficulty']:7s} {v['entity']:25s} {v['share']:.1%}")

    print(f"\nMonotonicite f1_max : {len(mono_violations)} violation(s)")
    for domain, kind, hi, lo in mono_violations:
        print(f"  {domain:20s} {kind}: {hi:.4f} > {lo:.4f}")

    report_path = REPO_ROOT / "scripts" / "validate_all_domains_report.json"
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "size": args.size,
                "seed": args.seed,
                "threshold": args.threshold,
                "results": results,
                "failures": failures,
                "dist_violations": dist_violations,
                "mono_violations": [
                    {"domain": d, "kind": k, "hi": hi, "lo": lo} for d, k, hi, lo in mono_violations
                ],
            },
            fh,
            indent=2,
        )
    print(f"\n[info] rapport ecrit dans {report_path}")

    if failures or mono_violations:
        sys.exit(1)


if __name__ == "__main__":
    main()
