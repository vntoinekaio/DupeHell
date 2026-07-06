"""Example: benchmark Splink v4 against DupeHell synthetic data with ground truth.

Usage:
    pip install "dupehell[demo]"
    python demo/splink_benchmark.py

Workflow:
    1. Estimate theoretical max F1 before generating data
    2. Generate a synthetic KYC dataset with known duplicates
    3. Run Splink v4 deduplication on it
    4. Compare Splink results against ground truth
    5. Compare real F1 vs theoretical F1_max
"""

import dupehell
import pandas as pd
import logging

logging.basicConfig(level=logging.WARNING)

try:
    import splink
    from splink import SettingsCreator, block_on, DuckDBAPI
    import splink.comparison_library as cl
    HAS_SPLINK = True
except ImportError:
    HAS_SPLINK = False

DOMAIN = "kyc"
SIZE = 10_000
SEED = 42
DIFFICULTY = "hell"

COL_FIRST = "given_name"
COL_LAST = "family_name"
COL_CITY = "residential_city"
COL_DOB = "birth_date"

if not HAS_SPLINK:
    print("=== Splink not installed ===")
    print("Install it with: pip install splink")
    print("Running theoretical bound estimation only...")
    print()

# ── 1: Theoretical bounds ──────────────────────────────────────────────────

report = dupehell.estimate_difficulty(
    domain=DOMAIN, size=SIZE, seed=SEED, difficulty=DIFFICULTY,
)

print("=== Theoretical bounds ===")
print(f"  F1 max        : {report.f1_max:.3f}")
print(f"  Precision max : {report.precision_max:.3f}")
print(f"  Recall max    : {report.recall_max:.3f}")
print(f"  True pairs    : {report.total_true_pairs}")
print(f"  Hard neg pairs: {report.total_hard_neg_pairs}")
print()

# ── 2: Generate ────────────────────────────────────────────────────────────
print("=== Generating dataset ===")
r = dupehell.generate(
    domain=DOMAIN, size=SIZE, seed=SEED, difficulty=DIFFICULTY,
    output_dir="./demo_output", output_format="parquet",
)
print(f"  Dataset : {r.dataset}")
print(f"  GT      : {r.ground_truth}")
print(f"  Records : {r.total_records}")
print(f"  Doublons: {r.exact_dups}")
print(f"  Hard negs: {r.hard_negs}")
print(f"  Masters : {r.masters}")
print()

if HAS_SPLINK:

    # ── 3: Load data ───────────────────────────────────────────────────────
    df = pd.read_parquet(r.dataset)
    gt = pd.read_parquet(r.ground_truth)

    df["unique_id"] = df.index.astype(str)
    gt_lookup = gt.set_index("record_id")["master_id"].to_dict()
    df["master_id"] = df["record_id"].map(gt_lookup)

    # ── 4: Splink v4 ───────────────────────────────────────────────────────
    db = DuckDBAPI()

    settings = SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
        comparisons=[
            cl.LevenshteinAtThresholds(COL_FIRST, [1, 2]),
            cl.LevenshteinAtThresholds(COL_LAST, [1, 2]),
            cl.ExactMatch(COL_CITY),
            cl.ExactMatch(COL_DOB),
        ],
        blocking_rules_to_generate_predictions=[
            block_on(COL_FIRST),
            block_on(COL_LAST),
            block_on(COL_DOB),
            block_on(COL_CITY),
            f"l.{COL_FIRST} = r.{COL_FIRST} AND l.{COL_CITY} = r.{COL_CITY}",
            f"l.{COL_LAST} = r.{COL_LAST} AND l.{COL_CITY} = r.{COL_CITY}",
            f"l.{COL_FIRST} = r.{COL_FIRST} AND l.{COL_LAST} = r.{COL_LAST}",
            f"levenshtein(l.{COL_FIRST}, r.{COL_FIRST}) <= 2 AND l.{COL_CITY} = r.{COL_CITY}",
            f"levenshtein(l.{COL_LAST}, r.{COL_LAST}) <= 2 AND l.{COL_DOB} = r.{COL_DOB}",
            f"levenshtein(l.{COL_FIRST}, r.{COL_FIRST}) <= 2 AND levenshtein(l.{COL_LAST}, r.{COL_LAST}) <= 2",
        ],
    )

    linker = splink.Linker(df, settings, db_api=db)

    linker.training.estimate_u_using_random_sampling(max_pairs=1e8)
    linker.training.estimate_probability_two_random_records_match(
        block_on(COL_FIRST), recall=0.7
    )
    for br_col in [COL_FIRST, COL_LAST, COL_CITY, COL_DOB]:
        linker.training.estimate_parameters_using_expectation_maximisation(
            block_on(br_col)
        )

    df_predict = linker.inference.predict(
        threshold_match_probability=0.01
    ).as_pandas_dataframe()

    print("=== Splink predictions ===")
    print(f"  Predicted pairs: {len(df_predict)}")
    print()

    # ── 5: Evaluate against ground truth ───────────────────────────────────
    def evaluate(df_predict, df):
        predicted_pairs = set()
        for _, row in df_predict.iterrows():
            predicted_pairs.add((row["unique_id_l"], row["unique_id_r"]))
        true_pairs = set()
        masters = df.groupby("master_id")["unique_id"].apply(list)
        for _, recs in masters.items():
            for i in range(len(recs)):
                for j in range(i + 1, len(recs)):
                    true_pairs.add((recs[i], recs[j]))
        tp = len(predicted_pairs & true_pairs)
        fp = len(predicted_pairs - true_pairs)
        fn = len(true_pairs - predicted_pairs)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        return precision, recall, f1, len(true_pairs)

    precision, recall, f1, true_pairs = evaluate(df_predict, df)

    print("=== Results ===")
    print(f"  True pairs          : {true_pairs}")
    print(f"  Precision          : {precision:.3f}")
    print(f"  Recall             : {recall:.3f}")
    print(f"  F1                 : {f1:.3f}")
    print(f"  Theoretical F1 max : {report.f1_max:.3f}")
    print(f"  Gap (F1_max - F1)  : {report.f1_max - f1:.3f}")
    print()

    if f1 >= report.f1_max * 0.9:
        print("OK  Splink atteint la limite theorique -- bottleneck = bruit.")
    else:
        print("!!  Splink sous-performe -- ameliorations d'algo possibles.")