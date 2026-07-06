"""Pipeline correctness tests for dupehell."""
import sys, subprocess, os, tempfile, hashlib, glob, shutil
import polars as pl
from dupehell import generate, estimate_difficulty

BASE = tempfile.mkdtemp(prefix="dh_test_")
print(f"Working in {BASE}")

REPO = r"C:\Users\Admin\Desktop\dupehell"
SCHEMAS = os.path.join(REPO, "schemas")
POOLS = os.path.join(REPO, "assets", "pools")
EXE = os.path.join(REPO, "target", "release", "dupehell.exe")

KW = dict(schemas_dir=SCHEMAS, pools_dir=POOLS)

results = []

def report(test, status, detail):
    results.append((test, status, detail))
    print(f"  {test}: {status}  {detail}")

# ── Test 1: Row count matches config ──────────────────────────────────
print("\n=== Test 1: Row count matches config ===")
d1 = tempfile.mkdtemp(prefix="t1_", dir=BASE)
try:
    r = generate("kyc", 1000, seed=42, difficulty="medium", output_dir=d1, **KW)
    df = pl.read_ipc(r.dataset)
    actual, expected = df.shape[0], r.total_records
    if actual == expected and 1000 < actual < 1200:
        report("Test 1", "PASS", f"rows={actual}, total_records={expected}")
    else:
        report("Test 1", "FAIL", f"rows={actual}, total_records={expected} (expected ~1000-1200)")
except Exception as e:
    report("Test 1", "FAIL", str(e))

# ── Test 2: Master ID consistency ─────────────────────────────────────
print("\n=== Test 2: Master ID consistency ===")
try:
    violations = (
        df.group_by("master_id").agg(pl.n_unique("entity_type").alias("n_entity_types"))
        .filter(pl.col("n_entity_types") > 1)
    )
    n = violations.shape[0]
    report("Test 2", "PASS" if n == 0 else "FAIL",
           "0 master_id spans multiple entity_types" if n == 0 else f"{n} master_ids have >1 entity_type")
except Exception as e:
    report("Test 2", "FAIL", str(e))

# ── Test 3: Record ID uniqueness ──────────────────────────────────────
print("\n=== Test 3: Record ID uniqueness ===")
try:
    n_dup = df.filter(pl.col("record_id").is_duplicated()).shape[0]
    report("Test 3", "PASS" if n_dup == 0 else "FAIL",
           "No duplicate record_id values" if n_dup == 0 else f"{n_dup} duplicate record_id values")
except Exception as e:
    report("Test 3", "FAIL", str(e))

# ── Test 4: Ground truth columns present ──────────────────────────────
print("\n=== Test 4: Ground truth columns ===")
try:
    gt = pl.read_ipc(r.ground_truth)
    required = {"record_id", "master_id", "entity_type", "match_type"}
    missing = required - set(gt.columns)
    if missing:
        report("Test 4", "FAIL", f"Missing columns: {missing}")
    else:
        valid = {"exact_dup", "hard_neg", "unique"}
        bad = gt.filter(~pl.col("match_type").is_in(valid)).select("match_type").unique()
        if bad.shape[0] > 0:
            report("Test 4", "FAIL", f"Invalid match_type values")
        else:
            report("Test 4", "PASS", "All columns present, match_types valid")
except Exception as e:
    report("Test 4", "FAIL", str(e))

# ── Test 5: Singleton master fraction ─────────────────────────────────
print("\n=== Test 5: Singleton master fraction ===")
d5 = tempfile.mkdtemp(prefix="t5_", dir=BASE)
try:
    out = subprocess.run(
        [EXE, "--domain", "kyc", "--size", "1000", "--seed", "1",
         "--singleton-master-fraction", "0.5", "--hard-neg-ratio", "0.0",
         "--output-dir", d5, "--schemas-dir", SCHEMAS, "--pools-dir", POOLS],
        capture_output=True, text=True, timeout=60
    )
    if out.returncode != 0:
        raise RuntimeError(f"CLI failed: {out.stderr[:300]}")
    data_files = [f for f in glob.glob(os.path.join(d5, "*.ipc")) if "_ground_truth" not in f]
    if not data_files:
        raise RuntimeError("No output IPC file found")
    df5 = pl.read_ipc(data_files[0])
    mc = df5.group_by("master_id").agg(pl.len().alias("cnt"))
    total_m = mc.shape[0]
    singletons = mc.filter(pl.col("cnt") == 1).shape[0]
    frac = singletons / total_m if total_m > 0 else 0
    report("Test 5", "PASS" if 0.35 <= frac <= 0.65 else "FAIL",
           f"singleton_master_fraction={frac:.3f} ({singletons}/{total_m}, target ~0.5)")
except Exception as e:
    report("Test 5", "FAIL", str(e))

# ── Test 6: Cross-entity master isolation ─────────────────────────────
print("\n=== Test 6: Cross-entity master isolation ===")
d6 = tempfile.mkdtemp(prefix="t6_", dir=BASE)
try:
    r6 = generate("publishing", 2000, seed=7, difficulty="medium", output_dir=d6, **KW)
    df6 = pl.read_ipc(r6.dataset)
    conflicts = (
        df6.group_by("master_id").agg(pl.n_unique("entity_type").alias("n"))
        .filter(pl.col("n") > 1)
    )
    n_con = conflicts.shape[0]
    report("Test 6", "PASS" if n_con == 0 else "FAIL",
           f"0 cross-entity conflicts out of {df6['master_id'].n_unique()} masters" if n_con == 0
           else f"{n_con} masters span multiple entity_types")
except Exception as e:
    report("Test 6", "FAIL", str(e))

# ── Test 7: Seed determinism across APIs ──────────────────────────────
print("\n=== Test 7: Seed determinism across APIs ===")
d7_py = tempfile.mkdtemp(prefix="t7_py_", dir=BASE)
d7_cli = tempfile.mkdtemp(prefix="t7_cli_", dir=BASE)
try:
    r7_py = generate("kyc", 100, seed=42, difficulty="medium", output_dir=d7_py, **KW)
    df7_py = pl.read_ipc(r7_py.dataset)
    py_hash = hashlib.sha256(df7_py.to_pandas().to_csv(index=False).encode()).hexdigest()
    out7 = subprocess.run(
        [EXE, "--domain", "kyc", "--size", "100", "--seed", "42",
         "--difficulty", "medium", "--output-dir", d7_cli,
         "--schemas-dir", SCHEMAS, "--pools-dir", POOLS],
        capture_output=True, text=True, timeout=60
    )
    if out7.returncode != 0:
        raise RuntimeError(f"CLI failed: {out7.stderr[:200]}")
    cli_data = [f for f in glob.glob(os.path.join(d7_cli, "*.ipc")) if "_ground_truth" not in f]
    if not cli_data:
        raise RuntimeError("No CLI output file")
    df7_cli = pl.read_ipc(cli_data[0])
    cli_hash = hashlib.sha256(df7_cli.to_pandas().to_csv(index=False).encode()).hexdigest()
    report("Test 7", "PASS" if py_hash == cli_hash else "FAIL",
           "Python and CLI produce identical data columns" if py_hash == cli_hash else "Data hashes differ")
except Exception as e:
    report("Test 7", "FAIL", str(e))

# ── Test 8: Estimate F1 sanity ────────────────────────────────────────
print("\n=== Test 8: Estimate F1 sanity ===")
try:
    r8_hell = estimate_difficulty("kyc", 10000, difficulty="hell", schemas_dir=SCHEMAS)
    r8_light = estimate_difficulty("kyc", 10000, difficulty="light", schemas_dir=SCHEMAS)
    f1_hell = r8_hell.f1_max
    f1_light = r8_light.f1_max
    hell_ok = 0.75 <= f1_hell <= 0.95
    light_ok = f1_light > f1_hell
    if hell_ok and light_ok:
        report("Test 8", "PASS", f"hell f1_max={f1_hell:.4f}, light f1_max={f1_light:.4f}")
    else:
        report("Test 8", "FAIL", f"hell f1_max={f1_hell:.4f}, light f1_max={f1_light:.4f} (hell in [0.75,0.95]={hell_ok}, light>hell={light_ok})")
except Exception as e:
    report("Test 8", "FAIL", str(e))

# ── Final summary ─────────────────────────────────────────────────────
print("\n\n## Final Results")
print("| Test | Status | Detail |")
print("|------|--------|--------|")
for test, status, detail in results:
    print(f"| {test} | {status} | {detail} |")

passed = sum(1 for _, s, _ in results if s == "PASS")
total = len(results)
print(f"\n**{passed}/{total} tests passed**")
