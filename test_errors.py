"""Exhaustive error path tests for dupehell2 CLI and Python API."""

import subprocess
import sys
import os
import json
import tempfile
from pathlib import Path

BINARY = r"C:\Users\Admin\Desktop\dupehell2\target\release\dupehell.exe"
FRENCH_WORDS = [
    "Le chemin", "introuvable", "système", "fichier",
    "erreur", "accès", "refusé", "non trouvé", "impossible",
    # extra coverage
    "chemin d'accès", "fichier spécifié",
]


def binary_path():
    if not os.path.exists(BINARY):
        raise RuntimeError(f"Binary not found: {BINARY}")
    return BINARY


def has_french(text: str) -> bool:
    lower = text.lower()
    for word in FRENCH_WORDS:
        if word.lower() in lower:
            return True
    return False


def run_cli(*args) -> tuple[int, str]:
    result = subprocess.run(
        [binary_path(), *args],
        capture_output=True, text=True, timeout=300,
    )
    stderr = result.stderr or ""
    stdout = result.stdout or ""
    combined = stderr + stdout
    return result.returncode, combined.strip()


def check_english(text: str, label: str) -> bool:
    if has_french(text):
        print(f"  FAIL [{label}]: French detected in: {text[:200]}")
        return False
    return True


# ── CLI Tests ────────────────────────────────────────────────────────────────

def test_1a_domain_nonexistent():
    """--domain nonexistent -> error with available domains"""
    rc, out = run_cli("--domain", "nonexistent")
    ok = True
    rc_ok = rc != 0
    if not rc_ok:
        print(f"  FAIL: expected non-zero exit, got {rc}")
        ok = False
    if "Available domains" not in out and "available" not in out.lower():
        print(f"  FAIL: no domain listing in: {out[:200]}")
        ok = False
    ok = check_english(out, "1a") and ok
    return ok, rc, out[:100]


def test_1b_size_too_small():
    """--size 5 -> error: size must be >= 10"""
    rc, out = run_cli("--size", "5")
    ok = True
    rc_ok = rc != 0
    if not rc_ok:
        print(f"  FAIL: expected non-zero exit, got {rc}")
        ok = False
    if "size must be >= 10" not in out:
        print(f"  FAIL: wrong error msg: {out[:200]}")
        ok = False
    ok = check_english(out, "1b") and ok
    return ok, rc, out[:100]


def test_1c_difficulty_unknown():
    """--difficulty unknown -> succeeds (Rust silently defaults to medium) or error"""
    rc, out = run_cli("--difficulty", "unknown", "--size", "100")
    ok = True
    # Rust code silently defaults to medium for unknown difficulty (schema.rs:73)
    # So this is expected to succeed
    if rc != 0:
        print(f"  NOTE: returned non-zero exit {rc}, checking error...")
        ok = check_english(out, "1c")
    else:
        print("  NOTE: unknown difficulty defaults to 'medium' silently (schema.rs:73)")
    return ok, rc, out[:100]


def test_1d_output_format_csv():
    """--output-format csv -> error: must be ipc or parquet"""
    rc, out = run_cli("--output-format", "csv", "--size", "100")
    ok = True
    rc_ok = rc != 0
    if not rc_ok:
        print(f"  FAIL: expected non-zero exit, got {rc}")
        ok = False
    if "must be 'ipc' or 'parquet'" not in out:
        print(f"  FAIL: wrong error msg: {out[:200]}")
        ok = False
    ok = check_english(out, "1d") and ok
    return ok, rc, out[:100]


def test_1e_hard_neg_ratio_abc():
    """--hard-neg-ratio abc -> clap parse error"""
    rc, out = run_cli("--hard-neg-ratio", "abc")
    ok = True
    rc_ok = rc != 0
    if not rc_ok:
        print(f"  FAIL: expected non-zero exit, got {rc}")
        ok = False
    if "invalid float literal" not in out.lower() and "invalid value" not in out.lower():
        print(f"  FAIL: not a clap parse error: {out[:200]}")
        ok = False
    ok = check_english(out, "1e") and ok
    return ok, rc, out[:100]


def test_1f_pools_dir_nonexistent():
    """--pools-dir ./nonexistent -> error: pools dir not found"""
    rc, out = run_cli("--pools-dir", "./nonexistent_xyz", "--size", "100")
    ok = True
    rc_ok = rc != 0
    if not rc_ok:
        print(f"  FAIL: expected non-zero exit, got {rc}")
        ok = False
    if "pools dir not found" not in out.lower():
        print(f"  FAIL: wrong error msg: {out[:200]}")
        ok = False
    ok = check_english(out, "1f") and ok
    return ok, rc, out[:100]


def test_1g_schemas_dir_nonexistent():
    """--schemas-dir ./nonexistent -> error with directory not found"""
    rc, out = run_cli("--schemas-dir", "./nonexistent_xyz", "--size", "100")
    ok = True
    rc_ok = rc != 0
    if not rc_ok:
        print(f"  FAIL: expected non-zero exit, got {rc}")
        ok = False
    if "directory not found" not in out.lower():
        print(f"  FAIL: no 'directory not found' hint: {out[:200]}")
        ok = False
    ok = check_english(out, "1g") and ok
    return ok, rc, out[:100]


def test_1h_domain_kyc_estimate():
    """--domain KYC --estimate -> works (case-insensitive FS)"""
    rc, out = run_cli("--domain", "KYC", "--estimate")
    ok = True
    rc_ok = rc == 0
    if not rc_ok:
        print(f"  FAIL: expected exit 0 (case-insensitive), got {rc}")
        ok = False
    ok = check_english(out, "1h") and ok
    return ok, rc, out[:100]


def test_1i_no_args():
    """no args -> uses defaults, succeeds"""
    rc, out = run_cli("--size", "100")
    ok = True
    if rc != 0:
        print(f"  FAIL: expected exit 0, got {rc}")
        ok = False
    if "Done in" not in out:
        print(f"  FAIL: no success output: {out[:200]}")
        ok = False
    ok = check_english(out, "1i") and ok
    return ok, rc, out[:100]


def test_1j_help():
    """--help -> shows help, exit 0"""
    rc, out = run_cli("--help")
    ok = True
    if rc != 0:
        print(f"  FAIL: expected exit 0, got {rc}")
        ok = False
    if "Usage:" not in out and "dupehell" not in out:
        print(f"  FAIL: no help text: {out[:200]}")
        ok = False
    ok = check_english(out, "1j") and ok
    return ok, rc, out[:100]


def test_1j_version():
    """--version -> shows version, exit 0"""
    rc, out = run_cli("--version")
    ok = True
    if rc != 0:
        print(f"  FAIL: expected exit 0, got {rc}")
        ok = False
    if "0.4" not in out:
        print(f"  FAIL: no version info: {out[:200]}")
        ok = False
    ok = check_english(out, "1j-version") and ok
    return ok, rc, out[:100]


# ── Python API Tests ─────────────────────────────────────────────────────────

def test_2a_generate_nonexistent():
    """generate('nonexistent', 100) -> FileNotFoundError, English, lists domains"""
    from dupehell import generate
    ok = True
    try:
        generate('nonexistent', 100)
        print("  FAIL: no exception raised")
        ok = False
    except FileNotFoundError as e:
        msg = str(e)
        if "Available domains" not in msg:
            print(f"  FAIL: no domain listing: {msg[:200]}")
            ok = False
        rc = 1
    except Exception as e:
        msg = str(e)
        print(f"  FAIL: wrong exception: {type(e).__name__}: {e}")
        ok = False
        rc = 1
    else:
        rc = 0
    ok = check_english(str(locals().get('msg', '')), "2a") and ok
    return ok, rc, str(locals().get('msg', ''))[:100]


def test_2b_generate_size_5():
    """generate('kyc', 5) -> ValueError: size must be >= 10"""
    from dupehell import generate
    ok = True
    try:
        generate('kyc', 5)
        print("  FAIL: no exception raised")
        ok = False
    except ValueError as e:
        msg = str(e)
        if "size must be >= 10" not in msg:
            print(f"  FAIL: wrong error: {msg[:200]}")
            ok = False
        rc = 1
    except Exception as e:
        print(f"  FAIL: wrong exception: {type(e).__name__}: {e}")
        ok = False
        rc = 1
    else:
        rc = 0
    ok = check_english(str(locals().get('msg', '')), "2b") and ok
    return ok, rc, str(locals().get('msg', ''))[:100]


def test_2c_generate_difficulty_unknown():
    """generate('kyc', 100, difficulty='unknown') -> succeeds (silent default)"""
    from dupehell import generate
    ok = True
    try:
        result = generate('kyc', 100, difficulty='unknown')
        print("  NOTE: unknown difficulty silently defaults to 'medium'")
        rc = 0
    except Exception as e:
        msg = str(e)
        ok = check_english(msg, "2c")
        rc = 1
    return ok, rc, str(locals().get('msg', ''))[:100]


def test_2d_generate_bad_format():
    """generate('kyc', 100, output_format='csv') -> ValueError"""
    from dupehell import generate
    ok = True
    try:
        generate('kyc', 100, output_format='csv')
        print("  FAIL: no exception raised")
        ok = False
    except ValueError as e:
        msg = str(e)
        if "must be 'ipc' or 'parquet'" not in msg:
            print(f"  FAIL: wrong error: {msg[:200]}")
            ok = False
        rc = 1
    except Exception as e:
        print(f"  FAIL: wrong exception: {type(e).__name__}: {e}")
        ok = False
        rc = 1
    else:
        rc = 0
    ok = check_english(str(locals().get('msg', '')), "2d") and ok
    return ok, rc, str(locals().get('msg', ''))[:100]


def test_2e_estimate_difficulty_nonexistent():
    """estimate_difficulty('nonexistent') -> ValueError (Rust wraps as PyValueError)"""
    from dupehell import estimate_difficulty
    ok = True
    try:
        estimate_difficulty('nonexistent')
        print("  FAIL: no exception raised")
        ok = False
    except ValueError as e:
        msg = str(e)
        if "schema file not found" not in msg.lower():
            print(f"  FAIL: wrong error: {msg[:200]}")
            ok = False
        rc = 1
    except Exception as e:
        print(f"  FAIL: wrong exception: {type(e).__name__}: {e}")
        ok = False
        rc = 1
    else:
        rc = 0
    ok = check_english(str(locals().get('msg', '')), "2e") and ok
    return ok, rc, str(locals().get('msg', ''))[:100]


def test_2f_load_and_validate_nonexistent():
    """load_and_validate('/nonexistent/path.json') -> FileNotFoundError"""
    from dupehell import load_and_validate
    ok = True
    try:
        load_and_validate('/nonexistent/path.json')
        print("  FAIL: no exception raised")
        ok = False
    except FileNotFoundError as e:
        msg = str(e)
        rc = 1
    except Exception as e:
        msg = str(e)
        print(f"  FAIL: wrong exception: {type(e).__name__}: {e}")
        ok = False
        rc = 1
    else:
        rc = 0
    ok = check_english(str(locals().get('msg', '')), "2f") and ok
    return ok, rc, str(locals().get('msg', ''))[:100]


def test_2g_load_and_validate_bad_json():
    """load_and_validate(bad_json) -> JSONDecodeError (or ValidationError), English"""
    from dupehell import load_and_validate
    ok = True
    with tempfile.NamedTemporaryFile(suffix='.json', mode='w', delete=False) as f:
        f.write('{bad json}')
        bad_path = f.name
    try:
        load_and_validate(bad_path)
        print("  FAIL: no exception raised")
        ok = False
    except Exception as e:
        msg = str(e)
        if isinstance(e, json.JSONDecodeError):
            rc = 1
        else:
            print(f"  NOTE: got {type(e).__name__} instead of JSONDecodeError: {msg[:100]}")
            rc = 1
    else:
        rc = 0
    finally:
        os.unlink(bad_path)
    ok = check_english(str(locals().get('msg', '')), "2g") and ok
    return ok, rc, str(locals().get('msg', ''))[:100]


# ── Main runner ──────────────────────────────────────────────────────────────

CLI_TESTS = [
    ("1a", "--domain nonexistent", test_1a_domain_nonexistent),
    ("1b", "--size 5", test_1b_size_too_small),
    ("1c", "--difficulty unknown", test_1c_difficulty_unknown),
    ("1d", "--output-format csv", test_1d_output_format_csv),
    ("1e", "--hard-neg-ratio abc", test_1e_hard_neg_ratio_abc),
    ("1f", "--pools-dir ./nonexistent", test_1f_pools_dir_nonexistent),
    ("1g", "--schemas-dir ./nonexistent", test_1g_schemas_dir_nonexistent),
    ("1h", "--domain KYC --estimate", test_1h_domain_kyc_estimate),
    ("1i", "no args (defaults)", test_1i_no_args),
    ("1j", "--help", test_1j_help),
    ("1j-v", "--version", test_1j_version),
]

PY_TESTS = [
    ("2a", "generate('nonexistent', 100)", test_2a_generate_nonexistent),
    ("2b", "generate('kyc', 5)", test_2b_generate_size_5),
    ("2c", "generate('kyc', 100, difficulty='unknown')", test_2c_generate_difficulty_unknown),
    ("2d", "generate('kyc', 100, output_format='csv')", test_2d_generate_bad_format),
    ("2e", "estimate_difficulty('nonexistent')", test_2e_estimate_difficulty_nonexistent),
    ("2f", "load_and_validate('/nonexistent/path.json')", test_2f_load_and_validate_nonexistent),
    ("2g", "load_and_validate(bad_json)", test_2g_load_and_validate_bad_json),
]


def fmt_preview(out: str, maxlen=100) -> str:
    return out.replace("\n", "\\n ")[:maxlen]


def main():
    results = []

    print("=" * 78)
    print("  CLI Error Path Tests")
    print("=" * 78)

    for tid, desc, fn in CLI_TESTS:
        print(f"\n  [{tid}] {desc}")
        try:
            ok, rc, preview = fn()
        except Exception as e:
            print(f"  FAIL: test crashed: {e}")
            ok, rc = False, -1
            preview = str(e)[:100]
        status = "PASS" if ok else "FAIL"
        has_fr = "YES" if has_french(preview) else "no"
        results.append((tid, status, rc, has_fr, preview))

    print("\n" + "=" * 78)
    print("  Python API Error Path Tests")
    print("=" * 78)

    for tid, desc, fn in PY_TESTS:
        print(f"\n  [{tid}] {desc}")
        try:
            ok, rc, preview = fn()
        except Exception as e:
            print(f"  FAIL: test crashed: {e}")
            ok, rc = False, -1
            preview = str(e)[:100]
        status = "PASS" if ok else "FAIL"
        has_fr = "YES" if has_french(preview) else "no"
        results.append((tid, status, rc, has_fr, preview))

    # Summary table
    print("\n\n" + "=" * 78)
    print("  RESULTS SUMMARY")
    print("=" * 78)
    print()
    hdr = f"{'Test':<8} {'Status':<8} {'Exit code':<10} {'French?':<9} Message preview (first 100 chars)"
    print(hdr)
    print("-" * len(hdr))
    failures = 0
    for tid, status, rc, has_fr, preview in results:
        print(f"{tid:<8} {status:<8} {rc:<10} {has_fr:<9} {preview}")
        if status == "FAIL" or has_fr == "YES":
            failures += 1

    print()
    print(f"Total tests: {len(results)}, Passed: {len(results) - failures}, Failed: {failures}")
    if failures:
        print("\nFAILURES DETECTED. Check rows with FAIL or YES in French column.")
    else:
        print("\nAll tests passed — no French, no panics, all errors graceful.")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
