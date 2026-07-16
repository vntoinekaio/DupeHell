"""Tests for DupeHell property-graph generation (--graph / generate_graph).

Covers the 13 behaviours from integration-graph.md §12:
  1. node count == total_records
  2. node schema (dataset cols + node_id replacing record_id)
  3. FK edge integrity (target_node_id exists in nodes)
  4. dup edge integrity (exact_dup endpoints share a master_id)
  5. dup edge count (k(k-1)/2, or k-1 spanning tree)
  6. hard_neg edge integrity (source match_type == hard_neg)
  7. three edge types present (fk, exact_dup, hard_neg)
  8. determinism (tabular data identical with/without --graph)
  9. IPC readable by pyarrow
 10. graph disabled -> nodes/edges None
 11. spanning-tree contract (per-cluster edge count is consistent)
 12. FK pool correct (FK edges resolve to real node_ids -> 2-col pool id+record_id)
 13. GT backward-compat (GT file still well-formed after cluster_map change)

`maturin develop` must have been run so `dupehell._core` is importable.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from dupehell import generate


def _read_table(path):
    if Path(path).suffix == ".parquet":
        import pyarrow.parquet as pq

        return pq.read_table(path)
    with pa.memory_map(path, "r") as src:
        return ipc.open_file(src).read_all()


def _gt_maps(gt_path):
    """Return (record_id->master_id, record_id->match_type) from GT file."""
    gt = _read_table(gt_path)
    rid = gt.column("record_id").to_pylist()
    mid = gt.column("master_id").to_pylist()
    mt = gt.column("match_type").to_pylist()
    return dict(zip(rid, mid)), dict(zip(rid, mt))


def _edges(edges_path):
    t = _read_table(edges_path)
    return (
        t.column("source_node_id").to_pylist(),
        t.column("target_node_id").to_pylist(),
        t.column("edge_type").to_pylist(),
        t.column("subtype").to_pylist(),
        t.column("weight").to_pylist(),
    )


def _node_ids(nodes_path):
    return set(_read_table(nodes_path).column("node_id").to_pylist())


# --------------------------------------------------------------------------- #
# 1. node count
# --------------------------------------------------------------------------- #
def test_node_count():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 2000, seed=42, output_dir=d, generate_graph=True)
        nodes = _read_table(r.nodes)
        assert nodes.num_rows == r.total_records


# --------------------------------------------------------------------------- #
# 2. node schema
# --------------------------------------------------------------------------- #
def test_node_schema():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 2000, seed=42, output_dir=d, generate_graph=True)
        nodes = _read_table(r.nodes)
        cols = nodes.schema.names
        assert cols[0] == "node_id"
        assert "record_id" not in cols
        # core dataset columns are retained
        for expected in ("master_id", "entity_type", "domain"):
            assert expected in cols


# --------------------------------------------------------------------------- #
# 3. FK edge integrity
# --------------------------------------------------------------------------- #
def test_fk_edge_integrity():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 5000, seed=42, output_dir=d, generate_graph=True)
        node_ids = _node_ids(r.nodes)
        _s, tgt, etype, _st, _w = _edges(r.edges)
        fk_targets = [t for (t, e) in zip(tgt, etype) if e == "fk"]
        assert fk_targets, "fintech should emit FK edges"
        for t in fk_targets:
            assert t in node_ids, f"FK target {t} not a node"


# --------------------------------------------------------------------------- #
# 4. dup edge integrity
# --------------------------------------------------------------------------- #
def test_dup_edge_integrity():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 5000, seed=42, output_dir=d, generate_graph=True)
        rid_to_mid, _ = _gt_maps(r.ground_truth)
        src, tgt, etype, _st, _w = _edges(r.edges)
        checked = 0
        for (s, t, e) in zip(src, tgt, etype):
            if e == "exact_dup":
                assert rid_to_mid[s] == rid_to_mid[t], (
                    f"exact_dup edge links different masters: {s} vs {t}"
                )
                checked += 1
        assert checked > 0


# --------------------------------------------------------------------------- #
# 5. dup edge count
# --------------------------------------------------------------------------- #
def test_dup_edge_count():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 5000, seed=42, output_dir=d, generate_graph=True)
        rid_to_mid, _ = _gt_maps(r.ground_truth)
        src, tgt, etype, _st, _w = _edges(r.edges)

        # cluster size per master among exact_dup records
        from collections import Counter

        size_by_mid = Counter(
            m for m in rid_to_mid.values() if _is_dup(rid_to_mid, m)
        )
        edges_by_mid = Counter()
        for (s, t, e) in zip(src, tgt, etype):
            if e == "exact_dup":
                edges_by_mid[rid_to_mid[s]] += 1

        for mid, k in size_by_mid.items():
            expected = k * (k - 1) // 2
            assert edges_by_mid[mid] == expected, (
                f"master {mid}: {k} records -> {expected} edges, "
                f"got {edges_by_mid[mid]}"
            )


def _is_dup(rid_to_mid, mid):
    """A master is duplicated if >1 of its records are exact_dup in GT."""
    from collections import Counter

    c = Counter(1 for m in rid_to_mid.values() if m == mid)
    return c[mid] >= 2


# --------------------------------------------------------------------------- #
# 6. hard_neg edge integrity
# --------------------------------------------------------------------------- #
def test_hard_neg_edge_integrity():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 5000, seed=42, output_dir=d, generate_graph=True)
        _rid_to_mid, rid_to_mt = _gt_maps(r.ground_truth)
        src, _tgt, etype, _st, _w = _edges(r.edges)
        checked = 0
        for (s, e) in zip(src, etype):
            if e == "hard_neg":
                assert rid_to_mt[s] == "hard_neg", (
                    f"hard_neg edge source {s} is {rid_to_mt[s]}, not hard_neg"
                )
                checked += 1
        assert checked > 0


# --------------------------------------------------------------------------- #
# 7. three edge types
# --------------------------------------------------------------------------- #
def test_three_edge_types():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 5000, seed=42, output_dir=d, generate_graph=True)
        _s, _t, etype, _st, _w = _edges(r.edges)
        assert {"fk", "exact_dup", "hard_neg"}.issubset(set(etype))


# --------------------------------------------------------------------------- #
# 8. determinism (tabular data unaffected by --graph)
# --------------------------------------------------------------------------- #
def test_determinism_tabular_unchanged():
    with tempfile.TemporaryDirectory() as d:
        r1 = generate("kyc", 3000, seed=7, output_dir=d, generate_graph=True)
        # same run_id -> dataset path is overwritten by the no-graph run
        r2 = generate("kyc", 3000, seed=7, output_dir=d, generate_graph=False)

        t1 = _read_table(r1.dataset).replace_schema_metadata(None)
        t2 = _read_table(r2.dataset).replace_schema_metadata(None)
        assert t1.equals(t2), "dataset differs with/without --graph (RNG changed)"
        assert r1.total_records == r2.total_records


# --------------------------------------------------------------------------- #
# 9. IPC readable
# --------------------------------------------------------------------------- #
def test_ipc_readable():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 2000, seed=42, output_dir=d,
                     generate_graph=True, graph_format="ipc")
        # open_file must succeed
        with pa.memory_map(r.nodes, "r") as src:
            ipc.open_file(src).read_all()
        with pa.memory_map(r.edges, "r") as src:
            ipc.open_file(src).read_all()


# --------------------------------------------------------------------------- #
# 10. graph disabled
# --------------------------------------------------------------------------- #
def test_graph_disabled():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 2000, seed=42, output_dir=d, generate_graph=False)
        assert r.nodes is None
        assert r.edges is None


# --------------------------------------------------------------------------- #
# 11. spanning-tree contract (per-cluster edge count consistent)
# --------------------------------------------------------------------------- #
def test_spanning_tree_contract():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 8000, seed=42, output_dir=d, generate_graph=True)
        rid_to_mid, _ = _gt_maps(r.ground_truth)
        src, tgt, etype, _st, _w = _edges(r.edges)

        size_by_mid = {}
        for m in rid_to_mid.values():
            size_by_mid[m] = size_by_mid.get(m, 0) + 1
        edges_by_mid = {}
        for (s, t, e) in zip(src, tgt, etype):
            if e == "exact_dup":
                edges_by_mid[rid_to_mid[s]] = edges_by_mid.get(rid_to_mid[s], 0) + 1

        for mid, k in size_by_mid.items():
            if k < 2:
                continue
            n = edges_by_mid.get(mid, 0)
            complete = k * (k - 1) // 2
            # Either the full complete graph, or the spanning-tree fallback (k-1).
            assert n == complete or n == k - 1, (
                f"master {mid}: {k} records but {n} exact_dup edges "
                f"(expected {complete} or {k - 1})"
            )


# --------------------------------------------------------------------------- #
# 12. FK pool correctness (FK edges resolve -> 2-col pool id+record_id)
# --------------------------------------------------------------------------- #
def test_fk_pool_record_ids_resolve():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 5000, seed=42, output_dir=d, generate_graph=True)
        node_ids = _node_ids(r.nodes)
        _s, tgt, etype, _st, _w = _edges(r.edges)
        fk = [(t, e) for (t, e) in zip(tgt, etype) if e == "fk"]
        assert fk, "expected FK edges from fintech 2-col FK pool"
        # Every FK edge target must be a real node_id, which is only possible
        # if the FK pool's 2nd column (record_id) was populated correctly.
        for (t, _e) in fk:
            assert t in node_ids


# --------------------------------------------------------------------------- #
# 13. GT backward-compat (GT file still well-formed after cluster_map change)
# --------------------------------------------------------------------------- #
def test_gt_backward_compat():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 3000, seed=42, output_dir=d, generate_graph=True)
        gt = _read_table(r.ground_truth)
        assert gt.schema.names == [
            "record_id",
            "master_id",
            "entity_type",
            "match_type",
            "difficulty",
        ]
        assert set(gt.column("match_type").to_pylist()).issubset(
            {"exact_dup", "hard_neg", "unique", "canary"}
        )


# --------------------------------------------------------------------------- #
# Bonus: parquet graph format
# --------------------------------------------------------------------------- #
def test_graph_parquet_format():
    with tempfile.TemporaryDirectory() as d:
        r = generate("fintech", 2000, seed=42, output_dir=d,
                     generate_graph=True, graph_format="parquet")
        assert r.nodes.endswith(".parquet")
        assert r.edges.endswith(".parquet")
        import pyarrow.parquet as pq

        assert pq.read_table(r.nodes).num_rows == r.total_records
        assert pq.read_table(r.edges).num_rows > 0
