# Benchmarks

Domain **kyc**, difficulty **medium**, seed **42**.  
Measurements via `dupehell` Python wheel (PyO3, single-thread).

## Metrics

| Size | Records generated | Time | rec/s | exact_dups | hard_negs | masters |
|---|---|---|---|---|---|---|
| 100 K | 101 506 | 2.6 s | 38 K | 100 000 | 1 500 | 60 306 |
| 500 K | 507 506 | 3.2 s | 158 K | 500 000 | 7 500 | 301 506 |
| 1 M | 1 015 006 | 3.9 s | 261 K | 1 000 000 | 15 000 | 603 006 |
| 5 M | 5 075 006 | 9.6 s | 530 K | 5 000 000 | 75 000 | 3 015 001 |
| 10 M | 10 150 006 | 17.3 s | 586 K | 10 000 000 | 150 000 | 6 029 966 |
| 25 M | ~25 375 006 | 30.3 s | 836 K | 25 000 000 | 375 000 | ~15 075 000 |
| 50 M | ~50 750 006 | 74.5 s | 681 K | 50 000 000 | 750 000 | ~30 150 000 |
| 75 M | ~76 125 006 | 121.1 s | 628 K | 75 000 000 | 1 125 000 | ~45 225 000 |

## Observations

- **Max throughput**: 836 K rec/s at 25M, then plateau at ~630 K rec/s for 75M
- **Bottleneck**: `sink` (generation + IPC write) accounts for ~80% of time; `gt` (ground truth) ~15%; `alloc` (IDs) ~5%
- **10M in 17s**, **50M in 75s**, **75M in 2 min**, **100M in ~3 min** — near-linear scaling
