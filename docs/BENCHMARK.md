<!-- DupeHell -- MIT License -->

# Benchmarks

**Environment:** local dev machine, `cargo build --release`, IPC output format,
`--difficulty hell` (heaviest noise profile — worst case for throughput),
generated dataset deleted immediately after each run (no disk accumulation
between runs). No `--pcore-only`, no `--graph`.

## Throughput across scale, all 40 domains (hell, IPC)

| domain | 1M rec/s | 10M rec/s | 20M rec/s | 50M rec/s |
|---|---:|---:|---:|---:|
| academia | 622,144 | 683,212 | 669,514 | 663,588 |
| agriculture | 828,807 | 906,747 | 867,488 | 868,849 |
| automotive | 709,613 | 778,949 | 767,109 | 727,007 |
| aviation | 710,171 | 755,635 | 722,878 | 704,487 |
| banking | 710,102 | 692,235 | 669,452 | 575,970 |
| biotech | 709,749 | 744,161 | 717,300 | 654,572 |
| blockchain | 711,235 | 733,160 | 712,426 | 637,807 |
| construction | 709,358 | 692,647 | 678,508 | 652,848 |
| crm | 415,100 | 361,426 | 356,293 | 348,333 |
| cybersecurity | 827,427 | 844,872 | 830,644 | 746,488 |
| ecommerce | 552,355 | 514,153 | 509,056 | 474,150 |
| education | 711,000 | 722,095 | 712,558 | 675,849 |
| energy | 825,921 | 906,391 | 898,242 | 865,929 |
| fashion | 621,849 | 656,177 | 635,272 | 642,742 |
| fintech | 620,922 | 673,992 | 660,636 | 634,489 |
| food_beverage | 709,900 | 733,514 | 717,530 | 714,540 |
| gaming | 710,216 | 767,086 | 738,995 | 794,338 |
| healthcare | 551,833 | 503,920 | 498,827 | 507,992 |
| hospitality | 552,912 | 524,362 | 511,621 | 535,281 |
| hr | 711,004 | 722,440 | 717,599 | 661,574 |
| insurance | 452,529 | 441,034 | 420,857 | 471,512 |
| kyc | 311,029 | 288,302 | 275,617 | 302,728 |
| legal | 709,238 | 791,063 | 755,645 | 628,084 |
| logistics | 710,747 | 755,422 | 749,899 | 740,152 |
| manufacturing | 710,650 | 778,909 | 772,993 | 796,808 |
| maritime | 709,767 | 722,719 | 712,468 | 714,469 |
| media | 827,592 | 803,280 | 791,676 | 814,886 |
| mining | 827,984 | 875,162 | 859,581 | 865,758 |
| nonprofit | 709,624 | 755,336 | 744,324 | 729,096 |
| pharma | 826,350 | 817,160 | 824,388 | 771,845 |
| publishing | 621,424 | 622,727 | 623,411 | 574,532 |
| realestate | 621,925 | 682,976 | 664,928 | 612,687 |
| renewable_energy | 991,778 | 1,038,939 | 997,473 | 845,262 |
| retail | 828,253 | 874,524 | 867,450 | 712,399 |
| social_media | 987,840 | 996,633 | 987,872 | 887,246 |
| sports | 826,838 | 940,627 | 932,322 | 755,686 |
| supplychain | 622,356 | 615,538 | 604,391 | 475,792 |
| technology | 620,669 | 593,755 | 590,352 | 474,921 |
| telecom | 828,190 | 874,816 | 867,428 | 636,034 |
| travel | 552,622 | 566,793 | 560,501 | 502,743 |

**Summary (n=40 per tier):**

| tier | avg rec/s | slowest | fastest | avg peak RSS |
|---|---:|---|---|---:|
| 1M | 695,476 | kyc — 311,029 | renewable_energy — 991,778 | 256.2 MB |
| 10M | 718,822 | kyc — 288,302 | renewable_energy — 1,038,939 | 563.5 MB |
| 20M | 704,888 | kyc — 275,617 | renewable_energy — 997,473 | 659.8 MB |
| 50M | 659,987 | kyc — 302,728 | social_media — 887,246 | 1,125.6 MB |

`kyc` is the slowest domain at every scale tier — its `natural_person`
entity is the widest, most PII-dense schema in the crate, and at `hell`
all 9 noise categories are simultaneously active on it (vs. often a single
active category on sparser schemas). This is a confirmed, accepted
workload difference, not a perf regression — see the `hunt2407` perf pass
for the fixes already applied on this path.

Throughput per domain generally holds flat or dips only slightly from 1M
to 20M, then drops more noticeably at 50M for the heavier/denser schemas
(`crm`, `ecommerce`, `insurance`, `technology`, `supplychain`, `telecom`) —
consistent with the known non-monotonic slowdown at higher record counts
(I/O/chunking-bound, tracked separately).

50M/40-domain pass was the last full-sweep evaluation tier; future scale
work will use a narrower domain selection rather than repeating the full
40-domain matrix at higher sizes.

`--graph`, `--pcore-only`, and other difficulty tiers not covered in this
pass — see `project_benchmark_difficulty_format_backlog` memory for the
broader matrix protocol and prior partial results.

## IPC vs Parquet (hell), all 40 domains

Parquet is the CLI default; IPC is the internal/streaming-oriented format.
Same sweep methodology, same seed, same difficulty, at 1M/5M/10M/20M
(50M Parquet intentionally not run yet).

| domain | 1M IPC | 1M Parquet | 5M IPC | 5M Parquet | 10M IPC | 10M Parquet | 20M IPC | 20M Parquet |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| academia | 622,144 | 621,600 | 711,721 | 607,968 | 683,212 | 608,112 | 669,514 | 615,699 |
| agriculture | 828,807 | 829,140 | 922,521 | 829,782 | 906,747 | 830,352 | 867,488 | 810,966 |
| automotive | 709,613 | 710,469 | 803,687 | 733,243 | 778,949 | 744,032 | 767,109 | 727,889 |
| aviation | 710,171 | 710,833 | 755,397 | 692,056 | 755,635 | 712,361 | 722,878 | 692,713 |
| banking | 710,102 | 621,612 | 710,681 | 655,777 | 692,235 | 647,436 | 669,452 | 627,293 |
| biotech | 709,749 | 710,128 | 754,935 | 712,219 | 744,161 | 712,125 | 717,300 | 687,942 |
| blockchain | 711,235 | 709,761 | 754,900 | 711,099 | 733,160 | 702,331 | 712,426 | 692,713 |
| construction | 709,358 | 621,620 | 711,889 | 673,112 | 692,647 | 673,665 | 678,508 | 656,055 |
| crm | 415,100 | 382,505 | 389,618 | 366,693 | 361,426 | 339,307 | 356,293 | 339,374 |
| cybersecurity | 827,427 | 826,002 | 859,046 | 830,886 | 844,872 | 831,042 | 830,644 | 817,488 |
| ecommerce | 552,355 | 497,770 | 553,647 | 519,373 | 514,153 | 514,096 | 509,056 | 489,049 |
| education | 711,000 | 708,429 | 754,511 | 732,270 | 722,095 | 702,301 | 712,558 | 692,755 |
| energy | 825,921 | 827,303 | 923,035 | 923,088 | 906,391 | 906,816 | 898,242 | 882,633 |
| fashion | 621,849 | 621,361 | 673,215 | 654,964 | 656,177 | 631,106 | 635,272 | 623,600 |
| fintech | 620,922 | 619,965 | 692,578 | 673,637 | 673,992 | 656,110 | 660,636 | 647,778 |
| food_beverage | 709,900 | 709,709 | 754,974 | 712,154 | 733,514 | 702,477 | 717,530 | 707,340 |
| gaming | 710,216 | 709,331 | 733,089 | 754,860 | 767,086 | 722,765 | 738,995 | 738,835 |
| healthcare | 551,833 | 497,856 | 541,974 | 519,181 | 503,920 | 488,855 | 498,827 | 486,614 |
| hospitality | 552,912 | 495,989 | 541,975 | 518,972 | 524,362 | 514,231 | 511,621 | 498,691 |
| hr | 711,004 | 620,875 | 732,677 | 733,222 | 722,440 | 702,354 | 717,599 | 697,523 |
| insurance | 452,529 | 414,640 | 453,212 | 444,754 | 441,034 | 419,170 | 420,857 | 408,837 |
| kyc | 311,029 | 292,798 | 311,732 | 293,429 | 288,302 | 280,142 | 275,617 | 268,910 |
| legal | 709,238 | 710,624 | 804,069 | 778,986 | 791,063 | 778,888 | 755,645 | 767,372 |
| logistics | 710,747 | 621,358 | 755,212 | 733,233 | 755,422 | 744,119 | 749,899 | 744,294 |
| manufacturing | 710,650 | 709,229 | 778,060 | 778,260 | 778,909 | 767,089 | 772,993 | 761,055 |
| maritime | 709,767 | 621,773 | 733,184 | 712,391 | 722,719 | 702,129 | 712,468 | 702,377 |
| media | 827,592 | 710,538 | 803,263 | 804,204 | 803,280 | 791,204 | 791,676 | 778,813 |
| mining | 827,984 | 707,346 | 890,270 | 888,429 | 875,162 | 874,284 | 859,581 | 845,178 |
| nonprofit | 709,624 | 708,596 | 778,830 | 778,317 | 755,336 | 755,627 | 744,324 | 712,590 |
| pharma | 826,350 | 709,330 | 859,499 | 830,907 | 817,160 | 830,328 | 824,388 | 824,275 |
| publishing | 621,424 | 551,722 | 622,696 | 608,016 | 622,727 | 608,086 | 623,411 | 611,810 |
| realestate | 621,925 | 551,782 | 712,285 | 673,629 | 682,976 | 673,758 | 664,928 | 669,538 |
| renewable_energy | 991,778 | 825,710 | 1,037,693 | 997,032 | 1,038,939 | 1,037,448 | 997,473 | 1,017,798 |
| retail | 828,253 | 708,571 | 890,531 | 859,687 | 874,524 | 874,843 | 867,450 | 852,490 |
| social_media | 987,840 | 826,065 | 1,037,761 | 996,162 | 996,633 | 1,017,430 | 987,872 | 997,256 |
| sports | 826,838 | 828,584 | 958,781 | 922,831 | 940,627 | 923,401 | 932,322 | 932,101 |
| supplychain | 622,356 | 552,096 | 623,217 | 622,927 | 615,538 | 615,486 | 604,391 | 600,987 |
| technology | 620,669 | 552,943 | 608,195 | 593,654 | 593,755 | 572,748 | 590,352 | 573,111 |
| telecom | 828,190 | 826,997 | 889,771 | 830,548 | 874,816 | 859,394 | 867,428 | 860,124 |
| travel | 552,622 | 552,349 | 566,593 | 530,318 | 566,793 | 542,080 | 560,501 | 547,770 |

**Summary (avg across 40 domains):**

| size | IPC avg rec/s | Parquet avg rec/s | Parquet overhead |
|---|---:|---:|---:|
| 1M | 695,476 | 650,883 | -6.4% |
| 5M | 734,773 | 705,807 | -3.9% |
| 10M | 718,822 | 700,238 | -2.6% |
| 20M | 704,888 | 690,241 | -2.1% |

Contrary to the initial expectation (a 20-40% Parquet cost from ZSTD(3)
compression), the measured gap is modest and **shrinks** as scale grows —
the fixed per-file compression overhead gets amortized over more rows.
Parquet (the CLI default) is a reasonable default at any scale tested here;
IPC remains the better choice for pure raw-throughput or internal
streaming use cases.
