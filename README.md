# OS cache artifact audit replication package

This package contains the final manuscript, its preregistered held-out Twitter replay, raw policy outcomes, paired block differences, source-run logs, and trace-selection metadata. The held-out replay is complete. The privileged Linux pilot was not run because this host did not expose the required delegated cgroup v2, BPF, BTF, and PSI capabilities; Table 13b reports `Not run` rather than invented outcomes.

## Pinned sources

- Audited artifact: `Samarth-S-Maladkar/ebpf-ml-cache` at `efb3b285b9439d5e1c745d48e22c0e6dede24ed0`.
- Policy reference implementation: `1a1a11a/libCacheSim` at `c6ffd976c83d9e731b37fc91e1ce930039f5ddb9`.
- Dataset: cacheMon public Twitter `oracleGeneral` objects in the S3 bucket. The selected clusters are 18, 37, and 20. Cluster20 was selected by `random.Random(20261113)` from the sorted eligible ID list. See `traces/trace_selection.log` and `traces/eligible_cluster_ids.txt`.
- Pre-registration: `PREREGISTRATION.md`, pushed in commit `731472f79b6bc83800b6858604b3a73e7bfee613` before held-out traces were downloaded or analyzed.

## Results in this package

- `results/heldout_twitter/heldout_trace_raw.csv`: 9 cluster-capacity rows, with all 9 policy rates and exact counts.
- `results/heldout_twitter/heldout_trace_paired_blocks.csv`: 810 paired 10,000-request block differences (90 per cluster-capacity pair).
- `results/historical_cluster52/`: reconstructed 1-million-request logical replay and paired blocks at capacities 64, 256, and 1,024.
- `results/source_test_scoring.txt`, `results/source_ml_cache_sim.txt`, and `results/source_run_metadata.txt`: output and runtime metadata from the pinned upstream checks.

Each trace replay uses the first 1,000,000 oracleGeneral records, warms on the first 100,000, and measures the remaining 900,000. The primary contrast is Original-score minus Scan-rule-LRU. Equivalence requires its complete 90% paired moving-block bootstrap interval to lie strictly inside ±0.50 percentage points. Only 1 of 9 registered cluster-capacity pairs met that bound; the all-pairs equivalence claim was not supported. These are logical-cache measurements, not Linux page-cache performance.

## Reproduction

Use Python 3.12 and install the pinned dependency:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Download the immutable trace files into `traces/` from `https://cache-datasets.s3.amazonaws.com/cache_dataset_oracleGeneral/2020_twitter/`:

```powershell
curl.exe -fL -o traces/cluster18.oracleGeneral.sample10.zst https://cache-datasets.s3.amazonaws.com/cache_dataset_oracleGeneral/2020_twitter/cluster18.oracleGeneral.sample10.zst
curl.exe -fL -o traces/cluster37.oracleGeneral.sample10.zst https://cache-datasets.s3.amazonaws.com/cache_dataset_oracleGeneral/2020_twitter/cluster37.oracleGeneral.sample10.zst
curl.exe -fL -o traces/cluster20.oracleGeneral.sample10.zst https://cache-datasets.s3.amazonaws.com/cache_dataset_oracleGeneral/2020_twitter/cluster20.oracleGeneral.sample10.zst
```

Verify each SHA-256 against `traces.sha256`, then run:

```powershell
python extended_experiment.py --trace traces/cluster18.oracleGeneral.sample10.zst traces/cluster37.oracleGeneral.sample10.zst traces/cluster20.oracleGeneral.sample10.zst --slots 64 256 1024 --policies lru arc s3fifo sieve original no-override observed-one-hit active-inactive scan-rule-lru --out results/heldout_twitter
```

The runner is a reconstructed adapter for the pinned policies, not the missing original author-created supplement. S3-FIFO and SIEVE were checked against the pinned libCacheSim source by reproducing the paper’s cluster52 values. Large trace objects are excluded from Git; the exact byte sizes and hashes are recorded separately.

## Scope and remaining evidence

The current package does not include the missing original supplement, a Linux VM harness, complete historical synthetic-seed or CloudPhysics per-trial CSVs, full literature-search exports, or second-reviewer screening. The earlier manuscript’s synthetic and CloudPhysics table summaries are retained as descriptive source results but cannot be independently recalculated from the files in this package. No Linux latency, throughput, PSI, or controller-CPU value is reported. No Zenodo DOI has been minted.
