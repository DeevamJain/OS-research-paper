# Preregistration for Held-Out Twitter Cache Cluster Evaluation

**Status:** Publicly committed before held-out trace download or analysis in commit `731472f79b6bc83800b6858604b3a73e7bfee613`. Existing cluster52 and synthetic-trace results are exploratory and are not part of this confirmatory evaluation.

## Primary hypothesis and practical margin

The primary hypothesis is that Original-score and Scan-rule-LRU have equivalent overall miss rates on the held-out Twitter cache clusters at each registered capacity. The equivalence margin is **±0.5 percentage points** (absolute miss-rate difference). A half-point difference corresponds to at most one additional miss per 200 requests. This threshold defines the largest difference treated as practically negligible for this evaluation; it is fixed before the held-out traces are run and is not estimated from their results.

The paired difference is `Original-score miss rate − Scan-rule-LRU miss rate`, in percentage points. For each cluster-capacity pair, equivalence is supported only when the entire two-sided 90% confidence interval lies strictly within [−0.5, +0.5] percentage points. The full held-out evaluation supports equivalence only if every registered cluster-capacity pair meets this rule. An interval crossing either boundary is inconclusive; an interval wholly or partly beyond a boundary is reported as outside the registered equivalence criterion. No row will be omitted because of its direction or magnitude.

## Policies and capacities

Run every policy with the same trace preprocessing, warm-up, and replay implementation used for Tables 9–10:

- LRU
- ARC
- S3-FIFO
- SIEVE
- No-override
- Observed-one-hit
- Active-inactive-approx
- Original-score
- Scan-rule-LRU (the registered comparator in the primary hypothesis)

Capacities are **64, 256, and 1,024 slots**, matching Tables 9–10. Capacity is measured in simulator slots; these results do not imply equal metadata or Linux memory cost.

## Outcome and interval procedure

The sole primary outcome is overall logical miss rate, calculated over the post-warm-up requests using the unchanged pipeline. For each request, retain the paired miss indicators from Original-score and Scan-rule-LRU and compute their difference. Report each policy's miss rate and each paired difference for every cluster and capacity.

For each cluster-capacity pair, obtain the 90% interval by a paired moving-block bootstrap over the post-warm-up request sequence: block length 10,000 requests; 10,000 resamples; resample blocks jointly for both policies; pseudorandom seed 20261114. The interval is the 5th and 95th percentiles of the bootstrap paired differences. If the available post-warm-up sequence is shorter than 10,000 requests, use one whole-sequence block and mark the interval unavailable rather than silently changing the method. The test is the two one-sided tests (TOST) equivalence procedure at α=0.05, represented by the corresponding 90% interval. Report exact estimates and intervals; do not substitute a nonsignificant difference test for equivalence.

All other policy comparisons are secondary and descriptive. No Linux-performance, production-wide, or field-prevalence claim will be inferred from these simulator outcomes.

## Trace selection

Use exactly three Twitter cache clusters:

1. `cluster18`
2. `cluster37`
3. One additional cluster selected uniformly from the sorted list of available cluster identifiers after excluding cluster18, cluster37, cluster52, and any cluster already used in this study before this preregistration.

Use Python's `random.Random(20261113)` and select one item from that sorted eligible list. Record the complete eligible list, selected identifier, and selection-script output in the experiment log before opening or analyzing the selected trace. If either named cluster is unavailable or was previously analyzed, stop and amend this preregistration in a new dated commit before running any held-out trace; do not replace it based on observed results. If the eligible list is empty, stop and document the limitation.

For every downloaded trace, record the source URL, retrieval date, byte size, and SHA-256 digest in `traces.sha256`. Keep the raw trace immutable and run the registered pipeline against that file.

## Execution and reporting commitments

- Run the unchanged experiment pipeline on all nine cluster-capacity combinations and all registered policies.
- Preserve raw per-request or per-block outcomes and the exact command lines, software versions, configuration, and random seeds.
- Add Table 9b with one row per cluster and capacity, showing paired policy miss rates, paired difference, 90% interval, and the equivalence decision.
- Report null, mixed, adverse, failed, or incomplete results plainly. Do not tune policies or preprocessing after viewing held-out outcomes.
- This registration does not cover the separate privileged Linux evaluation, reference verification, human audit, second-repository analysis, archival DOI deposit, or paper-wide edits; those require their own recorded evidence and actual outputs.


