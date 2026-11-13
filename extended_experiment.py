Warning: fs was declared with const; use let for reassignable variables.
Warning: s was declared with const; use let for reassignable variables.
#!/usr/bin/env python3
"""Reproducible held-out trace replay for the preregistered cache comparison.

Input records are libCacheSim oracleGeneral binary records (24-byte little-
endian structs). This implementation reads the first 1,000,000 requests from
each downloaded trace, warms each policy on requests 1..100,000, then measures
requests 100,001..1,000,000. It emits policy-level raw CSV and paired 10,000-
request block contrasts. No Linux page-cache outcome is measured here.
"""
from __future__ import annotations

import argparse
import csv
import heapq
import random
import struct
from collections import OrderedDict
from pathlib import Path

try:
    import zstandard
except ImportError as exc:  # pragma: no cover - environment diagnostic
    raise SystemExit("Install zstandard==0.25.0 (see requirements.txt)") from exc

CAPACITIES = (64, 256, 1024)
POLICIES = (
    "lru", "arc", "s3fifo", "sieve", "original", "no-override",
    "observed-one-hit", "active-inactive", "scan-rule-lru",
)
RECORD = struct.Struct("<IQIq")
REQUEST_LIMIT = 1_000_000
WARMUP = 100_000
BLOCK = 10_000
BOOTSTRAPS = 10_000
BOOTSTRAP_SEED = 20261114
MARGIN_PP = 0.5


class LRU:
    def __init__(self, cap):
        self.cap = cap
        self.q = OrderedDict()

    def access(self, key):
        if key in self.q:
            self.q.move_to_end(key)
            return True
        if len(self.q) == self.cap:
            self.q.popitem(last=False)
        self.q[key] = None
        return False


class ARC:
    """ARC with T1/T2 resident lists, B1/B2 ghost lists, and adaptive p."""
    def __init__(self, cap):
        self.c = cap
        self.p = 0.0
        self.t1, self.t2 = OrderedDict(), OrderedDict()
        self.b1, self.b2 = OrderedDict(), OrderedDict()

    def _replace(self, key):
        if self.t1 and ((key in self.b2 and len(self.t1) == self.p) or len(self.t1) > self.p):
            old, _ = self.t1.popitem(last=False)
            self.b1[old] = None
        elif self.t2:
            old, _ = self.t2.popitem(last=False)
            self.b2[old] = None
        elif self.t1:
            old, _ = self.t1.popitem(last=False)
            self.b1[old] = None
        while len(self.b1) > self.c:
            self.b1.popitem(last=False)
        while len(self.b2) > self.c:
            self.b2.popitem(last=False)

    def access(self, key):
        if key in self.t1:
            self.t1.pop(key)
            self.t2[key] = None
            return True
        if key in self.t2:
            self.t2.move_to_end(key)
            return True
        if key in self.b1:
            delta = max(1.0, len(self.b2) / max(1, len(self.b1)))
            self.p = min(float(self.c), self.p + delta)
            self._replace(key)
            self.b1.pop(key)
            self.t2[key] = None
            return False
        if key in self.b2:
            delta = max(1.0, len(self.b1) / max(1, len(self.b2)))
            self.p = max(0.0, self.p - delta)
            self._replace(key)
            self.b2.pop(key)
            self.t2[key] = None
            return False
        if len(self.t1) + len(self.b1) == self.c:
            if len(self.t1) < self.c:
                self.b1.popitem(last=False)
                self._replace(key)
            else:
                # ARC drops this resident directly in the full-T1 special
                # case; it does not create a B1 ghost entry here.
                self.t1.popitem(last=False)
        elif len(self.t1) + len(self.b1) < self.c and len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2) >= self.c:
            if len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2) >= 2 * self.c:
                if self.b2:
                    self.b2.popitem(last=False)
            self._replace(key)
        self.t1[key] = None
        return False


class S3FIFO:
    """Unit-object port of the pinned libCacheSim 0.3.5 S3FIFO.c."""
    def __init__(self, cap):
        self.cap = cap
        self.small_cap = int(cap * 0.10)
        self.main_cap = cap - self.small_cap
        self.ghost_cap = int(cap * 0.90)
        self.small, self.main, self.ghost = OrderedDict(), OrderedDict(), OrderedDict()
        self.has_evicted = False

    def _evict_small(self):
        evicted = False
        while not evicted and self.small:
            key, freq = self.small.popitem(last=False)
            if freq >= 2:
                self.main[key] = 0
            else:
                self.ghost[key] = None
                while len(self.ghost) > self.ghost_cap:
                    self.ghost.popitem(last=False)
                evicted = True

    def _evict_main(self):
        evicted = False
        while not evicted and self.main:
            key, freq = self.main.popitem(last=False)
            if freq >= 1:
                self.main[key] = min(freq, 3) - 1
            else:
                evicted = True

    def _evict(self):
        self.has_evicted = True
        if len(self.main) > self.main_cap or not self.small:
            self._evict_main()
        else:
            self._evict_small()

    def access(self, key):
        if key in self.small:
            self.small[key] += 1
            return True
        if key in self.main:
            self.main[key] += 1
            return True
        in_ghost = key in self.ghost
        self.ghost.pop(key, None)
        if in_ghost:
            target = self.main
        else:
            target = self.main if not self.has_evicted and len(self.small) >= self.small_cap else self.small
        while len(self.small) + len(self.main) >= self.cap:
            self._evict()
        target[key] = 0
        return False


class SIEVE:
    """Unit-object port of the pinned libCacheSim Sieve.c hand semantics."""
    def __init__(self, cap):
        self.cap = cap
        self.q = OrderedDict()
        self.hand = None

    def access(self, key):
        if key in self.q:
            self.q[key] = True
            return True
        while len(self.q) >= self.cap:
            keys = list(self.q)
            start = keys.index(self.hand) if self.hand in self.q else 0
            for offset in range(2 * len(keys)):
                old = keys[(start + offset) % len(keys)]
                if self.q[old]:
                    self.q[old] = False
                    continue
                victim_idx = keys.index(old)
                self.q.pop(old)
                remaining = list(self.q)
                self.hand = remaining[victim_idx] if victim_idx < len(remaining) else None
                break
        self.q[key] = False
        return False


class ActiveInactive:
    """Two-list active/inactive approximation, active target floor(C/2)."""
    def __init__(self, cap):
        self.cap = cap
        self.active_cap = max(1, cap // 2)
        self.active, self.inactive = OrderedDict(), OrderedDict()

    def _demote(self):
        if self.active:
            key, _ = self.active.popitem(last=False)
            self.inactive[key] = None

    def _evict(self):
        if not self.inactive:
            self._demote()
        if self.inactive:
            self.inactive.popitem(last=False)

    def access(self, key):
        if key in self.active:
            self.active.move_to_end(key)
            return True
        if key in self.inactive:
            self.inactive.pop(key)
            self.active[key] = None
            if len(self.active) > self.active_cap:
                self._demote()
            return True
        if len(self.active) + len(self.inactive) >= self.cap:
            self._evict()
        self.inactive[key] = None
        return False


class ScorePolicy:
    """Heap-based exact recency/frequency and predicate diagnostics.

    Score equations follow the manuscript's executed definitions. `scan-rule-
    lru` uses 2*I[id>=100 and freq<=2] + age/100000 exactly; it is not silently
    changed to a strict-priority tier for long traces.
    """
    def __init__(self, cap, mode):
        self.cap, self.mode, self.t = cap, mode, 0
        self.e = {}  # key -> [freq, last, admission_seq, version, kind]
        self.seq = 0
        self.versions = 0
        self.expiry, self.unsat, self.sat, self.override, self.rule = [], [], [], [], []

    @staticmethod
    def _freq_term(freq):
        return 0.53 * max(0.0, 1.0 - freq / 6.0)

    def _kind(self, key, freq, last):
        if self.mode == "original" and key >= 100 and freq <= 2:
            return "override"
        if self.mode == "observed-one-hit" and freq == 1:
            return "override"
        if self.mode == "scan-rule-lru":
            return "rule"
        return "unsat" if self.t - last < 40 else "sat"

    def _push(self, key, rec):
        freq, last, seq, ver, kind = rec
        if kind == "override":
            # Override score grows with age, so the smallest last-access time
            # is the highest-priority victim.
            heapq.heappush(self.override, (last, seq, key, ver))
        elif kind == "rule":
            flag = 2.0 if key >= 100 and freq <= 2 else 0.0
            # The common t/100000 term is omitted from the priority key.
            heapq.heappush(self.rule, (-(flag - last / 100000.0), seq, key, ver))
        elif kind == "unsat":
            priority = self._freq_term(freq) - 0.008 * last
            heapq.heappush(self.unsat, (-priority, seq, key, ver))
            heapq.heappush(self.expiry, (last + 40, seq, key, ver))
        else:
            heapq.heappush(self.sat, (-self._freq_term(freq), seq, key, ver))

    def _compact_heaps(self):
        limit = max(256, 8 * self.cap)
        if any(len(h) > limit for h in (self.expiry, self.unsat, self.sat, self.override, self.rule)):
            self.expiry, self.unsat, self.sat, self.override, self.rule = [], [], [], [], []
            for key, rec in self.e.items():
                self._push(key, rec)

    def _promote_expired(self):
        while self.expiry and self.expiry[0][0] <= self.t:
            _, _, key, ver = heapq.heappop(self.expiry)
            rec = self.e.get(key)
            if rec and rec[3] == ver and rec[4] == "unsat":
                rec[4] = "sat"
                self._push(key, rec)

    def _top(self, heap, kind):
        while heap:
            _, _, key, ver = heap[0]
            rec = self.e.get(key)
            if rec and rec[3] == ver and rec[4] == kind:
                return key, rec
            heapq.heappop(heap)
        return None

    def _victim(self):
        self._promote_expired()
        if self.mode == "scan-rule-lru":
            return self._top(self.rule, "rule")[0]
        ov = self._top(self.override, "override")
        if ov:
            return ov[0]
        u = self._top(self.unsat, "unsat")
        s = self._top(self.sat, "sat")
        candidates = []
        if u:
            key, rec = u
            score = 0.32 * (self.t - rec[1]) / 40.0 + self._freq_term(rec[0])
            candidates.append((score, -rec[2], key))
        if s:
            key, rec = s
            score = 0.32 + self._freq_term(rec[0])
            candidates.append((score, -rec[2], key))
        return max(candidates)[2]

    def access(self, key):
        self.t += 1
        self._promote_expired()
        rec = self.e.get(key)
        hit = rec is not None
        if hit:
            rec[0] += 1
            rec[1] = self.t
            self.versions += 1
            rec[3] = self.versions
            rec[4] = self._kind(key, rec[0], rec[1])
            self._push(key, rec)
            self._compact_heaps()
            return True
        if len(self.e) >= self.cap:
            victim = self._victim()
            del self.e[victim]
        self.seq += 1
        self.versions += 1
        rec = [1, self.t, self.seq, self.versions, self._kind(key, 1, self.t)]
        self.e[key] = rec
        self._push(key, rec)
        self._compact_heaps()
        return False


def build(name, cap):
    if name == "lru": return LRU(cap)
    if name == "arc": return ARC(cap)
    if name == "s3fifo": return S3FIFO(cap)
    if name == "sieve": return SIEVE(cap)
    if name == "active-inactive": return ActiveInactive(cap)
    return ScorePolicy(cap, name)


def read_trace(path: Path, limit: int):
    """Read oracleGeneral zstd, libCacheSim CSV zstd, or CSV input."""
    if path.name.endswith(".csv.zst"):
        dctx = zstandard.ZstdDecompressor()
        with path.open("rb") as src, dctx.stream_reader(src) as reader:
            import io
            text = io.TextIOWrapper(reader, encoding="utf-8")
            count = 0
            for line in text:
                if count == 0 and line.startswith("#"):
                    continue
                if count >= limit:
                    break
                row = next(csv.reader([line]))
                if len(row) < 2:
                    continue
                yield int(row[1])
                count += 1
        return
    if path.suffix.lower() == ".csv":
        with path.open("r", newline="", encoding="utf-8") as src:
            reader = csv.DictReader(src)
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                yield int(row["lbn"])
        return
    dctx = zstandard.ZstdDecompressor()
    with path.open("rb") as src:
        with dctx.stream_reader(src) as reader:
            for i in range(limit):
                b = reader.read(RECORD.size)
                if not b:
                    break
                if len(b) != RECORD.size:
                    raise ValueError(f"truncated oracleGeneral record {i}: {len(b)} bytes")
                yield RECORD.unpack(b)[1]


def paired_block_ci(orig: bytearray, scan: bytearray):
    diffs = []
    for start in range(0, len(orig), BLOCK):
        end = min(start + BLOCK, len(orig))
        diffs.append(100.0 * (sum(orig[start:end]) - sum(scan[start:end])) / (end - start))
    if len(diffs) < 2:
        return diffs[0] if diffs else float("nan"), float("nan"), float("nan"), diffs
    rng = random.Random(BOOTSTRAP_SEED)
    n = len(diffs)
    means = []
    for _ in range(BOOTSTRAPS):
        means.append(sum(diffs[rng.randrange(n)] for _ in range(n)) / n)
    means.sort()
    point = 100.0 * (sum(orig) - sum(scan)) / len(orig)
    lo = means[int(0.05 * BOOTSTRAPS)]
    hi = means[int(0.95 * BOOTSTRAPS)]
    return point, lo, hi, diffs


def run_one(trace: Path, cap: int, limit: int, warmup: int):
    objs = [build(p, cap) for p in POLICIES]
    misses = {p: 0 for p in POLICIES}
    raw = {p: bytearray() for p in POLICIES}
    n = 0
    for key in read_trace(trace, limit):
        n += 1
        for name, policy in zip(POLICIES, objs):
            miss = not policy.access(key)
            if n > warmup:
                misses[name] += miss
                raw[name].append(int(miss))
    if n <= warmup:
        raise ValueError(f"trace has only {n} requests; need > {warmup}")
    point, lo, hi, blocks = paired_block_ci(raw["original"], raw["scan-rule-lru"])
    row = {
        "trace": trace.name, "cluster": trace.name.split(".")[0], "capacity": cap,
        "requests_total": n, "warmup_requests": min(warmup, n),
        "measured_requests": n - min(warmup, n),
        **{f"misses_{p.replace('-', '_')}": misses[p] for p in POLICIES},
        **{f"miss_pct_{p.replace('-', '_')}": 100.0 * misses[p] / max(1, n - min(WARMUP, n)) for p in POLICIES},
        "delta_original_minus_scan_rule_pp": point,
        "ci90_low_pp": lo, "ci90_high_pp": hi,
        "equivalent_pm_0_5pp": bool(lo > -MARGIN_PP and hi < MARGIN_PP),
        "bootstrap_blocks": len(blocks),
        "bootstrap_block_requests": BLOCK,
        "bootstrap_replicates": BOOTSTRAPS,
        "bootstrap_seed": BOOTSTRAP_SEED,
    }
    block_rows = [{"trace": trace.name, "cluster": row["cluster"], "capacity": cap,
                   "block_index": i, "paired_delta_original_minus_scan_rule_pp": d}
                  for i, d in enumerate(blocks)]
    return row, block_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace", nargs="+", required=True, help="one or more .zst trace files")
    ap.add_argument("--slots", nargs="+", type=int, default=list(CAPACITIES))
    ap.add_argument("--policies", nargs="+", choices=POLICIES, default=list(POLICIES))
    ap.add_argument("--limit", type=int, default=REQUEST_LIMIT)
    ap.add_argument("--warmup", type=int, default=WARMUP)
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    if tuple(args.policies) != POLICIES:
        raise SystemExit("The preregistered run requires the full registered policy list in its registered order.")
    if tuple(args.slots) != CAPACITIES:
        raise SystemExit(f"The preregistered capacities are {CAPACITIES}.")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    raw_path = out / "heldout_trace_raw.csv"
    blocks_path = out / "heldout_trace_paired_blocks.csv"
    fields = None
    with raw_path.open("w", newline="", encoding="utf-8") as fraw, blocks_path.open("w", newline="", encoding="utf-8") as fblocks:
        wraw = wblocks = None
        for trace_s in args.trace:
            trace = Path(trace_s)
            for cap in args.slots:
                row, block_rows = run_one(trace, cap, args.limit, args.warmup)
                if wraw is None:
                    wraw = csv.DictWriter(fraw, fieldnames=list(row))
                    wraw.writeheader()
                    wblocks = csv.DictWriter(fblocks, fieldnames=list(block_rows[0]))
                    wblocks.writeheader()
                wraw.writerow(row)
                wblocks.writerows(block_rows)
                fraw.flush(); fblocks.flush()
                print(f"{trace.name} C={cap}: Original={row['miss_pct_original']:.4f}% "
                      f"Scan-rule-LRU={row['miss_pct_scan_rule_lru']:.4f}% "
                      f"delta={row['delta_original_minus_scan_rule_pp']:.4f} pp "
                      f"90%CI=[{row['ci90_low_pp']:.4f},{row['ci90_high_pp']:.4f}] "
                      f"equivalent={row['equivalent_pm_0_5pp']}")
    print(f"Wrote {raw_path} and {blocks_path}")


if __name__ == "__main__":
    main()
