Warning: fs was declared with const; use let for reassignable variables.
Warning: s was declared with const; use let for reassignable variables.
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from extended_experiment import LRU, ARC, S3FIFO, SIEVE, ActiveInactive, ScorePolicy


class SlowScore:
    """Reference implementation used only to validate the heap implementation."""
    def __init__(self, cap, mode):
        self.cap, self.mode, self.t, self.seq = cap, mode, 0, 0
        self.e = {}  # key -> [freq, last, admission sequence]

    def score(self, key, rec):
        freq, last, _ = rec
        age = self.t - last
        normal = 0.32 * min(age / 40.0, 1.0) + 0.53 * max(0.0, 1.0 - freq / 6.0)
        if self.mode == "original" and key >= 100 and freq <= 2:
            return 2.0 + age / 200.0
        if self.mode == "observed-one-hit" and freq == 1:
            return 2.0 + age / 200.0
        if self.mode == "scan-rule-lru":
            return (2.0 if key >= 100 and freq <= 2 else 0.0) + age / 100000.0
        return normal

    def access(self, key):
        self.t += 1
        if key in self.e:
            self.e[key][0] += 1
            self.e[key][1] = self.t
            return True
        if len(self.e) >= self.cap:
            victim = max(self.e, key=lambda k: (self.score(k, self.e[k]), -self.e[k][2]))
            del self.e[victim]
        self.seq += 1
        self.e[key] = [1, self.t, self.seq]
        return False


def test_score_heaps_match_direct_scoring():
    for mode in ("original", "no-override", "observed-one-hit", "scan-rule-lru"):
        for cap in (4, 8, 16):
            for seed in range(10):
                rng = random.Random(seed)
                fast, slow = ScorePolicy(cap, mode), SlowScore(cap, mode)
                for step in range(5000):
                    key = rng.randrange(160)
                    fast_hit, slow_hit = fast.access(key), slow.access(key)
                    assert fast_hit == slow_hit, (mode, cap, seed, step, key, fast.e, slow.e)
                    assert set(fast.e) == set(slow.e), (mode, cap, seed, step, key, fast.e, slow.e)


def test_policy_capacity_and_miss_counts():
    lru = LRU(2)
    assert [not lru.access(k) for k in (1, 2, 1, 3, 2)] == [True, True, False, True, True]
    for cls in (ARC, S3FIFO, SIEVE, ActiveInactive):
        p = cls(8)
        for i in range(1000):
            p.access(i % 12)
            if cls is ARC:
                assert len(p.t1) + len(p.t2) <= 8
            elif cls is S3FIFO:
                assert len(p.small) + len(p.main) <= 8
            elif cls is SIEVE:
                assert len(p.q) <= 8
            else:
                assert len(p.active) + len(p.inactive) <= 8


if __name__ == "__main__":
    test_score_heaps_match_direct_scoring()
    test_policy_capacity_and_miss_counts()
    print("extended experiment checks passed")
