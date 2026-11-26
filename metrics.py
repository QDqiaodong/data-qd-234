"""Fixed-cardinality cumulative latency observations."""
from bisect import bisect_left
from itertools import accumulate
from threading import Lock


class Histograms:
    def __init__(self, operations, bounds_us):
        operations, bounds = tuple(operations), tuple(bounds_us)
        if (not operations or any(not isinstance(key, str) or not key for key in operations)
                or len(set(operations)) != len(operations)):
            raise ValueError("operation names must be nonempty and unique")
        if (not bounds or any(type(n) is not int or n < 0 for n in bounds)
                or any(a >= b for a, b in zip(bounds, bounds[1:]))):
            raise ValueError("bounds must be strictly increasing nonnegative integers")
        self._bounds = bounds
        self._rows = {key: ([0] * (len(bounds) + 1), 0) for key in sorted(operations)}
        self._lock = Lock()

    def observe(self, operation, duration_us):
        if not isinstance(operation, str):
            raise ValueError("operation must be a string")
        if type(duration_us) is not int or duration_us < 0:
            raise ValueError("duration_us must be a nonnegative integer")
        bucket = bisect_left(self._bounds, duration_us)
        with self._lock:
            bins, total = self._rows[operation]
            bins[bucket] += 1
            self._rows[operation] = bins, total + duration_us

    def snapshot(self):
        """Snapshot every operation at a single lock boundary."""
        with self._lock:
            captured = [(key, list(bins), total) for key, (bins, total) in self._rows.items()]
        series = []
        for key, bins, total in captured:
            cumulative = list(accumulate(bins))
            series.append({"operation": key, "le_counts": cumulative[:-1],
                           "count": cumulative[-1], "sum_us": total})
        return {"format": "latency-histogram-v1", "bounds_us": list(self._bounds), "series": series}
