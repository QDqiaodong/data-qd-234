"""SLO verdict and paid exact-timeout review for one logical window.

The module is intentionally stateless.  Histogram snapshots are cumulative
``latency-histogram-v1`` documents; all window reasoning uses their paired
deltas rather than treating either cumulative snapshot as the window.
"""

from collections.abc import Mapping, Sequence

__all__ = ["evaluate_window"]

_MAX_INT = (1 << 63) - 1
_MAX_SLICES = 32
_MAX_BOUNDS = 64
_MAX_WINDOW_SAMPLES_PER_WORKER = 1_000_000_000
_MAX_REVIEW_BUDGET = 8
_REVIEW_BATCH = 2


def _is_real_int(value):
    # ``bool`` is a subclass of int, but a boolean is not an integer count.
    return type(value) is int


def _uint63(value, name):
    if not _is_real_int(value) or value < 0 or value > _MAX_INT:
        raise ValueError(f"{name} must be a nonnegative integer <= 2**63-1")
    return value


def _text(value, name):
    if not isinstance(value, str) or not 1 <= len(value) <= 128:
        raise ValueError(f"{name} must be a string of 1..128 characters")
    return value


def _bounds(value):
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError("bounds_us must be a sequence of integers")
    if not 1 <= len(value) <= _MAX_BOUNDS:
        raise ValueError("bounds_us must contain 1..64 bounds")
    previous = -1
    for bound in value:
        _uint63(bound, "bucket bound")
        if bound <= previous:
            raise ValueError("bounds_us must be strictly increasing")
        previous = bound
    return tuple(value)


def _bucket_descriptors(bounds):
    """Return inclusive integer sample intervals for raw buckets."""
    descriptors = []
    for index, upper in enumerate(bounds):
        lower = 0 if index == 0 else bounds[index - 1] + 1
        descriptors.append((lower, upper))
    # The final +Inf bucket starts immediately after the last finite bound.
    descriptors.append((bounds[-1] + 1, None))
    return descriptors


def _raw_counts(row, bound_count):
    le_counts = row["le_counts"]
    raw = [le_counts[0]]
    raw.extend(le_counts[i] - le_counts[i - 1] for i in range(1, bound_count))
    raw.append(row["count"] - le_counts[-1])
    return raw


def _assert_histogram_feasible(bounds, row, what):
    """Check that counts and sum can represent integer microsecond samples."""
    descriptors = _bucket_descriptors(bounds)
    raw = _raw_counts(row, len(bounds))
    minimum_sum = 0
    finite_maximum_sum = 0
    has_unbounded_samples = False

    for count, (lower, upper) in zip(raw, descriptors):
        if count < 0:
            raise ValueError(f"{what} bucket counts regress or are inconsistent")
        minimum_sum += count * lower
        if upper is None:
            if count > 0:
                has_unbounded_samples = True
        else:
            finite_maximum_sum += count * upper

    total = row["sum_us"]
    if total < minimum_sum:
        raise ValueError(f"{what} sum_us is too small for its bucket counts")
    if not has_unbounded_samples and total > finite_maximum_sum:
        raise ValueError(f"{what} sum_us is too large for its bucket counts")


def _validate_row(row, bounds, operation):
    if not isinstance(row, Mapping):
        raise ValueError("histogram series rows must be objects")
    op_name = row.get("operation")
    if not isinstance(op_name, str) or not 1 <= len(op_name) <= 128:
        raise ValueError("histogram operation name is invalid")

    le_counts = row.get("le_counts")
    if not isinstance(le_counts, Sequence) or isinstance(le_counts, (str, bytes, bytearray)):
        raise ValueError("le_counts must be a list of integers")
    if len(le_counts) != len(bounds):
        raise ValueError("le_counts length must match bounds_us")

    previous = 0
    checked_le = []
    for value in le_counts:
        _uint63(value, "le_counts value")
        if value < previous:
            raise ValueError("le_counts must be cumulative and nondecreasing")
        previous = value
        checked_le.append(value)

    count = _uint63(row.get("count"), "count")
    total = _uint63(row.get("sum_us"), "sum_us")
    if checked_le[-1] > count:
        raise ValueError("le_counts cannot exceed count")

    normalized = {
        "operation": op_name,
        "le_counts": checked_le,
        "count": count,
        "sum_us": total,
    }
    # Every supplied row must itself be a realizable cumulative snapshot.
    _assert_histogram_feasible(bounds, normalized, "snapshot")
    if op_name == operation:
        return normalized
    return None


def _validate_snapshot(snapshot, operation):
    if not isinstance(snapshot, Mapping):
        raise ValueError("histogram snapshot must be an object")
    if snapshot.get("format") != "latency-histogram-v1":
        raise ValueError("snapshot format must be latency-histogram-v1")

    bounds = _bounds(snapshot.get("bounds_us"))
    series = snapshot.get("series")
    if not isinstance(series, Sequence) or isinstance(series, (str, bytes, bytearray)):
        raise ValueError("series must be a list of rows")
    if len(series) > _MAX_SLICES:
        raise ValueError("a snapshot may contain at most 32 rows")

    target = None
    for row in series:
        normalized = _validate_row(row, bounds, operation)
        if normalized is not None:
            if target is not None:
                raise ValueError("requested operation must occur exactly once")
            target = normalized

    if target is None:
        raise ValueError("requested operation row is missing")
    return bounds, target


def _window_delta(before, after):
    before_le = before["le_counts"]
    after_le = after["le_counts"]
    delta_le = []
    for old, new in zip(before_le, after_le):
        if new < old:
            raise ValueError("cumulative histogram counts regressed")
        delta_le.append(new - old)

    if after["count"] < before["count"] or after["sum_us"] < before["sum_us"]:
        raise ValueError("cumulative histogram counts regressed")

    delta = {
        "le_counts": delta_le,
        "count": after["count"] - before["count"],
        "sum_us": after["sum_us"] - before["sum_us"],
    }
    if delta["count"] > _MAX_WINDOW_SAMPLES_PER_WORKER:
        raise ValueError("a worker window may contain at most 10^9 completions")

    raw_counts = _raw_counts(delta, len(delta_le))
    if any(value < 0 for value in raw_counts):
        raise ValueError("window bucket deltas cannot be negative")
    return delta, raw_counts


def _validate_window_feasible(bounds, delta, raw_counts):
    descriptors = _bucket_descriptors(bounds)
    minimum_sum = 0
    finite_maximum_sum = 0
    unbounded = False
    for count, (lower, upper) in zip(raw_counts, descriptors):
        minimum_sum += count * lower
        if upper is None:
            unbounded = unbounded or count > 0
        else:
            finite_maximum_sum += count * upper
    total = delta["sum_us"]
    if total < minimum_sum:
        raise ValueError("window delta sum_us is too small for its bucket counts")
    if not unbounded and total > finite_maximum_sum:
        raise ValueError("window delta sum_us is too large for its bucket counts")


def _slow_interval(bounds, raw_counts, sum_us, cutoff_us):
    """Return attainable inclusive min/max counts strictly slower than cutoff."""
    descriptors = _bucket_descriptors(bounds)
    base_sum = 0
    fixed_slow = 0
    other_capacity = 0
    unbounded_other_slow = False
    mixed = None

    for count, (lower, upper) in zip(raw_counts, descriptors):
        base_sum += count * lower
        if count == 0:
            continue

        if lower > cutoff_us:
            # Every integer in this bucket is strictly above the SLO cutoff.
            fixed_slow += count
            if upper is None:
                unbounded_other_slow = True
            else:
                other_capacity += count * (upper - lower)
        elif upper is not None and upper <= cutoff_us:
            # Every integer in the bucket is at or below the cutoff.
            other_capacity += count * (upper - lower)
        else:
            # Contiguous integer buckets give at most one cutoff-straddling
            # bucket. Its slow members are exactly those with offset >= need.
            if mixed is not None:  # defensive; valid contiguous buckets prevent it
                raise ValueError("cutoff intersects more than one bucket")
            mixed = (count, lower, upper)

    extra = sum_us - base_sum
    if mixed is None:
        return fixed_slow, fixed_slow

    count, lower, upper = mixed
    need = cutoff_us + 1 - lower  # minimum offset for a slow sample
    max_fast_offset = need - 1

    if unbounded_other_slow:
        minimum_optional_slow = 0
    elif upper is None:
        # The straddling bucket is +Inf. With one slow sample, arbitrary extra
        # sum can be piled onto it; with none, every sample is capped at cutoff.
        maximum_extra_without_slow = other_capacity + count * max_fast_offset
        minimum_optional_slow = 0 if extra <= maximum_extra_without_slow else 1
    else:
        width = upper - lower
        # Making one sample slow raises its maximum offset by this amount.
        capacity_gain = width - max_fast_offset
        base_capacity = other_capacity + count * max_fast_offset
        if extra <= base_capacity:
            minimum_optional_slow = 0
        else:
            minimum_optional_slow = (
                extra - base_capacity + capacity_gain - 1
            ) // capacity_gain

    # Every optional slow sample needs at least ``need`` extra microseconds from
    # its bucket lower bound. This bounds the maximum possible slow count.
    maximum_optional_slow = min(count, extra // need)

    slow_min = fixed_slow + minimum_optional_slow
    slow_max = fixed_slow + maximum_optional_slow
    return slow_min, slow_max


def _verdict(samples, slow_min, slow_max, slow_percent):
    if samples == 0:
        return "unknown"
    threshold_scaled = samples * slow_percent
    if slow_min * 100 > threshold_scaled:
        return "breach"
    if slow_max * 100 <= threshold_scaled:
        return "healthy"
    return "unknown"


def evaluate_window(
    window_id,
    operation,
    cutoff_us,
    slow_percent,
    slices,
    *,
    review_budget=0,
    review=None,
    incident_open=False,
    notify,
):
    """Evaluate one operation over paired per-process histogram snapshots.

    Input validation and feasibility checks complete before any callback.
    """
    window_id = _text(window_id, "window_id")
    operation = _text(operation, "operation")
    cutoff_us = _uint63(cutoff_us, "cutoff_us")

    if not _is_real_int(slow_percent) or not 0 <= slow_percent <= 100:
        raise ValueError("slow_percent must be an integer in [0, 100]")
    if not _is_real_int(review_budget) or not 0 <= review_budget <= _MAX_REVIEW_BUDGET:
        raise ValueError("review_budget must be an integer in [0, 8]")
    if type(incident_open) is not bool:
        raise ValueError("incident_open must be a boolean")
    if review is not None and not callable(review):
        raise ValueError("review must be callable or None")
    if not callable(notify):
        raise ValueError("notify must be callable")
    if not isinstance(slices, Mapping) or not 1 <= len(slices) <= _MAX_SLICES:
        raise ValueError("slices must be a mapping of 1..32 worker pairs")

    workers = []
    for worker_id, pair in slices.items():
        _text(worker_id, "worker_id")
        if (
            not isinstance(pair, Sequence)
            or isinstance(pair, (str, bytes, bytearray))
            or len(pair) != 2
        ):
            raise ValueError(f"slices[{worker_id!r}] must be a (before, after) pair")
        before_snapshot, after_snapshot = pair

        bounds, before_row = _validate_snapshot(before_snapshot, operation)
        after_bounds, after_row = _validate_snapshot(after_snapshot, operation)
        if bounds != after_bounds:
            raise ValueError(f"bucket bounds changed for worker {worker_id!r}")

        delta, raw_counts = _window_delta(before_row, after_row)
        _validate_window_feasible(bounds, delta, raw_counts)
        slow_min, slow_max = _slow_interval(
            bounds, raw_counts, delta["sum_us"], cutoff_us
        )
        workers.append({
            "id": worker_id,
            "samples": delta["count"],
            "slow_min": slow_min,
            "slow_max": slow_max,
            "width": slow_max - slow_min,
            "requested": False,
        })

    samples = sum(worker["samples"] for worker in workers)
    slow_min = sum(worker["slow_min"] for worker in workers)
    slow_max = sum(worker["slow_max"] for worker in workers)
    verdict = _verdict(samples, slow_min, slow_max, slow_percent)
    review_used = 0

    if verdict == "unknown" and samples > 0 and review is not None:
        # Width here is the histogram/sum-narrowed width immediately before a
        # review would collapse it. Unrequested rows never change afterward.
        queue = sorted(
            (worker for worker in workers if worker["width"] > 0),
            key=lambda worker: (-worker["width"], worker["id"]),
        )

        while verdict == "unknown" and review_used < review_budget:
            candidates = [worker for worker in queue if not worker["requested"]]
            if not candidates:
                break

            remaining_budget = review_budget - review_used
            batch_size = min(_REVIEW_BATCH, remaining_budget, len(candidates))
            batch_workers = candidates[:batch_size]
            batch_ids = tuple(worker["id"] for worker in batch_workers)
            for worker in batch_workers:
                worker["requested"] = True
            review_used += len(batch_ids)

            try:
                answer = review(window_id, operation, cutoff_us, batch_ids)
            except OSError:
                # Charged already; requested markers prevent resending.
                continue

            if not isinstance(answer, Mapping):
                continue

            for worker in batch_workers:
                worker_id = worker["id"]
                if worker_id not in answer:
                    continue
                value = answer[worker_id]
                if (
                    _is_real_int(value)
                    and worker["slow_min"] <= value <= worker["slow_max"]
                ):
                    worker["slow_min"] = value
                    worker["slow_max"] = value
                    worker["width"] = 0

            slow_min = sum(worker["slow_min"] for worker in workers)
            slow_max = sum(worker["slow_max"] for worker in workers)
            verdict = _verdict(samples, slow_min, slow_max, slow_percent)

    if verdict == "breach" and not incident_open:
        notify(window_id, operation, "page")
    elif verdict == "healthy" and incident_open:
        notify(window_id, operation, "resolve")

    return {
        "samples": samples,
        "slow_min": slow_min,
        "slow_max": slow_max,
        "verdict": verdict,
        "review_used": review_used,
    }
