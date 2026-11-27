"""Fixed-cardinality cumulative latency observations and optional exemplars."""
from bisect import bisect_left
from collections.abc import Mapping
from itertools import accumulate
from threading import Lock


_MAX_EVIDENCE = 4096
_MAX_CAPACITY = 64
_MAX_RULES = 32
_MAX_ACTIVE_READERS = 4


def valid_evidence(evidence):
    """Return True for None or non-empty bytes of at most 4096 bytes."""
    return (
        evidence is None
        or (isinstance(evidence, bytes) and 1 <= len(evidence) <= _MAX_EVIDENCE)
    )


def _real_int(value):
    # bool is a subclass of int, but configuration and durations reject it.
    return type(value) is int


class _Entry:
    __slots__ = ("seq", "operation", "duration_us")

    def __init__(self, seq, operation, duration_us):
        self.seq = seq
        self.operation = operation
        self.duration_us = duration_us


class ExemplarReader:
    """Serial-use cursor over an opening-time exemplar view."""

    def __init__(self, registry, items, registered):
        self._registry = registry
        # Each item is (slot, seq).  Operation/duration stay in the real slot
        # record and evidence bytes stay in the application store until next().
        self._items = items
        self._index = 0
        self._closed = False
        self._getting = False
        self._registered = registered

    def __iter__(self):
        return self

    def __next__(self):
        registry = self._registry
        with registry._lock:
            if self._closed or self._index >= len(self._items):
                raise StopIteration
            if self._getting:
                raise RuntimeError("an exemplar reader may only be advanced serially")
            slot, seq = self._items[self._index]
            current = registry._slots[slot]
            if current is None or current.seq != seq:
                # Refcounts make this unreachable for an unread item, but keeps
                # a faulty store/integration from pairing old metadata with new
                # bytes if invariants are violated elsewhere.
                raise RuntimeError("exemplar view changed before it was read")
            operation = current.operation
            duration_us = current.duration_us
            self._getting = True

        try:
            # Do not hold the collector lock while calling the application
            # store.  The slot's reference prevents eviction until this read
            # has finished.
            evidence = registry._exemplar_store.get(slot)
        except BaseException:
            with registry._lock:
                self._getting = False
                if self._closed:
                    # close() retained the in-flight reference for this call;
                    # the failed get ends the call, so release it here.
                    registry._refs[slot] -= 1
            raise

        with registry._lock:
            self._getting = False
            registry._refs[slot] -= 1
            self._index += 1
            if self._index >= len(self._items) and self._registered:
                registry._active_readers -= 1
                self._registered = False

        return operation, duration_us, evidence

    def close(self):
        """Release all still-unread protections; idempotent."""
        registry = self._registry
        with registry._lock:
            if self._closed:
                return
            self._closed = True
            start = self._index + (1 if self._getting else 0)
            for index in range(start, len(self._items)):
                slot = self._items[index][0]
                registry._refs[slot] -= 1
            if self._registered:
                registry._active_readers -= 1
                self._registered = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class Histograms:
    def __init__(self, operations, bounds_us, *, exemplar_rules=None,
                 exemplar_capacity=0, exemplar_store=None):
        operations, bounds = tuple(operations), tuple(bounds_us)
        if (not operations or any(not isinstance(key, str) or not key for key in operations)
                or len(set(operations)) != len(operations)):
            raise ValueError("operation names must be nonempty and unique")
        if (not bounds or any(not _real_int(n) or n < 0 for n in bounds)
                or any(a >= b for a, b in zip(bounds, bounds[1:]))):
            raise ValueError("bounds must be strictly increasing nonnegative integers")
        self._bounds = bounds
        self._rows = {key: ([0] * (len(bounds) + 1), 0) for key in sorted(operations)}
        self._lock = Lock()

        self._exemplar_enabled = False
        self._exemplar_rules = {}
        self._exemplar_capacity = 0
        self._exemplar_store = None
        self._slots = ()
        self._refs = []
        self._exemplar_seq = 0
        self._active_readers = 0

        defaults = (exemplar_rules is None
                    and _real_int(exemplar_capacity)
                    and exemplar_capacity == 0
                    and exemplar_store is None)
        if defaults:
            return

        self._configure_exemplars(exemplar_rules, exemplar_capacity, exemplar_store)

    def _configure_exemplars(self, rules, capacity, store):
        if not isinstance(rules, Mapping) or not 1 <= len(rules) <= _MAX_RULES:
            raise ValueError(
                "exemplar_rules must contain 1..32 operation rules to enable exemplars")
        if not _real_int(capacity) or not 1 <= capacity <= _MAX_CAPACITY:
            raise ValueError("exemplar_capacity must be an integer from 1 to 64")
        if store is None:
            raise ValueError("exemplar_store is required when exemplars are enabled")
        put = getattr(store, "put", None)
        get = getattr(store, "get", None)
        if not callable(put) or not callable(get):
            raise ValueError("exemplar_store must provide callable put(slot, evidence) and get(slot)")

        parsed = {}
        reserve_total = 0
        for operation, rule in rules.items():
            if operation not in self._rows:
                raise ValueError(f"exemplar rule references unregistered operation: {operation!r}")
            if not isinstance(rule, tuple) or len(rule) != 2:
                raise ValueError(f"exemplar rule for {operation!r} must be (threshold_us, reserve)")
            threshold_us, reserve = rule
            if not _real_int(threshold_us) or threshold_us < 0:
                raise ValueError(f"invalid exemplar threshold for {operation!r}")
            if not _real_int(reserve) or reserve < 1:
                raise ValueError(f"invalid exemplar reserve for {operation!r}")
            parsed[operation] = (threshold_us, reserve)
            reserve_total += reserve

        if reserve_total > capacity:
            raise ValueError("sum of exemplar reserves must not exceed exemplar_capacity")

        self._exemplar_enabled = True
        self._exemplar_rules = parsed
        self._exemplar_capacity = capacity
        self._exemplar_store = store
        self._slots = [None] * capacity
        self._refs = [0] * capacity

    def observe(self, operation, duration_us, *, evidence=None):
        if not valid_evidence(evidence):
            raise ValueError(
                "evidence must be None or bytes with length from 1 to 4096")
        if not isinstance(operation, str):
            raise ValueError("operation must be a string")
        if not _real_int(duration_us) or duration_us < 0:
            raise ValueError("duration_us must be a nonnegative integer")
        bucket = bisect_left(self._bounds, duration_us)
        with self._lock:
            bins, total = self._rows[operation]
            bins[bucket] += 1
            self._rows[operation] = bins, total + duration_us

            if self._exemplar_enabled and evidence is not None:
                rule = self._exemplar_rules.get(operation)
                if rule is not None and duration_us > rule[0]:
                    self._retain_locked(operation, duration_us, evidence, rule[1])

    def _choose_slot_locked(self, operation, reserve):
        # An empty slot is always used: reserves are count policies, not
        # preallocated partitions.
        for slot, entry in enumerate(self._slots):
            if entry is None:
                return slot

        own = []
        donor_ops = set()
        counts = {}
        for slot, entry in enumerate(self._slots):
            if entry is None:
                continue
            counts[entry.operation] = counts.get(entry.operation, 0) + 1
            if entry.operation == operation:
                own.append((entry.seq, slot))

        own_count = counts.get(operation, 0)
        if own_count < reserve:
            for other, count in counts.items():
                if other == operation:
                    continue
                other_reserve = self._exemplar_rules[other][1]
                if count > other_reserve:
                    donor_ops.add(other)
            candidates = [
                (entry.seq, slot)
                for slot, entry in enumerate(self._slots)
                if (entry is not None and self._refs[slot] == 0
                        and entry.operation in donor_ops)
            ]
        else:
            # Fixed full-write policy: replace this operation's earliest
            # unprotected sample.  If none is replaceable, drop the new scene.
            candidates = [
                (seq, slot) for seq, slot in own
                if self._refs[slot] == 0
            ]

        if not candidates:
            return None
        candidates.sort()
        return candidates[0][1]

    def _retain_locked(self, operation, duration_us, evidence, reserve):
        slot = self._choose_slot_locked(operation, reserve)
        if slot is None:
            return
        self._exemplar_seq += 1
        # The application store is fast and non-callback by contract.  Calling
        # put under the collector lock makes bytes+metadata publication one
        # linearization point; readers never touch a slot with a live reference.
        self._exemplar_store.put(slot, evidence)
        self._slots[slot] = _Entry(self._exemplar_seq, operation, duration_us)

    def open_exemplars(self):
        """Open a finite view of currently occupied exemplar slots."""
        if not self._exemplar_enabled:
            raise RuntimeError("exemplars are not enabled for this registry")

        with self._lock:
            items = [
                (slot, entry.seq)
                for slot, entry in enumerate(self._slots)
                if entry is not None
            ]
            items.sort(key=lambda item: item[1])
            reader = ExemplarReader(self, items, registered=bool(items))
            if not items:
                return reader
            if self._active_readers >= _MAX_ACTIVE_READERS:
                raise RuntimeError("at most four exemplar readers may have pending fragments")
            for slot, *_ in items:
                self._refs[slot] += 1
            self._active_readers += 1
            return reader

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
