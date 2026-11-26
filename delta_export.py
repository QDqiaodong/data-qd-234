"""Incremental histogram export with loss-free sink switching.

The exporter keeps one cumulative registry boundary and at most one
unacknowledged batch.  It never changes or clears the registry.  Sink switches
use the supplied idempotent ``send``/``fence`` callbacks to avoid counting a
batch on two sinks.

Control calls (``step`` and ``switch_to``) are serialized by their caller on one
thread, as documented.  Registry snapshots are short and never held across an
external callback.
"""

from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy

__all__ = ["DeltaExporter", "InProcessMeter"]

_ADVANCED = "advanced"
_PENDING = "pending"
_IDLE = "idle"

_MAX_OPERATIONS = 32
_MAX_BOUNDS = 64
_MAX_NAME = 128


def _name(value, what):
    if not isinstance(value, str) or not 1 <= len(value) <= _MAX_NAME:
        raise ValueError(f"{what} must be a string of 1..128 characters")
    return value


def _real_int(value):
    # bool is a subclass of int, but is not accepted as a histogram integer.
    return type(value) is int


def _validate_start_snapshot(snapshot):
    if not isinstance(snapshot, Mapping):
        raise ValueError("registry.snapshot() must return a mapping")

    bounds = snapshot.get("bounds_us")
    if (not isinstance(bounds, Sequence)
            or isinstance(bounds, (str, bytes, bytearray))
            or not 1 <= len(bounds) <= _MAX_BOUNDS):
        raise ValueError("registry must expose 1..64 bucket bounds")
    if any(not _real_int(value) or value < 0 for value in bounds):
        raise ValueError("bucket bounds must be nonnegative integers")
    if any(left >= right for left, right in zip(bounds, bounds[1:])):
        raise ValueError("bucket bounds must be strictly increasing")

    series = snapshot.get("series")
    if (not isinstance(series, Sequence)
            or isinstance(series, (str, bytes, bytearray))
            or not 1 <= len(series) <= _MAX_OPERATIONS):
        raise ValueError("registry must expose 1..32 operations")

    operations = []
    seen = set()
    for row in series:
        if not isinstance(row, Mapping):
            raise ValueError("histogram series rows must be mappings")
        operation = row.get("operation")
        if not isinstance(operation, str) or not operation:
            raise ValueError("histogram operation names must be nonempty strings")
        if operation in seen:
            raise ValueError("histogram operation names must be unique")
        seen.add(operation)
        operations.append(operation)

        le_counts = row.get("le_counts")
        if (not isinstance(le_counts, Sequence)
                or isinstance(le_counts, (str, bytes, bytearray))
                or len(le_counts) != len(bounds)
                or any(not _real_int(value) or value < 0 for value in le_counts)):
            raise ValueError("invalid le_counts in registry snapshot")

        count = row.get("count")
        sum_us = row.get("sum_us")
        if (not _real_int(count) or count < 0
                or not _real_int(sum_us) or sum_us < 0):
            raise ValueError("invalid count or sum_us in registry snapshot")

    return tuple(bounds), tuple(operations)


class DeltaExporter:
    """Deliver cumulative histogram completions as contiguous delta batches."""

    def __init__(self, registry, stream_id, sinks, initial_sink, *, send, fence):
        _name(stream_id, "stream_id")

        if isinstance(sinks, (str, bytes, bytearray)) or not isinstance(sinks, Iterable):
            raise ValueError("sinks must contain exactly two sink names")
        sink_names = tuple(sinks)
        if len(sink_names) != 2:
            raise ValueError("sinks must contain exactly two sink names")
        for sink in sink_names:
            _name(sink, "sink name")
        if len(set(sink_names)) != len(sink_names):
            raise ValueError("sink names must be distinct")

        if not isinstance(initial_sink, str) or initial_sink not in sink_names:
            raise ValueError("initial_sink must be one of sinks")
        if not callable(send) or not callable(fence):
            raise ValueError("send and fence must be callable")
        snapshot_method = getattr(registry, "snapshot", None)
        if not callable(snapshot_method):
            raise ValueError("registry must provide a snapshot() method")

        start = registry.snapshot()
        bounds, operations = _validate_start_snapshot(start)

        self._registry = registry
        self.stream_id = stream_id
        self.sinks = sink_names
        self._sink_names = frozenset(sink_names)
        self._send = send
        self._fence = fence

        self._bounds = bounds
        self._operations = operations
        self._baseline = start

        # Desired sink.  While _switch_open is true, target differs from the
        # sink at which the outstanding batch must first be resolved.
        self._current_sink = initial_sink
        self._target_sink = initial_sink
        self._switch_open = False

        self._next_seq = 1
        self._pending = None

    @property
    def target_sink(self):
        """The sink requested by the latest completed ``switch_to`` call."""
        return self._target_sink

    def switch_to(self, sink):
        """Change the desired sink without performing external I/O."""
        if not isinstance(sink, str) or sink not in self._sink_names:
            raise ValueError(f"sink is not configured: {sink!r}")

        if sink == self._target_sink:
            # Repeating the current target, including an in-progress switch,
            # is explicitly a no-op.
            return

        if self._switch_open:
            raise RuntimeError(
                f"switch to {self._target_sink!r} is still outstanding; "
                f"cannot switch to {sink!r}"
            )

        self._target_sink = sink
        if self._pending is None:
            self._current_sink = sink
        else:
            self._switch_open = True
            pending = self._pending
            if pending["phase"] == "sealed":
                # A prior send already supplied the definitive sealed answer.
                pending["sink"] = sink
                pending["phase"] = "send"
            else:
                # Fencing is the one-callback way to turn an ambiguous old-sink
                # result into a definitive applied/sealed answer.
                pending["phase"] = "fence"

    def step(self):
        """Advance delivery by at most one external callback."""
        pending = self._pending
        if pending is None:
            current = self._registry.snapshot()
            delta = self._build_delta(current)
            if delta is None:
                return _IDLE

            seq = self._next_seq
            self._next_seq += 1
            pending = {
                "seq": seq,
                "delta": delta,
                "boundary": current,
                "sink": self._current_sink,
                "phase": "send",
            }
            self._pending = pending
            return self._send_batch(pending["sink"], seq, delta)

        if pending["phase"] == "fence":
            return self._fence_batch(pending)
        if pending["phase"] == "sealed":
            # This destination gave a definitive sealed answer before a switch
            # target was supplied.  There is no safe callback until switch_to().
            return _PENDING
        return self._send_batch(pending["sink"], pending["seq"], pending["delta"])

    def _build_delta(self, current):
        _validate_start_snapshot(current)
        if tuple(current["bounds_us"]) != self._bounds:
            raise ValueError("registry bucket bounds changed")

        base_rows = {row["operation"]: row for row in self._baseline["series"]}
        current_rows = {row["operation"]: row for row in current["series"]}
        if tuple(current_rows) != self._operations:
            raise ValueError("registry operation set or order changed")

        series = []
        total_count = 0
        for operation in self._operations:
            base = base_rows[operation]
            row = current_rows[operation]

            le_counts = []
            for old, new in zip(base["le_counts"], row["le_counts"]):
                if new < old:
                    raise ValueError("cumulative le_counts regressed")
                le_counts.append(new - old)

            count = row["count"] - base["count"]
            sum_us = row["sum_us"] - base["sum_us"]
            if count < 0 or sum_us < 0:
                raise ValueError("cumulative histogram counters regressed")

            total_count += count
            series.append({
                "operation": operation,
                "le_counts": le_counts,
                "count": count,
                "sum_us": sum_us,
            })

        if total_count == 0:
            return None
        return {"bounds_us": list(self._bounds), "series": series}

    def _send_batch(self, sink, seq, delta):
        try:
            answer = self._send(sink, self.stream_id, seq, delta)
        except TimeoutError:
            # A timeout is not evidence that the send failed or succeeded.
            return _PENDING

        if answer == "applied":
            self._acknowledge_pending()
            return _ADVANCED

        if answer == "sealed":
            return self._handle_sealed_send(sink)

        raise RuntimeError(f"send callback returned invalid result: {answer!r}")

    def _fence_batch(self, pending):
        sink = pending["sink"]
        seq = pending["seq"]
        try:
            answer = self._fence(sink, self.stream_id, seq)
        except TimeoutError:
            return _PENDING

        if answer == "applied":
            self._acknowledge_pending()
            return _ADVANCED

        if answer == "sealed":
            if not self._switch_open:
                raise RuntimeError("pending batch was sealed by its current sink")
            # The old sink is now guaranteed never to count this key.  Keep the
            # same seq and immutable delta on the requested new sink.  Do not
            # send there in this step: this step already made one callback.
            pending["sink"] = self._target_sink
            pending["phase"] = "send"
            return _PENDING

        raise RuntimeError(f"fence callback returned invalid result: {answer!r}")

    def _handle_sealed_send(self, sink):
        pending = self._pending
        if pending is None:
            raise RuntimeError("received sealed result without a pending batch")

        if self._switch_open and sink != self._target_sink:
            # This can be the definitive answer to an in-flight old-sink send.
            pending["sink"] = self._target_sink
            pending["phase"] = "send"
            return _PENDING

        if not self._switch_open and sink == self._target_sink:
            # The current destination is definitively unable to count this key.
            # Wait for the caller to choose the other configured sink.
            pending["phase"] = "sealed"
            return _PENDING

        raise RuntimeError("pending batch was sealed by its destination sink")

    def _acknowledge_pending(self):
        pending = self._pending
        # The acknowledged boundary is the snapshot captured when this batch
        # was created.  Observations made by the send callback are after it and
        # therefore remain in the next delta.
        self._baseline = pending["boundary"]
        self._pending = None
        self._current_sink = self._target_sink
        self._switch_open = False


class InProcessMeter:
    """Reference process-local meter implementing the sink callback contract."""

    def __init__(self, sinks):
        names = tuple(sinks)
        if len(names) != 2 or len(set(names)) != len(names):
            raise ValueError("meter requires two distinct sink names")
        self.sinks = names
        self.records = {sink: {} for sink in names}
        self.calls = []

    def send(self, sink, stream_id, seq, delta):
        self.calls.append({
            "method": "send",
            "sink": sink,
            "stream_id": stream_id,
            "seq": seq,
            "delta": deepcopy(delta),
        })
        key = (stream_id, seq)
        store = self.records[sink]
        record = store.get(key)
        if record is not None:
            if record["state"] == "applied" and record["delta"] != delta:
                raise AssertionError("same key was resent with different content")
            return record["state"]

        store[key] = {"state": "applied", "delta": deepcopy(delta)}
        return "applied"

    def fence(self, sink, stream_id, seq):
        self.calls.append({
            "method": "fence",
            "sink": sink,
            "stream_id": stream_id,
            "seq": seq,
            "delta": None,
        })
        key = (stream_id, seq)
        store = self.records[sink]
        record = store.get(key)
        if record is not None:
            return record["state"]

        record = {"state": "sealed", "delta": None}
        store[key] = record
        return "sealed"

    def call_tuples(self, sink=None):
        return [
            (call["method"], call["sink"], call["seq"])
            for call in self.calls
            if sink is None or call["sink"] == sink
        ]

    def applied_keys(self, sink):
        return {
            key for key, record in self.records[sink].items()
            if record["state"] == "applied"
        }

    def totals(self, sink):
        totals = {}
        for record in self.records[sink].values():
            if record["state"] != "applied":
                continue
            for row in record["delta"]["series"]:
                item = totals.setdefault(row["operation"], {"count": 0, "sum_us": 0})
                item["count"] += row["count"]
                item["sum_us"] += row["sum_us"]
        return totals
