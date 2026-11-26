"""Fixed-policy, process-local admission backed by completed work latency."""
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock
from time import monotonic_ns


@dataclass(frozen=True)
class Policy:
    normal_limit: int
    protected_limit: int
    budget_us: int
    min_samples: int
    max_slow_percent: int

    def __post_init__(self):
        fields = (self.normal_limit, self.protected_limit, self.budget_us,
                  self.min_samples, self.max_slow_percent)
        if (any(type(value) is not int for value in fields)
                or not 1 <= self.protected_limit < self.normal_limit
                or self.budget_us < 0 or self.min_samples < 1
                or not 0 <= self.max_slow_percent < 100):
            raise ValueError("invalid admission policy")


@dataclass(frozen=True)
class Decision:
    samples: int
    slow: int
    limit: int
    applied: bool


@dataclass
class _Operation:
    policy: Policy
    bucket: int
    seen: tuple
    limit: int
    active: int = 0


class Busy(RuntimeError):
    """The operation has no slot; no work was run."""


class Admission:
    def __init__(self, registry, policies, *, clock_ns=monotonic_ns):
        plans = dict(policies)
        if not plans:
            raise ValueError("at least one operation policy is required")
        if not callable(clock_ns):
            raise ValueError("clock_ns must be callable")
        self._registry = registry
        self._clock_ns = clock_ns
        self._lock = Lock()
        self._operations = {}
        initial = registry.snapshot()
        rows = {row["operation"]: row for row in initial["series"]}
        for name, policy in plans.items():
            if name not in rows or not isinstance(policy, Policy):
                raise ValueError(f"invalid policy or unregistered operation: {name!r}")
            try:
                bucket = initial["bounds_us"].index(policy.budget_us)
            except ValueError as exc:
                raise ValueError(f"{name!r}: budget_us must equal a histogram bucket bound") from exc
            self._operations[name] = _Operation(
                policy, bucket, self._counts(rows[name], bucket), policy.normal_limit)

    @staticmethod
    def _counts(row, bucket):
        return row["count"], row["le_counts"][bucket]

    def refresh(self):
        """Apply decisions from new observations, keeping incomplete windows open."""
        with self._lock:
            rows = {row["operation"]: row for row in self._registry.snapshot()["series"]}
            decisions = {}
            for name, state in self._operations.items():
                counts = self._counts(rows[name], state.bucket)
                samples = counts[0] - state.seen[0]
                slow = samples - (counts[1] - state.seen[1])
                policy = state.policy
                applied = samples >= policy.min_samples
                if applied:
                    state.limit = (policy.protected_limit
                                   if slow * 100 > samples * policy.max_slow_percent
                                   else policy.normal_limit)
                    state.seen = counts
                decisions[name] = Decision(samples, slow, state.limit, applied)
            return decisions

    @contextmanager
    def work(self, operation):
        """Try admitting work immediately, and observe its duration on exit."""
        with self._lock:
            state = self._operations[operation]
            if state.active >= state.limit:
                raise Busy(operation)
            state.active += 1
        elapsed = None
        try:
            start = self._clock_ns()
            try:
                yield
            finally:
                elapsed = (self._clock_ns() - start) // 1000
        finally:
            with self._lock:
                try:
                    # Completion and slot release are one boundary for refresh/admission.
                    if elapsed is not None:
                        self._registry.observe(operation, elapsed)
                finally:
                    state.active -= 1
