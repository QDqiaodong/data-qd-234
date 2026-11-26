"""Fixed-policy, process-local admission backed by completed work latency."""
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock, get_ident
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
    promised: int = 0


class Busy(RuntimeError):
    """The operation has no slot; no work was run."""


_OPEN = 0
_USED = 1
_CLOSED = 2


class _FlowTicket:
    """A held first-stage slot plus a single chance at the continuation slot."""

    def __init__(self, gate, first, next_op, owner):
        self._gate = gate
        self._first = first
        self._next = next_op
        self._owner = owner
        self._state = _OPEN

    @contextmanager
    def work(self):
        """Try occupying the continuation slot once; Busy does not spend the chance."""
        gate = self._gate
        if get_ident() != self._owner:
            raise RuntimeError("flow ticket can only be used by its owning thread")
        with gate._lock:
            if self._state == _CLOSED:
                raise RuntimeError("flow ticket has already left its first stage")
            if self._state == _USED:
                raise RuntimeError("flow ticket continuation can only be entered once")
            state = gate._operations[self._next]
            # Accepted tickets only face the hard actual-occupancy cap: a later
            # limit reduction is never re-reviewed against an outstanding promise.
            if state.active >= state.policy.normal_limit:
                raise Busy(self._next)
            state.promised -= 1
            state.active += 1
            self._state = _USED
        elapsed = None
        try:
            start = gate._clock_ns()
            try:
                yield
            finally:
                elapsed = (gate._clock_ns() - start) // 1000
        finally:
            with gate._lock:
                try:
                    if elapsed is not None:
                        gate._registry.observe(self._next, elapsed)
                finally:
                    gate._operations[self._next].active -= 1


class Admission:
    def __init__(self, registry, policies, *, clock_ns=monotonic_ns, staged=False):
        plans = dict(policies)
        if not plans:
            raise ValueError("at least one operation policy is required")
        if not callable(clock_ns):
            raise ValueError("clock_ns must be callable")
        self._staged = bool(staged)
        if self._staged:
            if not 1 <= len(plans) <= 32:
                raise ValueError("staged admission allows 1 to 32 configured operations")
            if any(policy.normal_limit > 32 for policy in plans.values()):
                raise ValueError("staged admission requires every normal_limit <= 32")
        self._registry = registry
        self._clock_ns = clock_ns
        self._lock = Lock()
        self._tickets = set() if self._staged else None
        self._operations = {}
        initial = registry.snapshot()
        rows = {row["operation"]: row for row in initial["series"]}
        for name, policy in plans.items():
            if name not in rows or not isinstance(policy, Policy):
                raise ValueError(
                    f"invalid policy or unregistered operation: {name!r}")
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
        if not self._staged:
            with self._lock:
                state = self._operations[operation]
                if state.active >= state.limit:
                    raise Busy(operation)
                state.active += 1
            with self._run(operation):
                yield
            return
        with self._lock:
            state = self._operations[operation]
            cap = state.policy.normal_limit
            if state.active + 1 > cap:
                raise Busy(operation)
            if state.limit < cap and state.active + 1 + state.promised > state.limit:
                raise Busy(operation)
            if not self._immediate_path_survives(operation, state):
                raise Busy(operation)
            state.active += 1
        with self._run(operation):
            yield

    @contextmanager
    def _run(self, operation):
        state = self._operations[operation]
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

    @contextmanager
    def flow(self, first, next_op):
        """Declare a two-stage request before starting the first stage.

        The first stage immediately occupies one ``first`` slot and records one
        sample for the whole outer hold. The continuation is declared now (a
        promise on ``next_op``) and cashed at most once via ``ticket.work()``.
        """
        if not self._staged:
            raise RuntimeError("flow() requires Admission(..., staged=True)")
        owner = get_ident()
        with self._lock:
            first_state = self._operations[first]      # KeyError for an unknown operation
            next_state = self._operations[next_op]     # KeyError for an unknown operation
            if first == next_op:
                raise ValueError("flow stages must use different operations")
            self._admit_flow(first, next_op, first_state, next_state)
            first_state.active += 1
            next_state.promised += 1
            ticket = _FlowTicket(self, first, next_op, owner)
            self._tickets.add(ticket)
        elapsed = None
        try:
            start = self._clock_ns()
            try:
                yield ticket
            finally:
                elapsed = (self._clock_ns() - start) // 1000
        finally:
            with self._lock:
                try:
                    if elapsed is not None:
                        self._registry.observe(first, elapsed)
                finally:
                    first_state.active -= 1
                    if ticket._state == _OPEN:
                        # The continuation chance was never cashed: cancel the promise.
                        next_state.promised -= 1
                    ticket._state = _CLOSED
                    self._tickets.discard(ticket)

    def _admit_flow(self, first, next_op, first_state, next_state):
        cap_first = first_state.policy.normal_limit
        # The normal limit doubles as the unbreakable actual-occupancy cap.
        if first_state.active + 1 > cap_first:
            raise Busy(first)
        # Rule 1: protection tier bounds the committed amount, at both ends.
        if first_state.limit < cap_first:
            if first_state.active + 1 + first_state.promised > first_state.limit:
                raise Busy(first)
        cap_next = next_state.policy.normal_limit
        if next_state.limit < cap_next:
            if next_state.active + next_state.promised + 1 > next_state.limit:
                raise Busy(next_op)
        # Rule 2a: an immediately cashable old ticket must remain cashable.
        if not self._immediate_path_survives(first, first_state):
            raise Busy(first)
        # Rule 2b: the whole accepted set must stay completable.
        if not self._completable(first, next_op):
            raise Busy(first)

    def _immediate_path_survives(self, occupied_op, occupied_state):
        """True unless this entry fills the last actual slot every ready ticket needs.

        A ready ticket targets an operation with a free actual slot. Tickets whose
        target is not the operation being entered are unaffected, so they neither
        clear nor condemn the entry: only tickets targeting ``occupied_op`` count.
        """
        cap = occupied_state.policy.normal_limit
        affected_ready = False
        for ticket in self._tickets:
            if ticket._state != _OPEN or ticket._next != occupied_op:
                continue
            target = self._operations[ticket._next]
            if target.active >= target.policy.normal_limit:
                continue
            affected_ready = True
            # This ticket stays cashable only if a slot remains after the entry.
            if occupied_state.active + 1 < cap:
                return True
        # With no affected ready ticket, rule 2a has nothing to preserve here.
        return not affected_ready

    def _completable(self, candidate_first, candidate_next):
        """Greedy feasibility of cashing every open ticket plus the candidate.

        Each ticket is an edge first->next. Cashing an edge needs one free actual
        slot at its head and, on completion, frees one slot at each endpoint; the
        head occupancy nets to zero. Cashing only ever lowers occupancy, so any
        currently cashable edge may be served without destroying feasibility.
        Work that needs no continuation (plain work, used tickets) is assumed to
        have already exited, so only open tickets contribute base occupancy.
        """
        active = {name: 0 for name in self._operations}
        caps = {name: state.policy.normal_limit
                for name, state in self._operations.items()}
        edges = []
        for ticket in self._tickets:
            if ticket._state == _OPEN:
                edges.append((ticket._first, ticket._next))
                active[ticket._first] += 1
        if active[candidate_first] + 1 > caps[candidate_first]:
            return False
        edges.append((candidate_first, candidate_next))
        active[candidate_first] += 1
        while edges:
            for index, (head_first, head_next) in enumerate(edges):
                if active[head_next] < caps[head_next]:
                    active[head_first] -= 1
                    del edges[index]
                    break
            else:
                return False
        return True
