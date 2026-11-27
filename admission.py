"""Fixed-policy, process-local admission backed by completed work latency."""
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock, get_ident
from time import monotonic_ns

from metrics import valid_evidence


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

    def work(self, operation, *, evidence=None):
        """Validate evidence eagerly and return an admission context manager."""
        if not valid_evidence(evidence):
            raise ValueError(
                "evidence must be None or bytes with length from 1 to 4096")
        return self._work_context(operation, evidence)

    @contextmanager
    def _work_context(self, operation, evidence):
        """Try admitting work immediately, and observe its duration on exit."""
        if not self._staged:
            with self._lock:
                state = self._operations[operation]
                if state.active >= state.limit:
                    raise Busy(operation)
                state.active += 1
            with self._run(operation, evidence):
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
        with self._run(operation, evidence):
            yield

    @contextmanager
    def _run(self, operation, evidence=None):
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
                        if evidence is None:
                            self._registry.observe(operation, elapsed)
                        else:
                            self._registry.observe(
                                operation, elapsed, evidence=evidence)
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


class _QueuedRequest:
    """One accepted DeadlineQueue submission, tracked until it settles."""

    __slots__ = ("name", "operation", "duration", "deadline", "seq",
                 "state", "est_end", "cm")

    def __init__(self, name, operation, duration, deadline, seq):
        self.name = name
        self.operation = operation
        self.duration = duration
        self.deadline = deadline
        self.seq = seq
        self.state = "waiting"
        self.est_end = None
        self.cm = None


class DeadlineQueue:
    """In-process scheduling front door over one non-staged Admission gate.

    submit() only accepts work for later dispatch; advance() is the only
    source of start decisions and never starts the same request twice;
    finish() is the only confirmation that physical work ended, settling the
    admission slot and the latency sample on the first call.  Interactive
    requests carry a deadline and are accepted only while every not yet
    started interactive commitment stays schedulable (finishing exactly at
    the deadline counts as on time).  Batch requests have no deadline and
    start whenever idle capacity cannot break a valid commitment.  Estimated
    ends from durations and extend() only plan future feasibility; dispatch
    always follows the actually unfinished running set, so a request whose
    estimate expired keeps its slot until finish() confirms it.
    """

    _WINDOW = 64
    _MAX_WAITING = 12
    _MAX_WAITING_INTERACTIVE = 6

    def __init__(self, admission, *, slots):
        if not isinstance(admission, Admission) or admission._staged:
            raise ValueError("a non-staged Admission instance is required")
        if type(slots) is not int or slots not in (1, 2):
            raise ValueError("slots must be 1 or 2")
        if not 1 <= len(admission._operations) <= 2:
            raise ValueError("DeadlineQueue supports one or two operations")
        self._admission = admission
        self._slots = slots
        self._limits = {name: state.policy.normal_limit
                        for name, state in admission._operations.items()}
        self._requests = {}
        self._waiting = []          # accepted, not yet started, submission order
        self._running = {}          # dispatched, not yet confirmed finished
        self._seq = 0
        self._tick = 0

    def submit(self, name, operation, now, duration, *, deadline=None):
        """Accept a request for later dispatch; never starts work itself.

        A request that is not accepted consumes no name, capacity, or sample.
        """
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a nonempty string")
        if name in self._requests:
            raise ValueError(f"duplicate request name: {name!r}")
        if not isinstance(operation, str) or operation not in self._limits:
            raise ValueError(f"unknown operation: {operation!r}")
        self._check_now(now)
        if type(duration) is not int or duration < 1:
            raise ValueError("duration must be a positive integer")
        if deadline is not None and type(deadline) is not int:
            raise ValueError("deadline must be None or an integer tick")
        self._absorb(now)
        if len(self._waiting) >= self._MAX_WAITING:
            return False
        request = _QueuedRequest(name, operation, duration, deadline, self._seq)
        if deadline is not None:
            committed = [req for req in self._waiting if req.deadline is not None]
            if len(committed) >= self._MAX_WAITING_INTERACTIVE:
                return False
            if not self._feasible(now, committed + [request]):
                return False
        self._requests[name] = request
        self._waiting.append(request)
        self._seq += 1
        return True

    def advance(self, now):
        """Start what this tick must and may start; return the started names."""
        self._check_now(now)
        self._absorb(now)
        started = []
        while True:
            dispatched = self._start_due_interactive(now, started)
            dispatched = self._start_safe_batch(now, started) or dispatched
            if not dispatched:
                return tuple(started)

    def extend(self, name, new_end, now):
        """Postpone a running request's estimated end; never dispatches."""
        self._check_now(now)
        request = self._requests.get(name)
        if request is None:
            raise ValueError(f"unknown request: {name!r}")
        if request.state != "running":
            raise ValueError(f"only a running request can be extended: {name!r}")
        if (type(new_end) is not int or new_end <= request.est_end
                or new_end < now):
            raise ValueError("new_end must move the estimated end further out")
        self._absorb(now)
        request.est_end = new_end
        self._reevaluate(now)

    def finish(self, name, now):
        """Confirm physical completion; only the first confirmation settles."""
        self._check_now(now)
        request = self._requests.get(name)
        if request is None:
            raise ValueError(f"unknown request: {name!r}")
        if request.state in ("waiting", "missed"):
            raise ValueError(f"request is not running: {name!r}")
        self._absorb(now)
        if request.state != "running":
            return False
        context = request.cm
        request.cm = None
        context.__exit__(None, None, None)
        del self._running[name]
        request.state = ("late"
                         if request.deadline is not None and now > request.deadline
                         else "done")
        return True

    def status(self, name):
        """One of waiting, running, missed, done, late."""
        request = self._requests.get(name)
        if request is None:
            raise ValueError(f"unknown request: {name!r}")
        return request.state

    def _check_now(self, now):
        if type(now) is not int or now < 0:
            raise ValueError("now must be a nonnegative integer tick")
        if now < self._tick:
            raise ValueError("ticks must be monotonically nondecreasing")

    def _absorb(self, now):
        if now > self._tick:
            self._tick = now
            self._reevaluate(now)

    def _reevaluate(self, now):
        """Drop waiting commitments that can no longer be honored.

        Older promises win by deadline, then submission order; a request that
        is individually impossible never blocks deliverable ones.
        """
        committed = sorted(
            (req for req in self._waiting if req.deadline is not None),
            key=lambda req: (req.deadline, req.seq))
        kept = []
        for request in committed:
            if self._feasible(now, kept + [request]):
                kept.append(request)
            else:
                request.state = "missed"
        if len(kept) != len(committed):
            self._waiting = [req for req in self._waiting if req.state == "waiting"]

    def _start_due_interactive(self, now, started):
        """Start interactive work that may not survive idling another tick.

        Idling is allowed while every commitment can still be honored with
        all starts pushed to the next tick; otherwise the earliest candidate
        by (deadline, submission order) with a complete feasible continuation
        starts now, until idling is safe again or capacity runs out.
        """
        dispatched = False
        while True:
            committed = self._committed()
            if not committed:
                return dispatched
            releases = {req.name: now + 1 for req in committed}
            if self._feasible(now, committed, releases=releases):
                return dispatched
            for request in sorted(committed,
                                  key=lambda req: (req.deadline, req.seq)):
                if not self._has_capacity(request):
                    continue
                rest = [other for other in committed if other is not request]
                extra = ((request.operation, now + request.duration),)
                if self._feasible(now, rest, extra_running=extra):
                    self._dispatch(request, now)
                    started.append(request.name)
                    dispatched = True
                    break
            else:
                return dispatched

    def _start_safe_batch(self, now, started):
        """Start waiting batch requests, oldest first, while commitments hold."""
        dispatched = False
        for request in list(self._waiting):
            if request.deadline is not None or request.state != "waiting":
                continue
            if not self._has_capacity(request):
                continue
            extra = ((request.operation, now + request.duration),)
            if self._feasible(now, self._committed(), extra_running=extra):
                self._dispatch(request, now)
                started.append(request.name)
                dispatched = True
        return dispatched

    def _committed(self):
        return [req for req in self._waiting if req.deadline is not None]

    def _has_capacity(self, request):
        if len(self._running) >= self._slots:
            return False
        active = sum(1 for other in self._running.values()
                     if other.operation == request.operation)
        return active < self._limits[request.operation]

    def _dispatch(self, request, now):
        context = self._admission.work(request.operation)
        context.__enter__()
        request.cm = context
        request.state = "running"
        request.est_end = now + request.duration
        self._running[request.name] = request
        self._waiting.remove(request)

    def _feasible(self, now, jobs, extra_running=(), releases=None):
        """True if every job can still finish by its deadline.

        Running requests occupy their slots until their estimated end;
        ``extra_running`` models a tentative start the same way.  Planning
        looks at most _WINDOW ticks ahead, which covers every commitment by
        contract.
        """
        limit = now + self._WINDOW
        total = [0] * self._WINDOW
        per_op = {operation: [0] * self._WINDOW for operation in self._limits}
        occupants = [(req.operation, req.est_end) for req in self._running.values()]
        occupants.extend(extra_running)
        for operation, end in occupants:
            end = min(max(end, now), limit)
            for tick in range(now, end):
                total[tick - now] += 1
                per_op[operation][tick - now] += 1
        planned = []
        for request in jobs:
            earliest = now if releases is None else releases.get(request.name, now)
            planned.append((min(request.deadline, limit), request.seq,
                            request.operation, request.duration, earliest))
        planned.sort()
        slots = self._slots
        limits = self._limits
        operations = tuple(per_op)
        memo = set()

        def visit(index):
            if index == len(planned):
                return True
            deadline, _, operation, duration, earliest = planned[index]
            signature = (index, tuple(total)) + tuple(
                tuple(per_op[name]) for name in operations)
            if signature in memo:
                return False
            per = per_op[operation]
            cap = limits[operation]
            latest = deadline - duration
            tick = max(now, earliest)
            while tick <= latest:
                base = tick - now
                offset = 0
                while (offset < duration and total[base + offset] < slots
                       and per[base + offset] < cap):
                    offset += 1
                if offset == duration:
                    for step in range(duration):
                        total[base + step] += 1
                        per[base + step] += 1
                    if visit(index + 1):
                        return True
                    for step in range(duration):
                        total[base + step] -= 1
                        per[base + step] -= 1
                tick += 1
            memo.add(signature)
            return False

        return visit(0)
