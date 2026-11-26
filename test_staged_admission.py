"""Deterministic regression tests for two-stage (staged) admission."""
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
import json
from threading import Event, Lock
from urllib.parse import urlsplit
from unittest import TestCase

from admission import Admission, Busy, Policy
from demo import local_http
from endpoint import scrape_app
from metrics import Histograms


class FakeClock:
    """Single-threaded deterministic clock; concurrency tests never advance it."""

    def __init__(self):
        self.ns = 0

    def __call__(self):
        return self.ns

    def advance(self, us):
        self.ns += us * 1000


def policy(normal=2, protected=1, min_samples=2):
    return Policy(normal, protected, 50, min_samples, 50)


def rows(registry):
    return {row["operation"]: row for row in registry.snapshot()["series"]}


class StagedConfigTest(TestCase):
    def gate(self, operations, **kwargs):
        registry = Histograms(operations, [10, 50])
        return registry, Admission(registry, {name: policy() for name in operations},
                                   staged=True, **kwargs)

    def test_staged_configuration_bounds(self):
        with self.assertRaises(ValueError):
            self.gate([])
        with self.assertRaises(ValueError):
            self.gate([f"op{i}" for i in range(33)])
        registry = Histograms(["a", "b"], [10, 50])
        with self.assertRaises(ValueError):
            Admission(registry, {"a": policy(), "b": Policy(33, 1, 50, 2, 50)}, staged=True)

    def test_flow_unavailable_without_staged(self):
        registry = Histograms(["a", "b"], [10, 50])
        gate = Admission(registry, {"a": policy(), "b": policy()})
        with self.assertRaises(RuntimeError):
            with gate.flow("a", "b"):
                self.fail("flow must stay disabled")

    def test_flow_argument_validation(self):
        _, gate = self.gate(["a", "b"])
        with self.assertRaises(ValueError):
            with gate.flow("a", "a"):
                self.fail("same-stage flow must be rejected")
        with self.assertRaises(KeyError):
            with gate.flow("a", "missing"):
                self.fail("unknown next op must raise")
        with self.assertRaises(KeyError):
            with gate.flow("missing", "a"):
                self.fail("unknown first op must raise")


class StagedAccountingTest(TestCase):
    def setUp(self):
        self.registry = Histograms(["a", "b", "c"], [10, 50])
        self.clock = FakeClock()
        plans = {name: policy(normal=2, protected=1) for name in ("a", "b", "c")}
        self.gate = Admission(self.registry, plans, staged=True, clock_ns=self.clock)

    def state(self, name):
        return self.gate._operations[name]

    def test_skip_continuation_cancels_promise_and_releases_slots(self):
        with self.gate.flow("a", "b"):
            self.assertEqual(self.state("a").active, 1)
            self.assertEqual(self.state("b").promised, 1)
            self.assertEqual(rows(self.registry)["a"]["count"], 0)
        self.assertEqual((self.state("a").active, self.state("a").promised), (0, 0))
        self.assertEqual((self.state("b").active, self.state("b").promised), (0, 0))
        # First-stage sample is recorded; no continuation sample exists.
        self.assertEqual(rows(self.registry)["a"]["count"], 1)
        self.assertEqual(rows(self.registry)["b"]["count"], 0)

    def test_continuation_busy_does_not_spend_the_chance_or_sample(self):
        # Fill both target slots: one filler first stage whose promise points at
        # c, plus plain work. No promise competes with the a->b continuation.
        filler = self.gate.flow("b", "c")
        filler.__enter__()
        plain = self.gate.work("b")
        plain.__enter__()
        ticket_cm = self.gate.flow("a", "b")
        ticket = ticket_cm.__enter__()
        try:
            with self.assertRaises(Busy):
                with ticket.work():
                    self.fail("denied continuation ran")
            # Still open: retry later works, and the promise is still held.
            self.assertEqual(self.state("b").promised, 1)
            self.assertEqual(rows(self.registry)["b"]["count"], 0)
            # The plain work exits, freeing one target actual slot.
            plain.__exit__(None, None, None)
            with ticket.work():
                self.clock.advance(7)
            self.assertEqual(self.state("b").promised, 0)
        finally:
            ticket_cm.__exit__(None, None, None)
            filler.__exit__(None, None, None)
        data = rows(self.registry)
        self.assertEqual(data["a"]["count"], 1)
        # Filler b first stage (7us), plain b work (0us), cashed continuation (7us).
        self.assertEqual(data["b"]["count"], 3)
        self.assertEqual(data["b"]["sum_us"], 14)

    def test_continuation_busy_at_full_hard_cap_can_retry(self):
        # Same full-cap setup; once the plain work leaves, the held ticket retries.
        filler = self.gate.flow("b", "c")
        filler.__enter__()
        plain = self.gate.work("b")
        plain.__enter__()
        ticket_cm = self.gate.flow("a", "b")
        t2 = ticket_cm.__enter__()
        try:
            with self.assertRaises(Busy):
                with t2.work():
                    self.fail("hard cap must block the continuation")
            self.assertEqual(t2._state, 0)
            plain.__exit__(None, None, None)
            with t2.work():
                self.clock.advance(9)
        finally:
            ticket_cm.__exit__(None, None, None)
            filler.__exit__(None, None, None)
        data = rows(self.registry)
        self.assertEqual(data["a"]["count"], 1)
        # Filler b first stage (9us), plain b work (0us), retried continuation (9us).
        self.assertEqual(data["b"]["count"], 3)
        self.assertEqual(data["b"]["sum_us"], 18)

    def test_continuation_may_only_be_used_once(self):
        with self.gate.flow("a", "b") as ticket:
            with ticket.work():
                pass
            with self.assertRaises(RuntimeError):
                with ticket.work():
                    self.fail("second continuation must be rejected")
            with self.assertRaises(RuntimeError):
                ticket.work().__enter__()
        with self.assertRaises(RuntimeError):
            with ticket.work():
                self.fail("closed ticket must be rejected")
        data = rows(self.registry)
        self.assertEqual(data["a"]["count"], 1)
        self.assertEqual(data["b"]["count"], 1)

    def test_first_stage_exception_still_samples_and_releases(self):
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with self.gate.flow("a", "b"):
                self.clock.advance(51)
                raise RuntimeError("boom")
        self.assertEqual((self.state("a").active, self.state("b").promised), (0, 0))
        data = rows(self.registry)
        self.assertEqual(data["a"]["count"], 1)
        self.assertEqual(data["a"]["sum_us"], 51)
        self.assertEqual(data["b"]["count"], 0)

    def test_continuation_exception_is_observed_and_not_swallowed(self):
        with self.assertRaisesRegex(RuntimeError, "inner boom"):
            with self.gate.flow("a", "b") as ticket:
                with ticket.work():
                    self.clock.advance(3)
                    raise RuntimeError("inner boom")
        data = rows(self.registry)
        self.assertEqual(data["b"]["count"], 1)
        self.assertEqual(data["b"]["sum_us"], 3)
        self.assertEqual(data["a"]["count"], 1)
        self.assertEqual((self.state("a").active, self.state("b").active), (0, 0))

    def test_first_sample_covers_continuation_time(self):
        with self.gate.flow("a", "b") as ticket:
            self.clock.advance(5)
            with ticket.work():
                self.clock.advance(10)
            self.clock.advance(2)
        data = rows(self.registry)
        self.assertEqual(data["a"]["sum_us"], 17)
        self.assertEqual(data["b"]["sum_us"], 10)

    def test_busy_flow_creates_no_state_or_samples(self):
        with self.gate.flow("a", "b"), self.gate.flow("a", "b"):
            before = rows(self.registry)
            with self.assertRaises(Busy):
                with self.gate.flow("a", "c"):
                    self.fail("hard cap admission must not run the body")
            self.assertEqual(rows(self.registry), before)
            self.assertEqual((self.state("a").active, self.state("a").promised), (2, 0))
            self.assertEqual((self.state("c").active, self.state("c").promised), (0, 0))

    def test_ticket_is_thread_affine(self):
        with self.gate.flow("a", "b") as ticket:
            with ThreadPoolExecutor(max_workers=1) as pool:
                with self.assertRaises(RuntimeError):
                    pool.submit(self._use, ticket).result(2)
            # Owner may still use it.
            with ticket.work():
                pass

    @staticmethod
    def _use(ticket):
        with ticket.work():
            pass

    def test_overlapping_scopes_may_exit_in_any_order(self):
        t1 = self.gate.flow("a", "b")
        t2 = self.gate.flow("c", "a")
        t1.__enter__()
        t2.__enter__()
        self.assertEqual(self.state("a").active, 1)
        self.assertEqual(self.state("c").active, 1)
        t2.__exit__(None, None, None)
        self.assertEqual(self.state("c").active, 0)
        self.assertEqual(self.state("a").active, 1)
        t1.__exit__(None, None, None)
        self.assertEqual(self.state("a").active, 0)
        data = rows(self.registry)
        self.assertEqual(data["a"]["count"], 1)
        self.assertEqual(data["c"]["count"], 1)


class ProtectedTierTest(TestCase):
    def setUp(self):
        self.registry = Histograms(["commit", "other"], [10, 50])
        self.clock = FakeClock()
        plans = {"commit": policy(normal=3, protected=1, min_samples=2),
                 "other": policy(normal=3, protected=1, min_samples=2)}
        self.gate = Admission(self.registry, plans, staged=True, clock_ns=self.clock)

    def slow_commit_to_protected(self, count):
        for _ in range(count):
            with self.gate.work("commit"):
                self.clock.advance(51)
        decision = self.gate.refresh()["commit"]
        self.assertEqual(decision.limit, 1)
        self.assertTrue(decision.applied)

    def test_protected_continuation_target_blocks_a_new_flow(self):
        # Downgrade commit (the flow's NEXT op) while its protected commitment is
        # already full; a new flow adding another commit promise must be refused,
        # proving both endpoints of a flow are reviewed.
        with self.gate.flow("other", "commit"):
            with self.gate.work("commit"):
                self.slow_commit_to_protected(2)
                # commitment(commit) = active 1 + promised 1 = protected_limit 1.
                ran = []
                with self.assertRaises(Busy):
                    with self.gate.flow("other", "commit"):
                        ran.append(True)
                self.assertEqual(ran, [])
                # Plain work on the unrelated op still enters under protection.
                with self.gate.work("other"):
                    self.clock.advance(1)

    def test_new_flow_admitted_in_normal_tier_with_promise_and_work(self):
        with self.gate.flow("other", "commit") as held:
            with self.gate.work("commit"):
                # Second flow targeting commit still fits the normal commitment (cap 3).
                with self.gate.flow("other", "commit") as newer:
                    self.assertEqual(self.gate._operations["commit"].promised, 2)
                    self.assertTrue(held)
                    self.assertTrue(newer)

    def test_protected_tier_rejects_new_flow_before_first_stage_body(self):
        # Same held shape as the normal-tier test: one plain commit slot + one
        # outstanding commit promise from an accepted ticket.
        with self.gate.flow("other", "commit") as ticket:
            with self.gate.work("commit"):
                self.slow_commit_to_protected(2)
                ran = []
                with self.assertRaises(Busy):
                    with self.gate.flow("other", "commit"):
                        ran.append(True)
                self.assertEqual(ran, [])
                # Work unrelated to commit is still admitted, and sampled.
                with self.gate.work("other"):
                    self.clock.advance(2)
                # The old ticket still cashes against the hard cap, skipping the
                # protected review: actual occupancy becomes 2, above limit 1.
                with ticket.work():
                    self.clock.advance(4)
                    self.assertEqual(self.gate._operations["commit"].active, 2)

    def test_protected_tier_full_acceptance_scenario_and_recovery(self):
        with self.gate.flow("other", "commit") as ticket:
            with self.gate.work("commit"):
                self.slow_commit_to_protected(2)
                with self.assertRaises(Busy):
                    with self.gate.flow("other", "commit"):
                        self.fail("protected commitment exceeded")
                # Outstanding promise survives the downgrade and cashes normally.
                with ticket.work():
                    self.clock.advance(4)
                    self.assertEqual(self.gate._operations["commit"].active, 2)
                self.assertEqual(self.gate._operations["commit"].active, 1)
            data = rows(self.registry)
            # Two slow driver samples, the plain held commit, and the cashed ticket.
            self.assertEqual(data["commit"]["count"], 4)
            self.assertEqual(data["commit"]["sum_us"], 212)
            # Fast samples restore the normal limit; identical new flow now enters.
            with self.gate.work("commit"):
                self.gate.refresh()
            decision = self.gate.refresh()["commit"]
            self.assertEqual(decision.limit, 3)
            with self.gate.flow("other", "commit") as fresh:
                self.assertEqual(self.gate._operations["commit"].promised, 1)
                with fresh.work():
                    self.clock.advance(1)
        data = rows(self.registry)
        self.assertEqual(data["commit"]["count"], 6)
        self.assertEqual(data["other"]["count"], 2)
        # Bucket boundary check: the two 51us drivers are slow (>50us); the held
        # and recovery work (0us) and the 1us ticket are in the lowest bucket.
        self.assertEqual(data["commit"]["le_counts"], [3, 3])


class PlainVsContinuationTest(TestCase):
    def setUp(self):
        self.registry = Histograms(["target", "src"], [10, 50])
        self.clock = FakeClock()
        plans = {"target": policy(normal=2), "src": policy(normal=2)}
        self.gate = Admission(self.registry, plans, staged=True, clock_ns=self.clock)

    def test_plain_work_cannot_take_the_last_ready_path(self):
        with self.gate.flow("src", "target"):
            with self.gate.work("target"):
                with self.assertRaises(Busy):
                    with self.gate.work("target"):
                        self.fail("last continuation opportunity was stolen")
            data = rows(self.registry)
            self.assertEqual(data["target"]["count"], 1)
        # Without the outstanding ticket the same plain requests both run.
        with self.gate.work("target"), self.gate.work("target"):
            pass
        data = rows(self.registry)
        self.assertEqual(data["target"]["count"], 3)
        self.assertEqual(data["target"]["sum_us"], 0)

    def test_concurrent_plain_work_exactly_one_wins_and_rejected_leave_no_samples(self):
        with self.gate.flow("src", "target"):
            ready = []
            start = Event()
            done = Event()
            results = []
            lock = Lock()

            def attempt():
                ready.append(1)
                start.wait(5)
                try:
                    with self.gate.work("target"):
                        with lock:
                            results.append(True)
                        done.wait(5)
                        return True
                except Busy:
                    with lock:
                        results.append(False)
                    return False

            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [pool.submit(attempt) for _ in range(5)]
                while len(ready) < 5:
                    Event().wait(0.01)
                start.set()
                # Wait until every rejected attempt has finished; the winner stays held.
                deadline = 0
                while sum(r is False for r in results) < 4 and deadline < 500:
                    deadline += 1
                    Event().wait(0.01)
                self.assertEqual(sum(results), 1)
                self.assertEqual(len(results), 5)
                done.set()
                self.assertEqual(sum(f.result(2) for f in futures), 1)
            data = rows(self.registry)
            # Only the single admitted plain work, once it exits, samples; while
            # held inside this block no target sample may exist yet either.
        data = rows(self.registry)
        self.assertEqual(data["target"]["count"], 1)
        self.assertEqual(data["src"]["count"], 1)


class OppositeDirectionsTest(TestCase):
    def setUp(self):
        self.registry = Histograms(["a", "b", "c", "d"], [10, 50])
        self.clock = FakeClock()
        plans = {name: policy(normal=2) for name in ("a", "b", "c", "d")}
        self.gate = Admission(self.registry, plans, staged=True, clock_ns=self.clock)

    def open(self, first, next_op):
        manager = self.gate.flow(first, next_op)
        return manager, manager.__enter__()

    @staticmethod
    def close(manager):
        manager.__exit__(None, None, None)

    def test_opposite_directions_cannot_all_be_accepted_then_all_finish(self):
        mgr1, outer1 = self.open("a", "b")
        mgr2, outer2 = self.open("a", "b")
        mgr_cd, unrelated = self.open("c", "d")
        mgr_ba, first_ba = self.open("b", "a")
        try:
            with self.assertRaises(Busy):
                with self.gate.flow("b", "a"):
                    self.fail("deadlocking second B->A must be refused")
            # The unrelated flow keeps moving independently and finishes.
            with unrelated.work():
                pass
            # First A->B continuation can cash: b still has one free actual slot.
            with outer1.work():
                pass
        finally:
            # Fully closing the first A->B request frees its a first-stage slot.
            self.close(mgr1)
        try:
            # The accepted B->A ticket can now cash against the freed a slot.
            with first_ba.work():
                pass
        finally:
            self.close(mgr_ba)
        # The remaining A->B ticket runs its continuation and closes.
        try:
            with outer2.work():
                pass
        finally:
            self.close(mgr2)
            self.close(mgr_cd)
        for name in ("a", "b", "c", "d"):
            self.assertEqual(self.gate._operations[name].active, 0)
            self.assertEqual(self.gate._operations[name].promised, 0)
        data = rows(self.registry)
        self.assertEqual(data["a"]["count"], 3)
        # ba1 first stage plus the two A->B continuations.
        self.assertEqual(data["b"]["count"], 3)
        self.assertEqual(data["c"]["count"], 1)
        self.assertEqual(data["d"]["count"], 1)

    def test_three_tickets_one_downstream_permissive_policy_branch(self):
        mgr1, t1 = self.open("a", "b")
        mgr2, t2 = self.open("c", "b")
        try:
            # First two must be admitted: hard caps and completion both fine.
            mgr3, t3 = self.open("d", "b")
            try:
                with t1.work():
                    with t2.work():
                        # Both downstream slots are actual-occupied now.
                        with self.assertRaises(Busy):
                            with t3.work():
                                self.fail("hard cap must block the third continuation")
                    # t2 released b: the same ticket retries successfully.
                    with t3.work():
                        pass
            finally:
                self.close(mgr3)
        finally:
            self.close(mgr2)
            self.close(mgr1)
        data = rows(self.registry)
        self.assertEqual(data["b"]["count"], 3)
        self.assertEqual(data["a"]["count"], 1)
        self.assertEqual(data["c"]["count"], 1)
        self.assertEqual(data["d"]["count"], 1)

    def test_when_third_first_stage_is_busy_first_two_still_run_together(self):
        # Two tickets whose FIRST stage occupies b (promises point at a and c),
        # so b's actual slots are full and a third first stage needing b is busy.
        mgr_h1, h1 = self.open("b", "a")
        mgr_h2, h2 = self.open("b", "c")
        try:
            self.assertEqual(self.gate._operations["b"].active, 2)
            with self.assertRaises(Busy):
                with self.gate.flow("b", "d"):
                    self.fail("no hard slot for a third first stage")
            # The two held tickets can still both cash and finish.
            with h1.work():
                pass
            with h2.work():
                pass
        finally:
            self.close(mgr_h2)
            self.close(mgr_h1)
        # With b free, the previously refused first stage is now accepted, and
        # two normal tickets sharing b as a downstream run together.
        mgr1, t1 = self.open("a", "b")
        mgr2, t2 = self.open("c", "b")
        try:
            with t1.work(), t2.work():
                self.assertEqual(self.gate._operations["b"].active, 2)
            with self.gate.flow("d", "b") as t3:
                with t3.work():
                    pass
        finally:
            self.close(mgr2)
            self.close(mgr1)
        data = rows(self.registry)
        # Two filler b first stages plus three downstream continuations.
        self.assertEqual(data["b"]["count"], 5)


class AdmissionRuleGuaranteeTest(TestCase):
    def setUp(self):
        self.registry = Histograms(["a", "b", "c", "d"], [10, 50])
        self.clock = FakeClock()
        plans = {name: policy(normal=2) for name in ("a", "b", "c", "d")}
        self.gate = Admission(self.registry, plans, staged=True, clock_ns=self.clock)

    def open(self, first, next_op):
        manager = self.gate.flow(first, next_op)
        return manager, manager.__enter__()

    @staticmethod
    def close(manager):
        manager.__exit__(None, None, None)

    def test_rule3_must_admit_while_commitment_within_normal_limit(self):
        # Unrelated over-commitment on a cannot block a request touching only b/c.
        m1, _ = self.open("a", "d")
        m2, _ = self.open("a", "d")
        try:
            # a is fully actual-occupied; a new a flow must be busy, but b work
            # that stays within b's normal commitment must still be admitted.
            with self.assertRaises(Busy):
                with self.gate.flow("a", "b"):
                    self.fail("a has no hard slot")
            with self.gate.work("b"):
                pass
            # A flow whose endpoints both stay within normal commitments enters.
            with self.gate.flow("c", "b"):
                pass
        finally:
            self.close(m2)
            self.close(m1)

    def test_rule2a_preserves_an_immediately_cashable_ticket(self):
        # One plain b first stage leaves a single free b slot; the waiting
        # a->b ticket can cash it immediately.
        mgr_b, _ = self.open("b", "d")
        mgr_t, waiting = self.open("a", "b")
        try:
            self.assertEqual(self.gate._operations["b"].active, 1)
            # A new first stage on b would consume the last slot the waiting
            # ticket can cash right now, so it must be rejected before its body.
            ran = []
            with self.assertRaises(Busy):
                with self.gate.flow("b", "c"):
                    ran.append(True)
            self.assertEqual(ran, [])
            # The waiting ticket can still cash immediately.
            with waiting.work():
                pass
        finally:
            self.close(mgr_t)
            self.close(mgr_b)

    def test_rule2a_allows_entry_when_no_ticket_is_immediately_ready(self):
        # Two first stages occupy a fully, so no ticket targeting a is ready;
        # filling b (which has no waiting ticket) is permitted.
        m1, _ = self.open("a", "b")
        m2, _ = self.open("a", "c")
        try:
            with self.gate.flow("b", "d"):
                pass
        finally:
            self.close(m2)
            self.close(m1)

    def test_promise_does_not_count_as_actual_occupancy(self):
        # A promise on b occupies no b slot, so plain b work can use the actual
        # slots. The first plain work runs (active was zero); filling the last
        # slot is then blocked because the promise must keep an immediate path.
        mgr, _ = self.open("c", "b")
        try:
            self.assertEqual(self.gate._operations["b"].promised, 1)
            self.assertEqual(self.gate._operations["b"].active, 0)
            with self.gate.work("b"):
                self.assertEqual(self.gate._operations["b"].active, 1)
                # While this actual slot is held, filling the last one is blocked
                # because the promise must keep an immediately cashable path.
                with self.assertRaises(Busy):
                    with self.gate.work("b"):
                        self.fail("last continuation opportunity was stolen")
        finally:
            self.close(mgr)


class HardCapAndRefreshTest(TestCase):
    def setUp(self):
        self.registry = Histograms(["a", "b"], [10, 50])
        self.clock = FakeClock()
        plans = {"a": policy(normal=2), "b": policy(normal=2)}
        self.gate = Admission(self.registry, plans, staged=True, clock_ns=self.clock)

    def test_normal_limit_is_never_breached_under_contention(self):
        ready = []
        start = Event()
        release = Event()
        decided = []
        decision_lock = Lock()

        def flow_attempt():
            ready.append(1)
            start.wait(5)
            try:
                with self.gate.flow("a", "b"):
                    with decision_lock:
                        decided.append("flow")
                    release.wait(5)
                return "flow"
            except Busy:
                with decision_lock:
                    decided.append("busy")
                return "busy"

        def work_attempt():
            ready.append(1)
            start.wait(5)
            try:
                with self.gate.work("a"):
                    with decision_lock:
                        decided.append("work")
                    release.wait(5)
                return "work"
            except Busy:
                with decision_lock:
                    decided.append("busy")
                return "busy"

        with ThreadPoolExecutor(max_workers=7) as pool:
            futures = ([pool.submit(flow_attempt) for _ in range(4)]
                       + [pool.submit(work_attempt) for _ in range(3)])
            while len(ready) < 7:
                Event().wait(0.01)
            start.set()
            # Wait for all five rejections while the two winners keep holding slots.
            deadline = 0
            while decided.count("busy") < 5 and deadline < 500:
                deadline += 1
                Event().wait(0.01)
            self.assertEqual(decided.count("busy"), 5)
            self.assertEqual(len(decided), 7)
            release.set()
            outcomes = [f.result(5) for f in futures]
        self.assertEqual(outcomes.count("busy"), 5)
        self.assertLessEqual(self.gate._operations["a"].active, 2)
        data = rows(self.registry)
        self.assertEqual(data["a"]["count"], 2)
        # Every admitted flow skipped its continuation; b never saw occupancy.
        self.assertEqual(data["b"]["count"], 0)
        self.assertEqual(self.gate._operations["b"].promised, 0)

    def test_refresh_and_metrics_loopback_keep_working_staged(self):
        with self.gate.flow("a", "b") as ticket:
            self.clock.advance(51)
            with ticket.work():
                self.clock.advance(51)
        # Give b enough samples in its own window for the downgrade to apply.
        with self.gate.work("b"):
            self.clock.advance(51)
        with self.gate.work("a"):
            self.clock.advance(51)
        decisions = self.gate.refresh()
        self.assertEqual(decisions["a"].limit, 1)
        self.assertEqual(decisions["b"].limit, 1)
        with self.gate.flow("a", "b"):
            with self.assertRaises(Busy):
                with self.gate.work("a"):
                    self.fail("protected tier must hold in staged mode")
        with local_http(scrape_app(self.registry)) as address:
            port = urlsplit(address).port
            conn = HTTPConnection("127.0.0.1", port, timeout=3)
            try:
                conn.request("GET", "/metrics")
                with conn.getresponse() as response:
                    self.assertEqual(response.status, 200)
                    payload = json.loads(response.read())
            finally:
                conn.close()
        self.assertEqual(payload, self.registry.snapshot())
        self.assertEqual({row["operation"]: row["count"] for row in payload["series"]},
                         {"a": 3, "b": 2})
