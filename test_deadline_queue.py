"""DeadlineQueue acceptance: real start results, statuses, and cumulative counts."""
from unittest import TestCase

from admission import Admission, DeadlineQueue, Policy
from metrics import Histograms


class TickClock:
    """Virtual clock where one tick is one microsecond, read as nanoseconds."""

    def __init__(self):
        self.tick = 0

    def __call__(self):
        return self.tick * 1000

    def set(self, tick):
        self.tick = tick


class Session:
    """A queue with its gate, registry, and clock, driven tick by tick."""

    def __init__(self, operations=("op",), slots=2):
        self.registry = Histograms(list(operations), [10, 50])
        self.clock = TickClock()
        plans = {name: Policy(2, 1, 50, 2, 50) for name in operations}
        self.gate = Admission(self.registry, plans, clock_ns=self.clock)
        self.slots = slots
        self.queue = DeadlineQueue(self.gate, slots=slots)
        self.inflight = set()

    def submit(self, name, operation, now, duration, *, deadline=None):
        self.clock.set(now)
        return self.queue.submit(name, operation, now, duration, deadline=deadline)

    def advance(self, now):
        self.clock.set(now)
        started = self.queue.advance(now)
        self.inflight.update(started)
        assert len(self.inflight) <= self.slots, "dispatch exceeded the slots"
        return started

    def extend(self, name, new_end, now):
        self.clock.set(now)
        return self.queue.extend(name, new_end, now)

    def finish(self, name, now):
        self.clock.set(now)
        settled = self.queue.finish(name, now)
        if settled:
            self.inflight.discard(name)
        return settled

    def row(self, operation="op"):
        rows = {row["operation"]: row
                for row in self.registry.snapshot()["series"]}
        return rows[operation]

    def statuses(self, *names):
        return {name: self.queue.status(name) for name in names}


class DeadlineQueueTest(TestCase):

    def test_commitments_honored_and_batch_uses_idle_slot(self):
        # Acceptance 1, on-time branch: the second slot stays free at tick 0,
        # so batch B must start at tick 1; R finishes as estimated at tick 2
        # and every interactive commitment is delivered.
        session = Session()
        queue = session.queue
        self.assertTrue(session.submit("R", "op", 0, 2, deadline=2))
        self.assertEqual(session.advance(0), ("R",))
        self.assertTrue(session.submit("I", "op", 0, 2, deadline=4))
        self.assertTrue(session.submit("J", "op", 0, 2, deadline=5))
        # Interactive work need not start early: the second slot is kept.
        self.assertEqual(session.advance(0), ())
        self.assertTrue(session.submit("B", "op", 1, 2))
        # B fits beside R without endangering I or J, so it must start now.
        self.assertEqual(session.advance(1), ("B",))
        print(f"tick 1 started=('B',) statuses={session.statuses('R', 'I', 'J', 'B')}")
        # Tick 2: R's estimate is due but unconfirmed, so no slot is handed
        # out and no third request starts; I keeps its commitment.
        self.assertEqual(session.advance(2), ())
        self.assertEqual(session.statuses("R", "I", "J", "B"),
                         {"R": "running", "I": "waiting",
                          "J": "waiting", "B": "running"})
        self.assertEqual(session.row()["count"], 0)
        # Confirming R at the same tick frees the slot for a re-advance.
        self.assertTrue(session.finish("R", 2))
        self.assertEqual(queue.status("R"), "done")
        self.assertEqual(session.advance(2), ("I",))
        self.assertTrue(session.finish("B", 3))
        self.assertEqual(session.advance(3), ("J",))
        self.assertTrue(session.finish("I", 4))
        self.assertTrue(session.finish("J", 5))
        row = session.row()
        self.assertEqual((row["count"], row["sum_us"]), (4, 8))
        self.assertEqual(session.statuses("R", "I", "J", "B"),
                         {"R": "done", "I": "done", "J": "done", "B": "done"})
        print(f"on-time branch: completions={row['count']} sum_us={row['sum_us']} "
              f"statuses={session.statuses('R', 'I', 'J', 'B')}")

    def test_extension_marks_missed_and_preserves_deliverable(self):
        # Acceptance 1+2, extension branch: after tick 1's dispatch, R's end
        # slips to tick 3; I becomes undeliverable and is missed, J is kept.
        session = Session()
        queue = session.queue
        self.assertTrue(session.submit("R", "op", 0, 2, deadline=2))
        self.assertEqual(session.advance(0), ("R",))
        self.assertTrue(session.submit("I", "op", 0, 2, deadline=4))
        self.assertTrue(session.submit("J", "op", 0, 2, deadline=5))
        self.assertEqual(session.advance(0), ())
        self.assertTrue(session.submit("B", "op", 1, 2))
        self.assertEqual(session.advance(1), ("B",))
        # The slip is reported only after tick 1's dispatch.
        session.extend("R", 3, 1)
        self.assertEqual(session.statuses("R", "I", "J", "B"),
                         {"R": "running", "I": "missed",
                          "J": "waiting", "B": "running"})
        print(f"after extend: statuses={session.statuses('R', 'I', 'J', 'B')}")
        # A missed request is not running, so finishing it is illegal.
        with self.assertRaises(ValueError):
            session.finish("I", 1)
        # Tick 2, advance first: R and B still hold both slots, no third
        # request starts, and nothing has been metered yet.  (I was never
        # dispatched in this branch, so there is nothing to confirm at 2.)
        self.assertEqual(session.advance(2), ())
        self.assertEqual(session.statuses("R", "B"),
                         {"R": "running", "B": "running"})
        self.assertEqual(session.row()["count"], 0)
        # Tick 3: R confirms late; exactly one completion, exactly +3us.
        before = session.row()
        self.assertTrue(session.finish("R", 3))
        self.assertEqual(queue.status("R"), "late")
        after = session.row()
        self.assertEqual(after["count"] - before["count"], 1)
        self.assertEqual(after["sum_us"] - before["sum_us"], 3)
        # A repeated confirmation changes nothing.
        self.assertFalse(session.finish("R", 3))
        self.assertEqual((session.row()["count"], session.row()["sum_us"]), (1, 3))
        self.assertTrue(session.finish("B", 3))
        self.assertEqual(session.advance(3), ("J",))
        self.assertTrue(session.finish("J", 5))
        self.assertEqual(queue.status("J"), "done")
        row = session.row()
        # I never started, so it was never metered: only R, B, J count.
        self.assertEqual((row["count"], row["sum_us"]), (3, 7))
        print(f"extension branch: completions={row['count']} sum_us={row['sum_us']} "
              f"statuses={session.statuses('R', 'I', 'J', 'B')}")

    def test_same_tick_finish_pending_keeps_commitment(self):
        # An expired estimate blocks dispatch but must not mark a still
        # reachable request missed; confirming the finish re-enables the tick.
        session = Session(slots=1)
        queue = session.queue
        self.assertTrue(session.submit("R", "op", 0, 2, deadline=2))
        self.assertEqual(session.advance(0), ("R",))
        self.assertTrue(session.submit("I", "op", 0, 2, deadline=4))
        self.assertEqual(session.advance(2), ())
        self.assertEqual(queue.status("I"), "waiting")
        self.assertTrue(session.finish("R", 2))
        self.assertEqual(session.advance(2), ("I",))
        self.assertTrue(session.finish("I", 4))
        self.assertEqual(queue.status("I"), "done")
        self.assertEqual((session.row()["count"], session.row()["sum_us"]), (2, 4))

    def test_stale_estimate_without_update_forfeits_commitment(self):
        # The caller moved past R's estimated end without finishing or
        # extending it, so I's commitment is lost at the later tick.
        session = Session(slots=1)
        queue = session.queue
        self.assertTrue(session.submit("R", "op", 0, 2, deadline=2))
        self.assertEqual(session.advance(0), ("R",))
        self.assertTrue(session.submit("I", "op", 0, 2, deadline=4))
        self.assertEqual(session.advance(2), ())
        self.assertEqual(queue.status("I"), "waiting")
        self.assertEqual(session.advance(3), ())
        self.assertEqual(queue.status("I"), "missed")
        self.assertTrue(session.finish("R", 3))
        self.assertEqual(queue.status("R"), "late")
        # Missed before starting, I is never dispatched or metered.
        self.assertEqual(session.advance(3), ())
        self.assertEqual((session.row()["count"], session.row()["sum_us"]), (1, 3))

    def test_operation_limits_and_per_operation_metering(self):
        session = Session(operations=("a", "b"), slots=2)
        self.assertTrue(session.submit("A1", "a", 0, 2, deadline=2))
        self.assertTrue(session.submit("A2", "a", 0, 2, deadline=2))
        # Two running per operation is the normal limit: a third overlapping
        # request of either operation cannot be promised.
        self.assertFalse(session.submit("A3", "a", 0, 2, deadline=2))
        self.assertFalse(session.submit("B1", "b", 0, 2, deadline=2))
        # Rejection consumed no name: B1 is reusable with a feasible deadline.
        self.assertTrue(session.submit("B1", "b", 0, 2, deadline=4))
        self.assertEqual(session.advance(0), ("A1", "A2"))
        self.assertTrue(session.finish("A1", 2))
        self.assertTrue(session.finish("A2", 2))
        self.assertEqual(session.advance(2), ("B1",))
        self.assertTrue(session.finish("B1", 4))
        self.assertEqual((session.row("a")["count"], session.row("a")["sum_us"]), (2, 4))
        self.assertEqual((session.row("b")["count"], session.row("b")["sum_us"]), (1, 2))

    def test_unsatisfiable_first_submit_consumes_nothing(self):
        session = Session()
        queue = session.queue
        self.assertFalse(session.submit("X", "op", 0, 5, deadline=3))
        self.assertFalse(session.submit("Y", "op", 0, 2, deadline=1))
        self.assertEqual(session.row()["count"], 0)
        with self.assertRaises(ValueError):
            queue.status("X")
        # The name was not consumed, and a feasible retry is accepted.
        self.assertTrue(session.submit("X", "op", 0, 2, deadline=2))
        self.assertEqual(session.advance(0), ("X",))
        self.assertTrue(session.finish("X", 2))
        self.assertEqual((session.row()["count"], session.row()["sum_us"]), (1, 2))

    def test_new_interactive_never_bumps_existing_commitments(self):
        session = Session(slots=1)
        queue = session.queue
        self.assertTrue(session.submit("I1", "op", 0, 2, deadline=4))
        self.assertTrue(session.submit("I2", "op", 0, 2, deadline=3))
        # I3 cannot join without breaking I1 or I2, so it is refused instead.
        self.assertFalse(session.submit("I3", "op", 0, 2, deadline=3))
        self.assertEqual(session.statuses("I1", "I2"),
                         {"I1": "waiting", "I2": "waiting"})
        self.assertEqual(session.advance(0), ("I2",))
        self.assertTrue(session.finish("I2", 2))
        self.assertEqual(session.advance(2), ("I1",))
        self.assertTrue(session.finish("I1", 4))
        self.assertEqual((session.row()["count"], session.row()["sum_us"]), (2, 4))

    def test_exact_deadline_counts_on_time_and_later_finish_is_late(self):
        session = Session(slots=1)
        queue = session.queue
        self.assertTrue(session.submit("E", "op", 0, 3, deadline=3))
        self.assertEqual(session.advance(0), ("E",))
        self.assertTrue(session.finish("E", 3))
        self.assertEqual(queue.status("E"), "done")
        self.assertTrue(session.submit("L", "op", 3, 2, deadline=6))
        # L need not start immediately; the idle slot is allowed.
        self.assertEqual(session.advance(3), ())
        self.assertEqual(session.advance(4), ("L",))
        # Estimate due but unconfirmed: still running, slot not handed out.
        self.assertEqual(session.advance(6), ())
        self.assertEqual(queue.status("L"), "running")
        # Even past the deadline an unconfirmed request stays running.
        self.assertEqual(session.advance(7), ())
        self.assertEqual(queue.status("L"), "running")
        self.assertTrue(session.finish("L", 7))
        self.assertEqual(queue.status("L"), "late")
        self.assertEqual((session.row()["count"], session.row()["sum_us"]), (2, 6))

    def test_batch_waits_when_starting_would_break_a_commitment(self):
        session = Session(slots=1)
        self.assertTrue(session.submit("I", "op", 0, 2, deadline=3))
        self.assertTrue(session.submit("B", "op", 0, 2))
        # Starting B now would push I past its deadline, so the slot idles.
        self.assertEqual(session.advance(0), ())
        # At tick 1 I must start to keep its promise and takes the slot.
        self.assertEqual(session.advance(1), ("I",))
        self.assertTrue(session.finish("I", 3))
        # Idle capacity is safe now: the batch request is not starved.
        self.assertEqual(session.advance(3), ("B",))
        self.assertTrue(session.finish("B", 5))
        self.assertEqual((session.row()["count"], session.row()["sum_us"]), (2, 4))

    def test_non_monotonic_ticks_rejected_without_state_change(self):
        session = Session()
        queue = session.queue
        self.assertTrue(session.submit("R", "op", 0, 2, deadline=2))
        self.assertEqual(session.advance(0), ("R",))
        self.assertEqual(session.advance(2), ())
        for bad in (1, True, 1.5, "2"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                queue.advance(bad)
            with self.assertRaises(ValueError):
                queue.submit("S", "op", bad, 1)
            with self.assertRaises(ValueError):
                queue.extend("R", 3, bad)
            with self.assertRaises(ValueError):
                queue.finish("R", bad)
        self.assertEqual(queue.status("R"), "running")
        self.assertEqual(session.row()["count"], 0)
        self.assertTrue(session.finish("R", 2))
        self.assertEqual(queue.status("R"), "done")
        self.assertEqual((session.row()["count"], session.row()["sum_us"]), (1, 2))

    def test_invalid_arguments_rejected_without_state_change(self):
        session = Session()
        queue = session.queue
        for bad_name in ("", 123, None):
            with self.subTest(bad_name=bad_name), self.assertRaises(ValueError):
                queue.submit(bad_name, "op", 0, 1)
        self.assertTrue(session.submit("N", "op", 0, 1))
        with self.assertRaises(ValueError):
            queue.submit("N", "op", 0, 1)
        with self.assertRaises(ValueError):
            queue.submit("M", "ghost", 0, 1)
        for bad_duration in (0, -1, 1.5, "2", True):
            with self.subTest(bad_duration=bad_duration), self.assertRaises(ValueError):
                queue.submit("D", "op", 0, bad_duration)
        for bad_deadline in ("x", 1.5, True):
            with self.subTest(bad_deadline=bad_deadline), self.assertRaises(ValueError):
                queue.submit("E", "op", 0, 1, deadline=bad_deadline)
        with self.assertRaises(ValueError):
            queue.extend("N", 5, 0)
        with self.assertRaises(ValueError):
            queue.extend("ghost", 5, 0)
        with self.assertRaises(ValueError):
            queue.finish("N", 0)
        with self.assertRaises(ValueError):
            queue.finish("ghost", 0)
        with self.assertRaises(ValueError):
            queue.status("ghost")
        self.assertEqual(queue.status("N"), "waiting")
        self.assertEqual(session.row()["count"], 0)
        # Extending a running request must move the estimate out, not back.
        self.assertTrue(session.submit("R", "op", 0, 4, deadline=4))
        # R must start now to keep its promise; batch N takes the idle slot.
        self.assertEqual(session.advance(0), ("R", "N"))
        for bad_end in (2, 4, "5", True):
            with self.subTest(bad_end=bad_end), self.assertRaises(ValueError):
                queue.extend("R", bad_end, 0)
        session.extend("R", 5, 0)
        with self.assertRaises(ValueError):
            queue.extend("R", 5, 0)
        self.assertTrue(session.finish("R", 5))
        self.assertEqual(queue.status("R"), "late")
        self.assertTrue(session.finish("N", 5))
        self.assertEqual(queue.status("N"), "done")
        self.assertEqual((session.row()["count"], session.row()["sum_us"]), (2, 10))

    def test_constructor_validation(self):
        session = Session()
        for bad_slots in (0, 3, "2", True, 1.5):
            with self.subTest(bad_slots=bad_slots), self.assertRaises(ValueError):
                DeadlineQueue(session.gate, slots=bad_slots)
        with self.assertRaises(ValueError):
            DeadlineQueue(object(), slots=1)
        staged = Admission(session.registry, {"op": Policy(2, 1, 50, 2, 50)},
                           clock_ns=session.clock, staged=True)
        with self.assertRaises(ValueError):
            DeadlineQueue(staged, slots=1)
        registry = Histograms(["a", "b", "c"], [10, 50])
        plans = {name: Policy(2, 1, 50, 2, 50) for name in ("a", "b", "c")}
        with self.assertRaises(ValueError):
            DeadlineQueue(Admission(registry, plans, clock_ns=TickClock()), slots=2)
        self.assertIsInstance(DeadlineQueue(session.gate, slots=1), DeadlineQueue)

    def test_queue_capacity_bounds(self):
        session = Session()
        queue = session.queue
        for index in range(6):
            self.assertTrue(
                session.submit(f"I{index}", "op", 0, 1, deadline=60))
        # At most six waiting interactive requests.
        self.assertFalse(session.submit("I6", "op", 0, 1, deadline=60))
        # The refused request consumed no name and batch room remains.
        self.assertTrue(session.submit("I6", "op", 0, 1))
        for index in range(5):
            self.assertTrue(session.submit(f"B{index}", "op", 0, 1))
        # Twelve waiting requests in total is the bound.
        self.assertFalse(session.submit("B5", "op", 0, 1))
        self.assertEqual(session.row()["count"], 0)
        self.assertEqual(queue.status("I6"), "waiting")


if __name__ == "__main__":
    import unittest
    unittest.main()
