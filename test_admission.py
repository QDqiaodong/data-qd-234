from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from unittest import TestCase

from admission import Admission, Busy, Decision, Policy
from admission_demo import DemoClock, run
from metrics import Histograms


class AdmissionTest(TestCase):
    def setUp(self):
        self.registry = Histograms(["parse", "lookup"], [10, 50])
        self.clock = DemoClock()
        self.policy = Policy(2, 1, 50, 2, 50)

    def gate(self, plans=None):
        if plans is None:
            plans = {"parse": self.policy, "lookup": self.policy}
        return Admission(self.registry, plans, clock_ns=self.clock)

    def test_historical_observations_and_incomplete_windows(self):
        self.registry.observe("parse", 1000)
        self.registry.observe("parse", 1000)
        gate = self.gate()
        self.assertEqual(gate.refresh()["parse"], Decision(0, 0, 2, False))
        self.registry.observe("parse", 51)
        self.assertEqual(gate.refresh()["parse"], Decision(1, 1, 2, False))
        self.registry.observe("parse", 50)
        # Exactly 50% slow is accepted; exactly 50us is not slow.
        self.assertEqual(gate.refresh(), {
            "parse": Decision(2, 1, 2, True), "lookup": Decision(0, 0, 2, False)})
        self.assertEqual(gate.refresh()["parse"], Decision(0, 0, 2, False))
        self.assertEqual(self.registry.snapshot()["series"][1]["count"], 4)

    def test_refresh_limits_new_work_without_revoking_active_leases(self):
        gate = self.gate()
        with gate.work("parse"):
            with gate.work("parse"):
                for _ in range(2):
                    self.registry.observe("parse", 51)
                self.assertEqual(gate.refresh()["parse"], Decision(2, 2, 1, True))
                with self.assertRaises(Busy):
                    with gate.work("parse"):
                        self.fail("denied application work ran")
                with gate.work("lookup"):
                    self.clock.advance(10)
            # One active request still uses the sole allowed slot.
            with self.assertRaises(Busy):
                with gate.work("parse"):
                    self.fail("denied application work ran")
            self.assertEqual(gate.refresh()["parse"], Decision(1, 0, 1, False))
        self.assertEqual(gate.refresh()["parse"], Decision(2, 0, 2, True))
        rows = {r["operation"]: r for r in self.registry.snapshot()["series"]}
        self.assertEqual(rows["parse"]["count"], 4)
        self.assertEqual(rows["lookup"]["sum_us"], 10)

    def test_work_exception_is_counted_and_releases_capacity(self):
        gate = self.gate()
        with self.assertRaisesRegex(RuntimeError, "work failed"):
            with gate.work("parse"):
                self.clock.advance(51)
                raise RuntimeError("work failed")
        with gate.work("parse"):
            self.clock.advance(50)
            with gate.work("parse"):
                pass
        rows = {r["operation"]: r for r in self.registry.snapshot()["series"]}
        self.assertEqual(rows["parse"]["count"], 3)
        self.assertEqual(rows["parse"]["sum_us"], 101)
        self.assertEqual(gate.refresh()["parse"], Decision(3, 1, 2, True))

    def test_concurrent_admission_cannot_oversubscribe(self):
        gate = self.gate()
        release = Event()
        attempted = Barrier(7, timeout=3)

        def attempt():
            try:
                with gate.work("parse"):
                    attempted.wait()
                    if not release.wait(3):
                        raise RuntimeError("workers not released")
                    return True
            except Busy:
                attempted.wait()
                return False

        with ThreadPoolExecutor(max_workers=6) as pool:
            workers = [pool.submit(attempt) for _ in range(6)]
            try:
                attempted.wait()
                self.assertEqual(gate.refresh()["parse"], Decision(0, 0, 2, False))
            finally:
                release.set()
            self.assertEqual(sum(f.result(4) for f in workers), 2)
        self.assertEqual(gate.refresh()["parse"], Decision(2, 0, 2, True))

    def test_invalid_policies_rejected(self):
        for params in [(1, 1, 50, 2, 50), (2, 0, 50, 2, 50), (2, 1, 50, 0, 50),
                       (2, 1, 50, 2, 100), (2, 1, 50, 2, False)]:
            with self.subTest(params=params), self.assertRaises(ValueError):
                Policy(*params)
        for plans in ({}, {"unknown": self.policy}, {"parse": Policy(2, 1, 11, 2, 50)}):
            with self.subTest(plans=plans), self.assertRaises(ValueError):
                self.gate(plans)

    def test_same_http_request_is_rejected_and_then_restored(self):
        limited, rejected, unaffected, empty, recovered, allowed, scrape = run()
        self.assertEqual(limited, Decision(3, 2, 1, True))
        self.assertEqual(rejected, (429, b"busy"))
        self.assertEqual(unaffected, (200, b"42"))
        self.assertEqual(empty, Decision(0, 0, 1, False))
        self.assertEqual(recovered, Decision(2, 0, 2, True))
        self.assertEqual(allowed, (200, b"42"))
        self.assertEqual(scrape, {"format": "latency-histogram-v1", "bounds_us": [10, 50],
                                  "series": [
                                      {"operation": "lookup", "le_counts": [1, 1],
                                       "count": 1, "sum_us": 10},
                                      {"operation": "parse", "le_counts": [0, 4],
                                       "count": 6, "sum_us": 302}]})
