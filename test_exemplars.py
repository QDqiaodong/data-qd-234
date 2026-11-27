"""Deterministic tests for optional diagnostic exemplar retention."""
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
import json
from threading import Event
from urllib.parse import urlsplit
from unittest import TestCase

from admission import Admission, Busy, Policy
from delta_export import DeltaExporter, InProcessMeter
from demo import local_scrape
from metrics import Histograms


class RecordingStore:
    """Slot store that records actual slot reads and writes."""

    def __init__(self, capacity):
        self.capacity = capacity
        self.slots = [None] * capacity
        self.put_calls = []
        self.get_calls = []

    def put(self, slot, evidence):
        if not 0 <= slot < self.capacity:
            raise AssertionError(f"write outside slot range: {slot}")
        self.put_calls.append((slot, evidence))
        self.slots[slot] = evidence

    def get(self, slot):
        self.get_calls.append(slot)
        return self.slots[slot]


class PausableGetStore(RecordingStore):
    def __init__(self, capacity):
        super().__init__(capacity)
        self.get_entered = Event()
        self.release_get = Event()

    def get(self, slot):
        self.get_entered.set()
        if not self.release_get.wait(3):
            raise RuntimeError("paused get was not released")
        return super().get(slot)


def registry(capacity=3, reserves=None, store=None, operations=("a",), threshold=10):
    if reserves is None:
        reserves = {op: 1 for op in operations}
    rules = {op: (threshold, reserves[op]) for op in operations}
    store = store or RecordingStore(capacity)
    return Histograms(
        list(operations), [50],
        exemplar_rules=rules,
        exemplar_capacity=capacity,
        exemplar_store=store,
    ), store


class ConfigurationTest(TestCase):
    def test_disabled_by_default_and_open_is_runtime_error(self):
        h = Histograms(["a"], [10])
        with self.assertRaises(RuntimeError):
            h.open_exemplars()
        h.observe("a", 11, evidence=b"valid-evidence-is-ignored")
        self.assertEqual(h.snapshot()["series"][0]["count"], 1)

    def test_partial_configuration_is_value_error(self):
        store = RecordingStore(1)
        rules = {"a": (10, 1)}
        cases = [
            dict(exemplar_rules=rules, exemplar_capacity=1),
            dict(exemplar_rules=rules, exemplar_store=store),
            dict(exemplar_capacity=1, exemplar_store=store),
            dict(exemplar_rules=rules),
            dict(exemplar_capacity=1),
            dict(exemplar_store=store),
        ]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                Histograms(["a"], [10], **kwargs)

    def test_invalid_configuration(self):
        store = RecordingStore(3)
        valid = dict(exemplar_rules={"a": (10, 1)},
                     exemplar_capacity=3, exemplar_store=store)
        bad = [
            dict(exemplar_rules={}),
            dict(exemplar_rules={f"o{i}": (1, 1) for i in range(33)}),
            dict(exemplar_rules={"missing": (10, 1)}),
            dict(exemplar_rules={"a": (-1, 1)}),
            dict(exemplar_rules={"a": (True, 1)}),
            dict(exemplar_rules={"a": (10.0, 1)}),
            dict(exemplar_rules={"a": (10, 0)}),
            dict(exemplar_rules={"a": (10, True)}),
            dict(exemplar_rules={"a": (10, 1.5)}),
            dict(exemplar_capacity=0),
            dict(exemplar_capacity=65),
            dict(exemplar_capacity=True),
            dict(exemplar_capacity=2,
                 exemplar_rules={"a": (10, 1), "b": (10, 2)},
                 exemplar_store=store),
            dict(exemplar_store=object()),
        ]
        for change in bad:
            kwargs = dict(valid)
            kwargs.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                Histograms(["a", "b"], [10], **kwargs)

    def test_invalid_evidence_does_not_count_or_write(self):
        h, store = registry(capacity=2)
        for evidence in ("not-bytes", b"", b"x" * 4097, 123, bytearray(b"x")):
            with self.subTest(length=len(evidence) if hasattr(evidence, "__len__") else None):
                with self.assertRaises(ValueError):
                    h.observe("a", 11, evidence=evidence)
        self.assertEqual(store.put_calls, [])
        self.assertEqual(h.snapshot()["series"][0]["count"], 0)

    def test_threshold_is_strict_and_only_configured_operations_retain(self):
        h, store = registry(capacity=2, operations=("a", "b"))
        h.observe("a", 10, evidence=b"equal")
        h.observe("a", 11, evidence=b"greater")
        h.observe("b", 11, evidence=b"also-configured")
        self.assertEqual([evidence for _, evidence in store.put_calls],
                         [b"greater", b"also-configured"])

        only_a, only_store = registry(capacity=1, operations=("a",))
        only_a.observe("a", 11, evidence=b"yes")
        with self.assertRaises(KeyError):
            only_a.observe("missing", 11, evidence=b"unknown")
        self.assertEqual([evidence for _, evidence in only_store.put_calls],
                         [b"yes"])


class ReaderTest(TestCase):
    def test_empty_reader_does_not_consume_quota(self):
        h, store = registry(capacity=1)
        empties = [h.open_exemplars() for _ in range(10)]
        for reader in empties:
            self.assertIs(iter(reader), reader)
            with self.assertRaises(StopIteration):
                next(reader)
            reader.close()
            reader.close()
        self.assertEqual(h._active_readers, 0)

    def test_open_does_not_read_bytes_and_view_is_finite(self):
        h, store = registry(capacity=4)
        h.observe("a", 11, evidence=b"A")
        first = h.open_exemplars()
        self.assertEqual(store.get_calls, [])
        h.observe("a", 12, evidence=b"B")

        self.assertEqual(next(first), ("a", 11, b"A"))
        with self.assertRaises(StopIteration):
            next(first)
        first.close()

        second = h.open_exemplars()
        self.assertEqual(list(second), [("a", 11, b"A"), ("a", 12, b"B")])
        self.assertEqual(store.get_calls, [0, 0, 1])

    def test_close_releases_view_and_is_idempotent(self):
        h, store = registry(capacity=2)
        h.observe("a", 11, evidence=b"A")
        h.observe("a", 11, evidence=b"B")
        reader = h.open_exemplars()
        reader.close()
        reader.close()
        with self.assertRaises(StopIteration):
            next(reader)

        h.observe("a", 11, evidence=b"C")
        self.assertEqual(store.put_calls[-1], (0, b"C"))
        self.assertEqual(list(h.open_exemplars()),
                         [("a", 11, b"B"), ("a", 11, b"C")])

    def test_identical_bytes_from_separate_completions_are_not_merged(self):
        h, store = registry(capacity=2)
        h.observe("a", 11, evidence=b"same")
        h.observe("a", 11, evidence=b"same")
        self.assertEqual(len(store.put_calls), 2)
        self.assertEqual(list(h.open_exemplars()), [
            ("a", 11, b"same"), ("a", 11, b"same")])

    def test_at_most_four_readers_with_pending_fragments(self):
        h, _ = registry(capacity=1)
        h.observe("a", 11, evidence=b"A")
        readers = [h.open_exemplars() for _ in range(4)]
        with self.assertRaises(RuntimeError):
            h.open_exemplars()
        self.assertEqual(h._active_readers, 4)

        # Natural exhaustion returns the reader's quota.
        self.assertEqual(next(readers[0]), ("a", 11, b"A"))
        replacement = h.open_exemplars()
        self.assertEqual(h._active_readers, 4)
        with self.assertRaises(RuntimeError):
            h.open_exemplars()

        # Closing another reader frees enough quota for a replacement.
        readers[1].close()
        replacement2 = h.open_exemplars()
        self.assertEqual(h._active_readers, 4)
        for reader in readers[2:] + [replacement, replacement2]:
            reader.close()


class AcceptancePolicyTest(TestCase):
    def test_same_operation_full_conflict_and_independent_reader_refs(self):
        for close_second in (False, True):
            with self.subTest(close_second=close_second):
                store = RecordingStore(3)
                h = Histograms(
                    ["a"], [50],
                    exemplar_rules={"a": (10, 1)},
                    exemplar_capacity=3,
                    exemplar_store=store,
                )

                def keep(letter):
                    h.observe("a", 11, evidence=letter.encode())

                keep("A")
                keep("B")
                first = h.open_exemplars()
                second = h.open_exemplars()
                keep("C")
                keep("D")

                # Chosen fixed strategy: overwrite the earliest replaceable
                # sample, so C is replaced by D. Dropping D would also be a
                # legal policy, but this implementation documents overwrite.
                check = h.open_exemplars()
                self.assertEqual(
                    list(check),
                    [("a", 11, b"A"), ("a", 11, b"B"), ("a", 11, b"D")],
                )
                self.assertEqual(store.get_calls, [0, 1, 2])
                store.get_calls.clear()

                self.assertEqual(next(first), ("a", 11, b"A"))

                # The second reader still protects A, so a probe cannot reuse
                # slot 0; it reuses the unprotected newest slot 2 instead.
                keep("X")
                self.assertEqual(store.put_calls[-1], (2, b"X"))

                if close_second:
                    second.close()
                else:
                    self.assertEqual(next(second), ("a", 11, b"A"))

                keep("E")
                self.assertEqual(store.put_calls[-1], (0, b"E"))
                self.assertNotIn(3, [slot for slot, _ in store.put_calls])

                # A reader parked before B still reads the original B from
                # slot 1, despite E now occupying A's old slot.
                self.assertEqual(next(first), ("a", 11, b"B"))
                if close_second:
                    with self.assertRaises(StopIteration):
                        next(second)
                else:
                    self.assertEqual(next(second), ("a", 11, b"B"))
                    with self.assertRaises(StopIteration):
                        next(second)
                with self.assertRaises(StopIteration):
                    next(first)

                if close_second:
                    self.assertEqual(store.get_calls, [0, 1])
                else:
                    self.assertEqual(store.get_calls, [0, 0, 1, 1])

    def test_cross_operation_borrow_and_reserve(self):
        store = RecordingStore(3)
        h = Histograms(
            ["jia", "yi"], [50],
            exemplar_rules={"jia": (10, 1), "yi": (10, 1)},
            exemplar_capacity=3,
            exemplar_store=store,
        )

        def keep(op, evidence):
            h.observe(op, 11, evidence=evidence)

        keep("jia", b"j1")
        keep("jia", b"j2")
        keep("jia", b"j3")
        parked = h.open_exemplars()

        # All Jia samples are protected by the parked reader: Yi is counted
        # normally but cannot borrow any slot.
        keep("yi", b"y1")
        self.assertEqual(len(store.put_calls), 3)
        yi_row = {r["operation"]: r for r in h.snapshot()["series"]}["yi"]
        self.assertEqual(yi_row["count"], 1)

        # Reading the earliest Jia releases exactly that slot's protection.
        self.assertEqual(next(parked), ("jia", 11, b"j1"))
        keep("yi", b"y2")
        self.assertEqual(store.put_calls[-1], (0, b"y2"))

        current = h.open_exemplars()
        self.assertEqual(list(current), [
            ("jia", 11, b"j2"),
            ("jia", 11, b"j3"),
            ("yi", 11, b"y2"),
        ])

        # The parked reader still protects Jia's remaining two samples, so Jia's
        # new slow completion is counted but cannot replace anything.
        keep("jia", b"j4")
        self.assertEqual(len(store.put_calls), 4)
        rows = {r["operation"]: r for r in h.snapshot()["series"]}
        self.assertEqual(rows["yi"]["count"], 2)
        self.assertEqual(rows["jia"]["count"], 4)
        self.assertEqual(list(h.open_exemplars()), [
            ("jia", 11, b"j2"),
            ("jia", 11, b"j3"),
            ("yi", 11, b"y2"),
        ])

        # Even after all old Jia protections are gone, Jia may only evict its
        # own oldest sample, never Yi's sole reserved sample.
        parked.close()
        keep("jia", b"j5")
        self.assertEqual(store.put_calls[-1], (1, b"j5"))
        self.assertEqual(store.slots[0], b"y2")
        self.assertEqual(list(h.open_exemplars()), [
            ("jia", 11, b"j3"),
            ("yi", 11, b"y2"),
            ("jia", 11, b"j5"),
        ])
        rows = {r["operation"]: r for r in h.snapshot()["series"]}
        self.assertEqual(rows["jia"]["count"], 5)


class FakeClock:
    def __init__(self):
        self.us = 0

    def __call__(self):
        return self.us * 1000

    def advance(self, us):
        self.us += us


class AdmissionEvidenceTest(TestCase):
    def make_gate(self, staged):
        clock = FakeClock()
        store = RecordingStore(4)
        h = Histograms(
            ["a", "b"], [50],
            exemplar_rules={"a": (50, 1)},
            exemplar_capacity=4,
            exemplar_store=store,
        )
        policy = Policy(2, 1, 50, 1, 0)
        gate = Admission(
            h, {"a": policy, "b": policy},
            clock_ns=clock, staged=staged,
        )
        return h, store, clock, gate

    def test_plain_work_evidence_in_normal_and_staged_modes(self):
        for staged in (False, True):
            with self.subTest(staged=staged):
                h, store, clock, gate = self.make_gate(staged)

                with gate.work("a", evidence=b"equal-threshold"):
                    clock.advance(50)
                self.assertEqual(store.put_calls, [])

                with gate.work("a", evidence=b"slow-success"):
                    clock.advance(51)
                self.assertEqual([e for _, e in store.put_calls],
                                 [b"slow-success"])
                decision = gate.refresh()["a"]
                self.assertTrue(decision.applied)
                self.assertEqual(decision.limit, 1)

                with self.assertRaisesRegex(RuntimeError, "body failed"):
                    with gate.work("a", evidence=b"slow-exception"):
                        clock.advance(51)
                        raise RuntimeError("body failed")
                self.assertEqual([e for _, e in store.put_calls],
                                 [b"slow-success", b"slow-exception"])

                with gate.work("a", evidence=b"held"):
                    with self.assertRaises(Busy):
                        with gate.work("a", evidence=b"busy"):
                            self.fail("Busy work must not run")

                before = h.snapshot()
                with self.assertRaises(ValueError):
                    gate.work("a", evidence=b"")
                with self.assertRaises(ValueError):
                    with gate.work("a", evidence=b""):
                        self.fail("invalid evidence must not enter work")
                self.assertEqual(h.snapshot(), before)

                row = {r["operation"]: r for r in h.snapshot()["series"]}["a"]
                # equal, slow success, exceptional slow, held; Busy excluded.
                self.assertEqual(row["count"], 4)
                self.assertEqual(gate._operations["a"].active, 0)

    def test_flow_does_not_accept_evidence_parameter(self):
        _, _, _, gate = self.make_gate(staged=True)
        with self.assertRaises(TypeError):
            with gate.flow("a", "b", evidence=b"not-supported"):
                self.fail("flow must not gain an evidence parameter")


class MetricsDeltaAndConcurrencyTest(TestCase):
    def test_http_metrics_and_delta_omit_evidence_when_capacity_pinned(self):
        store = RecordingStore(2)
        h = Histograms(
            ["a"], [50],
            exemplar_rules={"a": (10, 1)},
            exemplar_capacity=2,
            exemplar_store=store,
        )
        meter = InProcessMeter(("old", "new"))
        exporter = DeltaExporter(
            h, "test-stream", ("old", "new"), "old",
            send=meter.send, fence=meter.fence,
        )

        h.observe("a", 11, evidence=b"secret-0")
        h.observe("a", 11, evidence=b"secret-1")
        pinned = h.open_exemplars()
        self.addCleanup(pinned.close)
        # Both slots are read-protected; these are counted but not retained.
        h.observe("a", 11, evidence=b"secret-2")
        h.observe("a", 11, evidence=b"secret-3")

        with local_scrape(h) as address:
            port = urlsplit(address).port
            conn = HTTPConnection("127.0.0.1", port, timeout=3)
            try:
                conn.request("GET", "/metrics")
                with conn.getresponse() as response:
                    raw = response.read()
                    self.assertEqual(response.status, 200)
            finally:
                conn.close()

        scrape = json.loads(raw)
        self.assertEqual(scrape, h.snapshot())
        self.assertEqual(scrape["series"][0]["count"], 4)
        self.assertNotIn(b"secret", raw)

        self.assertEqual(exporter.step(), "advanced")
        delta = meter.calls[0]["delta"]
        self.assertEqual(delta["series"][0]["count"], 4)
        self.assertNotIn("secret", repr(delta))
        pinned.close()

    def test_paused_store_get_does_not_block_observation_or_snapshot(self):
        store = PausableGetStore(1)
        h = Histograms(
            ["a"], [50],
            exemplar_rules={"a": (10, 1)},
            exemplar_capacity=1,
            exemplar_store=store,
        )
        h.observe("a", 11, evidence=b"A")
        reader = h.open_exemplars()

        with ThreadPoolExecutor(max_workers=2) as pool:
            read_future = pool.submit(next, reader)
            self.assertTrue(store.get_entered.wait(3))

            observe_future = pool.submit(
                lambda: h.observe("a", 11, evidence=b"B"))
            observe_future.result(2)
            snapshot = pool.submit(h.snapshot).result(2)
            self.assertEqual(snapshot["series"][0]["count"], 2)

            store.release_get.set()
            self.assertEqual(read_future.result(2), ("a", 11, b"A"))

        # The in-flight read released its protection after returning; the next
        # sample can now reuse slot zero.
        h.observe("a", 11, evidence=b"C")
        self.assertEqual(store.put_calls[-1], (0, b"C"))
        self.assertEqual(h.snapshot()["series"][0]["count"], 3)
