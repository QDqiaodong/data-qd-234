"""Deterministic tests for cumulative-to-delta export and sink switching."""

from copy import deepcopy
from unittest import TestCase

from admission import Admission, Decision, Policy
from delta_export import DeltaExporter, InProcessMeter
from metrics import Histograms


STREAM = "stream-1"


class FakeClock:
    def __init__(self):
        self.ns = 0

    def __call__(self):
        return self.ns

    def advance(self, us):
        self.ns += us * 1000


class ScriptedMeter(InProcessMeter):
    """In-process meter with deterministic one-shot network faults."""

    def __init__(self, sinks):
        super().__init__(sinks)
        self.scripts = {}

    def schedule(self, method, sink, seq, *actions):
        self.scripts[(method, sink, seq)] = list(actions)

    def _fault(self, method, sink, seq):
        queue = self.scripts.get((method, sink, seq))
        if not queue:
            return None
        action = queue.pop(0)
        if not queue:
            del self.scripts[(method, sink, seq)]
        return action

    def send(self, sink, stream_id, seq, delta):
        self.calls.append({
            "method": "send",
            "sink": sink,
            "stream_id": stream_id,
            "seq": seq,
            "delta": deepcopy(delta),
        })
        fault = self._fault("send", sink, seq)
        if fault == "timeout_before":
            raise TimeoutError("request was lost before the meter applied it")

        key = (stream_id, seq)
        store = self.records[sink]
        record = store.get(key)
        if record is None:
            record = {"state": "applied", "delta": deepcopy(delta)}
            store[key] = record
        elif record["state"] == "applied" and record["delta"] != delta:
            raise AssertionError("same applied key was resent with different content")

        if fault == "timeout_after":
            raise TimeoutError("meter applied the batch but the reply was lost")
        return record["state"]

    def fence(self, sink, stream_id, seq):
        self.calls.append({
            "method": "fence",
            "sink": sink,
            "stream_id": stream_id,
            "seq": seq,
            "delta": None,
        })
        fault = self._fault("fence", sink, seq)
        if fault == "timeout_before":
            raise TimeoutError("fence request was lost before taking effect")

        key = (stream_id, seq)
        store = self.records[sink]
        record = store.get(key)
        if record is None:
            record = {"state": "sealed", "delta": None}
            store[key] = record

        if fault == "timeout_after":
            raise TimeoutError("fence took effect but its reply was lost")
        return record["state"]


def op_rows(delta):
    return {row["operation"]: row for row in delta["series"]}


def policy():
    return Policy(
        normal_limit=2,
        protected_limit=1,
        budget_us=10,
        min_samples=2,
        max_slow_percent=50,
    )


class ConfigurationTest(TestCase):
    def setUp(self):
        self.registry = Histograms(["a", "b"], [10, 20])
        self.meter = ScriptedMeter(("old", "new"))

    def make_exporter(self, **overrides):
        kwargs = {
            "stream_id": STREAM,
            "sinks": ("old", "new"),
            "initial_sink": "old",
            "send": self.meter.send,
            "fence": self.meter.fence,
        }
        kwargs.update(overrides)
        return DeltaExporter(self.registry, kwargs.pop("stream_id"),
                             kwargs.pop("sinks"), kwargs.pop("initial_sink"),
                             **kwargs)

    def test_validates_text_names(self):
        for bad in ("", "x" * 129, 7, None):
            with self.subTest(stream_id=bad):
                with self.assertRaises(ValueError):
                    self.make_exporter(stream_id=bad)
            with self.subTest(sink=bad):
                with self.assertRaises(ValueError):
                    self.make_exporter(sinks=(bad, "new"))

    def test_validates_sinks_and_callbacks(self):
        with self.assertRaises(ValueError):
            self.make_exporter(sinks=("only",))
        with self.assertRaises(ValueError):
            self.make_exporter(sinks=("old", "new", "third"))
        with self.assertRaises(ValueError):
            self.make_exporter(sinks=("same", "same"))
        with self.assertRaises(ValueError):
            self.make_exporter(sinks="old")
        with self.assertRaises(ValueError):
            self.make_exporter(initial_sink="missing")
        with self.assertRaises(ValueError):
            self.make_exporter(send=None)
        with self.assertRaises(ValueError):
            self.make_exporter(fence=None)

    def test_supports_exporter_limits_without_changing_histograms(self):
        many_ops = Histograms([f"op{i}" for i in range(33)], [1])
        with self.assertRaises(ValueError):
            DeltaExporter(many_ops, STREAM, ("old", "new"), "old",
                          send=self.meter.send, fence=self.meter.fence)

        many_bounds = Histograms(["a"], list(range(65)))
        with self.assertRaises(ValueError):
            DeltaExporter(many_bounds, STREAM, ("old", "new"), "old",
                          send=self.meter.send, fence=self.meter.fence)

        # The original collector still accepts these configurations.
        self.assertEqual(len(many_ops.snapshot()["series"]), 33)
        self.assertEqual(len(many_bounds.snapshot()["bounds_us"]), 65)

    def test_switch_unknown_sink_is_value_error(self):
        exporter = self.make_exporter()
        with self.assertRaises(ValueError):
            exporter.switch_to("missing")

    def test_outstanding_switch_rejects_other_target(self):
        exporter = self.make_exporter()
        self.registry.observe("a", 1)
        self.meter.schedule("send", "old", 1, "timeout_before")
        self.assertEqual(exporter.step(), "pending")

        exporter.switch_to("new")
        exporter.switch_to("new")  # same desired target is a no-op
        with self.assertRaises(RuntimeError):
            exporter.switch_to("old")


class DeltaSemanticsTest(TestCase):
    def test_startup_snapshot_is_the_exclusion_boundary(self):
        registry = Histograms(["a", "b"], [10, 20])
        registry.observe("a", 999)  # history before exporter startup
        meter = ScriptedMeter(("old", "new"))
        exporter = DeltaExporter(registry, STREAM, ("old", "new"), "old",
                                 send=meter.send, fence=meter.fence)

        self.assertEqual(exporter.step(), "idle")
        self.assertEqual(meter.calls, [])

        registry.observe("a", 10)   # inclusive first bound
        registry.observe("b", 11)   # second finite bucket
        registry.observe("b", 25)   # +Inf bucket
        self.assertEqual(exporter.step(), "advanced")

        delta = meter.calls[0]["delta"]
        self.assertEqual(delta["bounds_us"], [10, 20])
        rows = op_rows(delta)
        self.assertEqual(set(rows), {"a", "b"})
        self.assertEqual(rows["a"], {
            "operation": "a", "le_counts": [1, 1], "count": 1, "sum_us": 10})
        self.assertEqual(rows["b"], {
            "operation": "b", "le_counts": [0, 1], "count": 2, "sum_us": 36})

    def test_zero_increment_operations_remain_present(self):
        registry = Histograms(["a", "b", "c"], [10])
        meter = ScriptedMeter(("old", "new"))
        exporter = DeltaExporter(registry, STREAM, ("old", "new"), "old",
                                 send=meter.send, fence=meter.fence)
        registry.observe("b", 1)
        exporter.step()
        rows = op_rows(meter.calls[0]["delta"])
        self.assertEqual(rows["a"]["count"], 0)
        self.assertEqual(rows["a"]["le_counts"], [0])
        self.assertEqual(rows["c"]["count"], 0)
        self.assertEqual(rows["b"]["le_counts"], [1])


class RetryAndIdleTest(TestCase):
    def test_unknown_send_retry_keeps_seq_and_immutable_delta(self):
        registry = Histograms(["a", "b"], [10, 20])
        meter = ScriptedMeter(("old", "new"))
        exporter = DeltaExporter(registry, STREAM, ("old", "new"), "old",
                                 send=meter.send, fence=meter.fence)

        registry.observe("a", 11)
        meter.schedule("send", "old", 1, "timeout_after")
        self.assertEqual(exporter.step(), "pending")

        # These completions arrive after the batch snapshot and cannot alter it.
        registry.observe("a", 12)
        registry.observe("b", 10)

        self.assertEqual(exporter.step(), "advanced")  # idempotent duplicate applied
        first_calls = [call for call in meter.calls if call["seq"] == 1]
        self.assertEqual(len(first_calls), 2)
        self.assertEqual(first_calls[0]["delta"], first_calls[1]["delta"])
        self.assertEqual(op_rows(first_calls[0]["delta"])["a"]["count"], 1)

        self.assertEqual(exporter.step(), "advanced")  # seq 2 contains new work
        second = [call for call in meter.calls if call["seq"] == 2][0]
        rows = op_rows(second["delta"])
        self.assertEqual(rows["a"]["count"], 1)
        self.assertEqual(rows["a"]["sum_us"], 12)
        self.assertEqual(rows["b"]["count"], 1)

        for _ in range(3):
            self.assertEqual(exporter.step(), "idle")
        self.assertEqual(len(meter.calls), 3)  # two seq-1 sends + one seq-2 send
        self.assertEqual(meter.totals("old")["a"], {"count": 2, "sum_us": 23})

    def test_empty_switch_numbering_and_drive_without_duplicates(self):
        registry = Histograms(["a"], [10, 20])
        meter = ScriptedMeter(("old", "new"))
        exporter = DeltaExporter(registry, STREAM, ("old", "new"), "old",
                                 send=meter.send, fence=meter.fence)

        self.assertEqual(exporter.step(), "idle")
        self.assertEqual(meter.calls, [])

        registry.observe("a", 1)
        self.assertEqual(exporter.step(), "advanced")
        exporter.switch_to("new")

        registry.observe("a", 2)
        self.assertEqual(exporter.step(), "advanced")
        self.assertEqual([c["seq"] for c in meter.calls], [1, 2])
        self.assertEqual([c["sink"] for c in meter.calls], ["old", "new"])

        for _ in range(5):
            self.assertEqual(exporter.step(), "idle")
        self.assertEqual(len(meter.calls), 2)
        self.assertEqual(meter.totals("old")["a"], {"count": 1, "sum_us": 1})
        self.assertEqual(meter.totals("new")["a"], {"count": 1, "sum_us": 2})

    def test_switch_inside_callback_and_callback_observation_are_isolated(self):
        registry = Histograms(["a"], [10])
        meter = ScriptedMeter(("old", "new"))
        exporter_holder = {}

        def send(sink, stream_id, seq, delta):
            # A control call can be driven re-entrantly by the same thread from
            # the application callback; no exporter lock is held across send.
            exporter_holder["exporter"].switch_to("new")
            registry.observe("a", 9)  # after this batch's snapshot boundary
            return meter.send(sink, stream_id, seq, delta)

        exporter = DeltaExporter(registry, STREAM, ("old", "new"), "old",
                                 send=send, fence=meter.fence)
        exporter_holder["exporter"] = exporter
        registry.observe("a", 1)

        self.assertEqual(exporter.step(), "advanced")
        self.assertEqual(exporter.target_sink, "new")
        self.assertEqual(meter.calls[0]["sink"], "old")
        self.assertEqual(meter.calls[0]["delta"]["series"][0]["sum_us"], 1)

        self.assertEqual(exporter.step(), "advanced")
        second = meter.calls[-1]
        self.assertEqual((second["sink"], second["seq"]), ("new", 2))
        self.assertEqual(second["delta"]["series"][0]["sum_us"], 9)

    def test_externally_sealed_current_sink_waits_for_switch(self):
        registry = Histograms(["a"], [10])
        meter = ScriptedMeter(("old", "new"))
        exporter = DeltaExporter(registry, STREAM, ("old", "new"), "old",
                                 send=meter.send, fence=meter.fence)
        registry.observe("a", 1)

        # An administrative/external fence permanently seals this old key.
        self.assertEqual(meter.fence("old", STREAM, 1), "sealed")
        self.assertEqual(exporter.step(), "pending")
        self.assertEqual(exporter.step(), "pending")  # no safe callback yet

        exporter.switch_to("new")
        self.assertEqual(exporter.step(), "advanced")
        self.assertEqual(meter.applied_keys("new"), {(STREAM, 1)})
        self.assertEqual(meter.applied_keys("old"), set())

    def test_rehomed_send_timeout_retries_same_sink_seq_and_content(self):
        registry = Histograms(["a"], [10])
        meter = ScriptedMeter(("old", "new"))
        exporter = DeltaExporter(registry, STREAM, ("old", "new"), "old",
                                 send=meter.send, fence=meter.fence)
        registry.observe("a", 1)
        meter.schedule("send", "old", 1, "timeout_before")
        self.assertEqual(exporter.step(), "pending")

        exporter.switch_to("new")
        self.assertEqual(exporter.step(), "pending")  # old fence seals key 1

        meter.schedule("send", "new", 1, "timeout_after")
        self.assertEqual(exporter.step(), "pending")  # applied, reply lost
        self.assertEqual(exporter.step(), "advanced")  # idempotent retry

        new_seq_one = [c for c in meter.calls
                       if c["sink"] == "new" and c["seq"] == 1]
        self.assertEqual(len(new_seq_one), 2)
        self.assertEqual(new_seq_one[0]["delta"], new_seq_one[1]["delta"])
        self.assertEqual(exporter.step(), "idle")
        self.assertEqual(len(meter.calls), 4)  # old send, fence, two new sends


class AcceptanceSwitchTest(TestCase):
    def _harness(self, stream):
        clock = FakeClock()
        registry = Histograms(["normal", "direct"], [10, 20])
        registry.observe("normal", 30)  # one historical sample before export

        gate = Admission(registry, {
            "normal": policy(),
            "direct": policy(),
        }, staged=True, clock_ns=clock)

        meter = ScriptedMeter(("old", "new"))
        ticket_holder = {}

        def send(sink, stream_id, seq, delta):
            # The callback synchronously completes the promised continuation.
            # It must not be cashed again on an exporter retry/rehome.
            if seq == 1 and not ticket_holder.get("cashed"):
                with ticket_holder["ticket"].work():
                    clock.advance(25)
                ticket_holder["cashed"] = True
            return meter.send(sink, stream_id, seq, delta)

        exporter = DeltaExporter(
            registry, stream, ("old", "new"), "old",
            send=send, fence=meter.fence,
        )
        return clock, registry, gate, meter, ticket_holder, exporter

    def _assert_first_delta(self, delta):
        rows = op_rows(delta)
        self.assertEqual(rows["normal"], {
            "operation": "normal",
            "le_counts": [0, 1],
            "count": 1,
            "sum_us": 11,
        })
        self.assertEqual(rows["direct"], {
            "operation": "direct",
            "le_counts": [1, 1],
            "count": 1,
            "sum_us": 10,
        })

    def _assert_second_delta(self, delta):
        rows = op_rows(delta)
        for operation in ("normal", "direct"):
            self.assertEqual(rows[operation], {
                "operation": operation,
                # 25us is above the final 20us bound: finite le buckets do not move.
                "le_counts": [0, 0],
                "count": 1,
                "sum_us": 25,
            })

    def test_batch_boundaries_refresh_and_clean_switch(self):
        clock, registry, gate, meter, holder, exporter = self._harness("happy")

        with gate.work("normal"):
            clock.advance(11)
        registry.observe("direct", 10)

        with gate.flow("normal", "direct") as ticket:
            holder["ticket"] = ticket
            self.assertEqual(exporter.step(), "advanced")
            self._assert_first_delta(meter.calls[0]["delta"])

            # Request the switch before the outer/first-stage sample exits.
            exporter.switch_to("new")

        # The first-stage 25us completion is recorded only now.  Together with
        # the continuation's 25us sample, it belongs to batch two.
        self.assertEqual(exporter.step(), "advanced")
        second_call = meter.calls[-1]
        self.assertEqual((second_call["sink"], second_call["seq"]), ("new", 2))
        self._assert_second_delta(second_call["delta"])

        self.assertEqual(registry.snapshot(), {
            "format": "latency-histogram-v1",
            "bounds_us": [10, 20],
            "series": [
                {"operation": "direct", "le_counts": [1, 1],
                 "count": 2, "sum_us": 35},
                {"operation": "normal", "le_counts": [0, 1],
                 "count": 3, "sum_us": 66},
            ],
        })
        self.assertEqual(gate.refresh(), {
            "direct": Decision(2, 1, 2, True),
            "normal": Decision(2, 2, 1, True),
        })

        self.assertEqual(meter.totals("old"), {
            "direct": {"count": 1, "sum_us": 10},
            "normal": {"count": 1, "sum_us": 11},
        })
        self.assertEqual(meter.totals("new"), {
            "direct": {"count": 1, "sum_us": 25},
            "normal": {"count": 1, "sum_us": 25},
        })

    def test_lost_replies_are_resolved_without_double_counting(self):
        for mode in ("applied_lost", "unapplied_lost"):
            with self.subTest(mode=mode):
                self._switch_failure_case(mode)

    def _switch_failure_case(self, mode):
        stream = "fault-" + mode
        clock, registry, gate, meter, holder, exporter = self._harness(stream)

        if mode == "applied_lost":
            # Meter effect happens, then the reply is lost.
            meter.schedule("send", "old", 1, "timeout_after")
        else:
            # Request/effect never reaches the meter.
            meter.schedule("send", "old", 1, "timeout_before")

        with gate.work("normal"):
            clock.advance(11)
        registry.observe("direct", 10)

        with gate.flow("normal", "direct") as ticket:
            holder["ticket"] = ticket
            self.assertEqual(exporter.step(), "pending")
            first_delta = deepcopy(meter.calls[0]["delta"])
            self._assert_first_delta(first_delta)

            exporter.switch_to("new")
            with self.assertRaises(RuntimeError):
                exporter.switch_to("old")

        # First old-end contact after switch times out without evidence.
        if mode == "unapplied_lost":
            meter.schedule("fence", "old", 1, "timeout_before", "timeout_after")
        else:
            meter.schedule("fence", "old", 1, "timeout_before")

        self.assertEqual(exporter.step(), "pending")
        self.assertEqual(meter.call_tuples("new"), [])

        if mode == "applied_lost":
            # Idempotent fence discovers that seq 1 is already applied.
            self.assertEqual(exporter.step(), "advanced")
            self.assertEqual(meter.call_tuples("old"), [
                ("send", "old", 1),
                ("fence", "old", 1),
                ("fence", "old", 1),
            ])
            self.assertEqual(meter.applied_keys("old"), {(stream, 1)})
            self.assertEqual(meter.call_tuples("new"), [])

            # Batch two is the first batch sent to the new sink.
            self.assertEqual(exporter.step(), "advanced")
            self.assertEqual(meter.call_tuples("new"), [("send", "new", 2)])
            self._assert_second_delta(
                [c for c in meter.calls if c["sink"] == "new"][0]["delta"])

            before = deepcopy(meter.totals("old"))
            late_answer = meter.send("old", stream, 1, first_delta)
            self.assertEqual(late_answer, "applied")
            self.assertEqual(meter.totals("old"), before)
            self.assertEqual(meter.applied_keys("new"), {(stream, 2)})

        else:
            # The scheduled effective-seal/reply-loss call has now happened.
            self.assertEqual(exporter.step(), "pending")
            self.assertEqual(meter.call_tuples("new"), [])

            # Retrying the idempotent fence gets the definitive sealed result.
            self.assertEqual(exporter.step(), "pending")
            old_record = meter.records["old"][(stream, 1)]
            self.assertEqual(old_record["state"], "sealed")

            # Same seq and immutable original delta move to the new sink.
            self.assertEqual(exporter.step(), "advanced")
            new_first = [c for c in meter.calls
                         if c["sink"] == "new" and c["seq"] == 1][0]
            self.assertEqual(new_first["delta"], first_delta)

            # The post-switch completions are not retroactively folded in.
            self.assertEqual(exporter.step(), "advanced")
            new_second = [c for c in meter.calls
                          if c["sink"] == "new" and c["seq"] == 2][0]
            self._assert_second_delta(new_second["delta"])

            self.assertEqual(meter.applied_keys("old"), set())
            self.assertEqual(meter.applied_keys("new"),
                             {(stream, 1), (stream, 2)})

            before_old = deepcopy(meter.totals("old"))
            before_new = deepcopy(meter.totals("new"))
            late_answer = meter.send("old", stream, 1, first_delta)
            self.assertEqual(late_answer, "sealed")
            self.assertEqual(meter.totals("old"), before_old)
            self.assertEqual(meter.totals("new"), before_new)

        # Continued driving is idle and makes no additional external calls.
        call_count = len(meter.calls)
        for _ in range(3):
            self.assertEqual(exporter.step(), "idle")
        self.assertEqual(len(meter.calls), call_count)

        # Cumulative registry and admission state were never cleared.
        snapshot = op_rows({"series": registry.snapshot()["series"]})
        self.assertEqual(snapshot["normal"]["count"], 3)
        self.assertEqual(snapshot["direct"]["count"], 2)
        decisions = gate.refresh()
        self.assertTrue(decisions["normal"].applied)
        self.assertTrue(decisions["direct"].applied)
