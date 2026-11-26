"""Deterministic acceptance and edge-case tests for SLO adjudication."""

import copy
import unittest

from metrics import Histograms
from slo import evaluate_window


MAX_INT = (1 << 63) - 1


def histogram_pair(bounds, before_values, window_values, operation="op"):
    """Build a real paired latency-histogram-v1 snapshot pair."""
    registry = Histograms([operation], bounds)
    for value in before_values:
        registry.observe(operation, value)
    before = registry.snapshot()
    for value in window_values:
        registry.observe(operation, value)
    return before, registry.snapshot()


def snapshot(bounds, le_counts, count, sum_us, operation="op"):
    return {
        "format": "latency-histogram-v1",
        "bounds_us": list(bounds),
        "series": [{
            "operation": operation,
            "le_counts": list(le_counts),
            "count": count,
            "sum_us": sum_us,
        }],
    }


def zero_pair(bounds):
    empty = snapshot(bounds, [0] * len(bounds), 0, 0)
    return empty, copy.deepcopy(empty)


class Recorder:
    def __init__(self, answer=None, error=None):
        self.answer = answer if answer is not None else {}
        self.error = error
        self.calls = []

    def __call__(self, window_id, operation, cutoff_us, worker_ids):
        self.calls.append((window_id, operation, cutoff_us, tuple(worker_ids)))
        if self.error is not None:
            raise self.error
        if callable(self.answer):
            return self.answer(window_id, operation, cutoff_us, worker_ids)
        return self.answer


class NotifyRecorder:
    def __init__(self, side_effect=None):
        self.calls = []
        self.side_effect = side_effect

    def __call__(self, *args):
        self.calls.append(tuple(args))
        if self.side_effect is not None:
            raise self.side_effect


class AcceptanceTests(unittest.TestCase):
    def test_real_histograms_inclusive_boundaries_and_healthy_exact_threshold(self):
        slices = {
            "a": histogram_pair([10, 20], [100], [11, 12]),
            "b": histogram_pair([15], [1000], [15, 16]),
        }
        original = copy.deepcopy(slices)
        review = Recorder()
        notify = NotifyRecorder()

        result = evaluate_window(
            "window-1", "op", 15, 25, slices,
            review_budget=8, review=review, incident_open=False, notify=notify,
        )

        self.assertEqual(result, {
            "samples": 4,
            "slow_min": 1,
            "slow_max": 1,
            "verdict": "healthy",
            "review_used": 0,
        })
        self.assertEqual(slices, original)
        self.assertEqual(review.calls, [])
        self.assertEqual(notify.calls, [])

    def test_other_operation_rows_do_not_participate(self):
        registry = Histograms(["op", "ignored"], [15])
        for value in (0, 15, 16, 100):
            registry.observe("ignored", value)
        before = registry.snapshot()
        registry.observe("op", 15)
        registry.observe("op", 16)
        after = registry.snapshot()

        result = evaluate_window(
            "w", "op", 15, 50, {"a": (before, after)},
            review_budget=8, review=Recorder(), notify=NotifyRecorder(),
        )
        self.assertEqual(result["samples"], 2)
        self.assertEqual((result["slow_min"], result["slow_max"]), (1, 1))
        self.assertEqual(result["verdict"], "healthy")

    def test_sum_in_open_bucket_tightens_billion_sample_window(self):
        workers = 1_000_000_000

        # Bounds [10] makes the only populated interval 11..infinity.
        # cutoff=11 and one microsecond above the all-lower-bound sum means
        # exactly one sample is 12 and therefore strictly slower than 11.
        before, _ = zero_pair([10])
        after = snapshot([10], [0], workers, 11 * workers + 1)
        review = Recorder()
        result = evaluate_window(
            "billion-exact", "op", 11, 25, {"big": (before, after)},
            review_budget=8, review=review, notify=NotifyRecorder(),
        )
        self.assertEqual(result["samples"], workers)
        self.assertEqual((result["slow_min"], result["slow_max"]), (1, 1))
        self.assertEqual(result["verdict"], "healthy")
        self.assertEqual(result["review_used"], 0)
        self.assertEqual(review.calls, [])

        # With cutoff=100, the same compact representation and only 90 extra
        # us over bucket minima permits 0 or 1 slow sample, not 0..10^9.
        before, _ = zero_pair([10])
        after = snapshot([10], [0], workers, 11 * workers + 90)
        result = evaluate_window(
            "billion-tight", "op", 100, 25, {"big": (before, after)},
            review_budget=8, review=review, notify=NotifyRecorder(),
        )
        self.assertEqual((result["slow_min"], result["slow_max"]), (0, 1))
        self.assertEqual(result["verdict"], "healthy")

    def ambiguous_slices(self):
        # Two samples in (10, 20] per worker, with sum 28: each worker can
        # have zero slow samples (e.g. 14+14) or one (e.g. 12+16), but not two.
        return {"a": histogram_pair([10, 20], [], [13, 15]),
                "b": histogram_pair([10, 20], [], [13, 15])}

    def test_initial_aggregate_interval_is_zero_to_two(self):
        notify = NotifyRecorder()
        result = evaluate_window(
            "w", "op", 15, 25, self.ambiguous_slices(),
            review_budget=0, incident_open=True, notify=notify,
        )
        self.assertEqual((result["samples"], result["slow_min"], result["slow_max"]),
                         (4, 0, 2))
        self.assertEqual(result["verdict"], "unknown")
        self.assertEqual(result["review_used"], 0)
        self.assertEqual(notify.calls, [])

    def test_one_budget_valid_first_answer_resolves_open_incident(self):
        review = Recorder({"a": 0})
        notify = NotifyRecorder()
        result = evaluate_window(
            "w", "op", 15, 25, self.ambiguous_slices(),
            review_budget=1, review=review, incident_open=True, notify=notify,
        )
        self.assertEqual(result["verdict"], "healthy")
        self.assertEqual((result["slow_min"], result["slow_max"]), (0, 1))
        self.assertEqual(result["review_used"], 1)
        self.assertEqual(review.calls, [("w", "op", 15, ("a",))])
        self.assertEqual(notify.calls, [("w", "op", "resolve")])

    def test_out_of_range_and_boolean_answers_keep_unknown(self):
        for bad_value in (2, -1, True):
            with self.subTest(bad_value=bad_value):
                review = Recorder({"a": bad_value})
                notify = NotifyRecorder()
                result = evaluate_window(
                    "w", "op", 15, 25, self.ambiguous_slices(),
                    review_budget=1, review=review, incident_open=True, notify=notify,
                )
                self.assertEqual(result["verdict"], "unknown")
                self.assertEqual((result["slow_min"], result["slow_max"]), (0, 2))
                self.assertEqual(result["review_used"], 1)
                self.assertEqual(review.calls, [("w", "op", 15, ("a",))])
                self.assertEqual(notify.calls, [])

    def test_batch_one_invalid_and_second_valid_still_resolves(self):
        review = Recorder({"a": 2, "b": 0})
        notify = NotifyRecorder()
        result = evaluate_window(
            "w", "op", 15, 25, self.ambiguous_slices(),
            review_budget=2, review=review, incident_open=True, notify=notify,
        )
        self.assertEqual(result["verdict"], "healthy")
        self.assertEqual((result["slow_min"], result["slow_max"]), (0, 1))
        self.assertEqual(result["review_used"], 2)
        # Eager two-target batching uses one round trip but charges two items.
        self.assertEqual(review.calls, [("w", "op", 15, ("a", "b"))])
        self.assertEqual(notify.calls, [("w", "op", "resolve")])

    def test_batch_zero_and_one_settles_at_exact_threshold(self):
        review = Recorder({"a": 0, "b": 1})
        notify = NotifyRecorder()
        result = evaluate_window(
            "w", "op", 15, 25, self.ambiguous_slices(),
            review_budget=2, review=review, incident_open=True, notify=notify,
        )
        self.assertEqual(result["verdict"], "healthy")
        self.assertEqual(result["slow_max"], 1)
        self.assertEqual(result["review_used"], 2)
        self.assertEqual(len(review.calls), 1)
        self.assertEqual(review.calls[0][3], ("a", "b"))
        self.assertEqual(notify.calls, [("w", "op", "resolve")])

    def test_both_one_pages_exactly_once_and_reviews_both(self):
        review = Recorder({"a": 1, "b": 1})
        notify = NotifyRecorder()
        result = evaluate_window(
            "w", "op", 15, 25, self.ambiguous_slices(),
            review_budget=2, review=review, incident_open=False, notify=notify,
        )
        self.assertEqual(result["verdict"], "breach")
        self.assertEqual((result["slow_min"], result["slow_max"]), (2, 2))
        self.assertEqual(result["review_used"], 2)
        self.assertEqual(review.calls, [("w", "op", 15, ("a", "b"))])
        self.assertEqual(notify.calls, [("w", "op", "page")])

        # An already-open incident is not paged again.
        notify = NotifyRecorder()
        result = evaluate_window(
            "w", "op", 15, 25, self.ambiguous_slices(),
            review_budget=2, review=Recorder({"a": 1, "b": 1}),
            incident_open=True, notify=notify,
        )
        self.assertEqual(result["verdict"], "breach")
        self.assertEqual(notify.calls, [])

    def test_oserror_and_non_mapping_batch_charge_and_do_not_retry(self):
        for bad_answer in (OSError("source unavailable"), ["not", "a", "mapping"]):
            with self.subTest(bad_answer=type(bad_answer).__name__):
                if isinstance(bad_answer, OSError):
                    review = Recorder(error=bad_answer)
                else:
                    review = Recorder(bad_answer)
                notify = NotifyRecorder()
                result = evaluate_window(
                    "w", "op", 15, 25, self.ambiguous_slices(),
                    review_budget=2, review=review, incident_open=True, notify=notify,
                )
                self.assertEqual(result["verdict"], "unknown")
                self.assertEqual(result["review_used"], 2)
                self.assertEqual(len(review.calls), 1)
                self.assertEqual(review.calls[0][3], ("a", "b"))
                self.assertEqual(notify.calls, [])

    def test_failed_batch_is_not_resent_but_other_workers_can_continue(self):
        slices = dict(self.ambiguous_slices())
        slices["c"] = histogram_pair([10, 20], [], [13, 15])
        calls = []

        def review(window_id, operation, cutoff_us, worker_ids):
            calls.append(tuple(worker_ids))
            if worker_ids == ("a", "b"):
                raise OSError("batch source unavailable")
            return {"c": 0}

        result = evaluate_window(
            "w", "op", 15, 25, slices,
            review_budget=3, review=review, incident_open=True,
            notify=NotifyRecorder(),
        )
        self.assertEqual(result["verdict"], "unknown")
        self.assertEqual(result["review_used"], 3)
        self.assertEqual(calls, [("a", "b"), ("c",)])

    def test_notify_exception_is_propagated_without_retry(self):
        review = Recorder({"a": 1, "b": 1})
        notify = NotifyRecorder(RuntimeError("pager down"))
        with self.assertRaises(RuntimeError):
            evaluate_window(
                "w", "op", 15, 25, self.ambiguous_slices(),
                review_budget=2, review=review, incident_open=False, notify=notify,
            )
        self.assertEqual(len(review.calls), 1)
        self.assertEqual(len(notify.calls), 1)

    def test_review_order_is_width_descending_then_id_ascending(self):
        # Both workers use cutoff=13 in (10,20].  The wide worker has three
        # samples/sum 39 and interval 0..2 (width 2); the narrow worker has
        # two samples/sum 25 and interval 0..1 (width 1).
        wide_before = snapshot([10, 20], [0, 0], 0, 0)
        wide_after = snapshot([10, 20], [0, 3], 3, 39)
        narrow_before = snapshot([10, 20], [0, 0], 0, 0)
        narrow_after = snapshot([10, 20], [0, 2], 2, 25)
        slices = {
            "z-wide": (wide_before, wide_after),
            "a-narrow": (narrow_before, narrow_after),
        }
        review = Recorder({})
        result = evaluate_window(
            "w", "op", 13, 25, slices,
            review_budget=2, review=review, notify=NotifyRecorder(),
        )
        self.assertEqual((result["slow_min"], result["slow_max"]), (0, 3))
        self.assertEqual(result["verdict"], "unknown")
        self.assertEqual(review.calls[0][3], ("z-wide", "a-narrow"))

        tie_review = Recorder({})
        evaluate_window(
            "w", "op", 15, 25, self.ambiguous_slices(),
            review_budget=2, review=tie_review, notify=NotifyRecorder(),
        )
        self.assertEqual(tie_review.calls[0][3], ("a", "b"))

    def test_zero_completions_unknown_without_review_or_notify(self):
        empty = snapshot([10], [0], 0, 0)
        review = Recorder()
        notify = NotifyRecorder()
        result = evaluate_window(
            "w", "op", 15, 25, {"a": (empty, copy.deepcopy(empty))},
            review_budget=8, review=review, incident_open=True, notify=notify,
        )
        self.assertEqual(result, {
            "samples": 0, "slow_min": 0, "slow_max": 0,
            "verdict": "unknown", "review_used": 0,
        })
        self.assertEqual(review.calls, [])
        self.assertEqual(notify.calls, [])


class ValidationTests(unittest.TestCase):
    def invalid_delta_slices(self):
        # Each cumulative snapshot is feasible by itself:
        # before = one sample at 20 (sum 20); after = three samples with sum 41,
        # e.g. 11+11+19. The delta claims two (10,20] samples summing to 21,
        # although their minimum is 22.
        before = snapshot([10, 20], [0, 1], 1, 20)
        after = snapshot([10, 20], [0, 3], 3, 41)
        invalid_worker = (before, after)

        # This first worker would be a definite breach if validation were
        # skipped or evaluation short-circuited.
        empty = snapshot([15], [0], 0, 0)
        breach_after = snapshot([15], [0], 2, 32)
        breach_worker = (empty, breach_after)
        return {"aaa-first-breach": breach_worker, "zzz-invalid": invalid_worker}

    def test_infeasible_delta_rejects_whole_round_before_callbacks(self):
        review = Recorder()
        notify = NotifyRecorder()
        with self.assertRaises(ValueError):
            evaluate_window(
                "w", "op", 15, 25, self.invalid_delta_slices(),
                review_budget=8, review=review, incident_open=False, notify=notify,
            )
        self.assertEqual(review.calls, [])
        self.assertEqual(notify.calls, [])

    def test_missing_target_row_rejects(self):
        good = snapshot([10], [0], 0, 0)
        missing = snapshot([10], [0], 0, 0, operation="other")
        review, notify = Recorder(), NotifyRecorder()
        with self.assertRaises(ValueError):
            evaluate_window(
                "w", "op", 15, 25, {"a": (good, missing)},
                review=review, notify=notify,
            )
        self.assertEqual(review.calls, [])
        self.assertEqual(notify.calls, [])

    def test_changed_bounds_regression_and_bad_counts_reject(self):
        empty10 = snapshot([10], [0], 0, 0)
        empty20 = snapshot([20], [0], 0, 0)
        cases = {
            "changed bounds": {"a": (empty10, empty20)},
            "count regression": {
                "a": (snapshot([10], [1], 1, 5), snapshot([10], [1], 0, 5))
            },
            "le regression": {
                "a": (snapshot([10, 20], [2, 2], 2, 30),
                      snapshot([10, 20], [1, 2], 2, 30))
            },
            "le above count": {
                "a": (empty10, snapshot([10], [2], 1, 5))
            },
            "sum too small": {
                "a": (empty10, snapshot([10, 20], [0, 2], 2, 21))
            },
        }
        for name, slices in cases.items():
            with self.subTest(name=name):
                review, notify = Recorder(), NotifyRecorder()
                with self.assertRaises(ValueError):
                    evaluate_window(
                        "w", "op", 15, 25, slices,
                        review=review, notify=notify,
                    )
                self.assertEqual(review.calls, [])
                self.assertEqual(notify.calls, [])

    def test_scalar_constraints_and_booleans(self):
        slices = {"a": zero_pair([10])}
        valid_notify = NotifyRecorder()
        bad_calls = [
            lambda: evaluate_window("", "op", 1, 25, slices, notify=valid_notify),
            lambda: evaluate_window("w", "", 1, 25, slices, notify=valid_notify),
            lambda: evaluate_window("w", "op", -1, 25, slices, notify=valid_notify),
            lambda: evaluate_window("w", "op", MAX_INT + 1, 25, slices, notify=valid_notify),
            lambda: evaluate_window("w", "op", 1, True, slices, notify=valid_notify),
            lambda: evaluate_window("w", "op", 1, 101, slices, notify=valid_notify),
            lambda: evaluate_window("w", "op", 1, 25, slices, review_budget=True,
                                    notify=valid_notify),
            lambda: evaluate_window("w", "op", 1, 25, slices, review_budget=9,
                                    notify=valid_notify),
            lambda: evaluate_window("w", "op", 1, 25, slices, incident_open=1,
                                    notify=valid_notify),
        ]
        for index, bad_call in enumerate(bad_calls):
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    bad_call()

    def test_worker_and_slice_collection_limits(self):
        notify = NotifyRecorder()
        with self.assertRaises(ValueError):
            evaluate_window("w", "op", 1, 25, {}, notify=notify)
        with self.assertRaises(ValueError):
            evaluate_window("w", "op", 1, 25, [], notify=notify)

        too_many = {f"w{i:02d}": zero_pair([10]) for i in range(33)}
        with self.assertRaises(ValueError):
            evaluate_window("w", "op", 1, 25, too_many, notify=notify)

        bad_id = {"": zero_pair([10])}
        with self.assertRaises(ValueError):
            evaluate_window("w", "op", 1, 25, bad_id, notify=notify)

    def test_worker_window_completion_limit(self):
        before = snapshot([10], [0], 0, 0)
        after = snapshot([10], [0], 1_000_000_001, 11 * 1_000_000_001)
        with self.assertRaises(ValueError):
            evaluate_window(
                "w", "op", 15, 25, {"a": (before, after)},
                notify=NotifyRecorder(),
            )


class IntervalEndpointTests(unittest.TestCase):
    def test_known_endpoints(self):
        from slo import _slow_interval

        # Acceptance fixture: two (10,20] samples, sum 28, cutoff 15.
        self.assertEqual(
            _slow_interval((10, 20), [0, 2, 0], 28, 15), (0, 1)
        )
        # Sum 32 still permits one fast 15 and one slow 17, or two slow 16s.
        self.assertEqual(
            _slow_interval((10, 20), [0, 2, 0], 32, 15), (1, 2)
        )
        # Sum 36 forces both samples above 15 (16 + 20).
        self.assertEqual(
            _slow_interval((10, 20), [0, 2, 0], 36, 15), (2, 2)
        )
        # Equality at the cutoff is fast.
        self.assertEqual(
            _slow_interval((15,), [1, 0], 15, 15), (0, 0)
        )
        # Overflow starts at 16 for bound 15; both are fixed slow at cutoff 15.
        self.assertEqual(
            _slow_interval((15,), [0, 2], 40, 15), (2, 2)
        )
        # An all-slow +Inf sample can absorb excess sum, so it does not force
        # the separate straddling finite-bucket sample to be slow.
        self.assertEqual(
            _slow_interval((10, 20), [0, 1, 1], 1000, 15), (1, 2)
        )
        # In the +Inf mixed bucket, exceeding all fast capacity forces exactly
        # one slow sample when two slow samples would need 180 extra us.
        self.assertEqual(
            _slow_interval((10,), [0, 2], 22 + 179, 100), (1, 1)
        )


if __name__ == "__main__":
    unittest.main()
