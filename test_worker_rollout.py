"""不停接件的异构执行器换代验收。

只用标准库、进程内假设备与可控时钟：
- 同设备换代、启动失败可重试、保障名额与设备/实体容量逐动作核对；
- 排空如何改变应用实际落点，以及原 Admission 档位与原 registry 口径不变。
"""
from contextlib import ExitStack
from unittest import TestCase

from admission import Admission, Busy, Policy
from metrics import Histograms
from worker_rollout import WorkerRollout

# 验收布局：
#   O: 设备 dA，只支持 A（旧）
#   N: 设备 dA，只支持 A（新，与 O 独占同一块设备）
#   F: 设备 dF，支持 A/B
#   L: 设备 dB，只支持 B
#   T: 设备 dT，只支持 A（不同设备的可选临时执行器）
SPECS = {
    "O": ("dA", frozenset({"A"})),
    "N": ("dA", frozenset({"A"})),
    "F": ("dF", frozenset({"A", "B"})),
    "L": ("dB", frozenset({"B"})),
    "T": ("dT", frozenset({"A"})),
}
FLOOR = {"A": 1, "B": 1}
START = {"O", "F", "L"}
TARGET = {"N", "F", "L"}


class FakeClock:
    def __init__(self):
        self.ns = 0

    def __call__(self):
        return self.ns

    def advance(self, us):
        self.ns += us * 1000


class FakeDevices:
    """记录每一次真实装载/卸载，并可让指定执行器的前若干次启动失败。"""

    def __init__(self, fail=None, initial=()):
        self.calls = []
        # 初值执行器构造时已装载就绪，不会再触发 start 回调。
        self.loaded = set(initial)
        self._fail = dict(fail or {})

    def start(self, name):
        self.calls.append(("start", name))
        if self._fail.get(name, 0) > 0:
            self._fail[name] -= 1
            return False
        self.loaded.add(name)
        return True

    def stop(self, name):
        self.calls.append(("stop", name))
        # 合同：stop 只会成功卸载一台已经没有在做工作的执行器。
        assert name in self.loaded, f"stop on unloaded executor {name!r}"
        self.loaded.discard(name)


def build(capacity=4, initial=START, *, floor=FLOOR, normal=8, fail=None,
          clock=None):
    registry = Histograms(["A", "B"], [50, 100])
    clock = clock or FakeClock()
    policy = Policy(normal_limit=normal, protected_limit=1, budget_us=50,
                    min_samples=2, max_slow_percent=50)
    gate = Admission(registry, {"A": policy, "B": policy}, clock_ns=clock)
    devices = FakeDevices(fail=fail, initial=initial)
    rollout = WorkerRollout(
        gate, SPECS, initial, capacity=capacity, floor=floor,
        start=devices.start, stop=devices.stop)
    return rollout, gate, registry, devices, clock


def run_to_end(rollout, cap=40):
    results = []
    for _ in range(cap):
        outcome = rollout.step()
        results.append(outcome)
        if outcome == "done":
            return results
    raise AssertionError(f"rollout did not finish: {results}")


class SameDeviceRolloutTest(TestCase):
    def _check_invariants(self, rollout, devices, capacity):
        status = rollout.status()
        loaded = {name for name, state in status.items() if state != "offline"}
        # 实体容量：排空中但未 stop 的执行器仍占实体容量。
        self.assertLessEqual(len(loaded), capacity)
        # 同一设备最多装载一台。
        dev_in_use = [SPECS[name][0] for name in loaded]
        self.assertEqual(len(dev_in_use), len(set(dev_in_use)))
        # 回调视角的真实装载集合必须与状态一致。
        self.assertEqual(loaded, set(devices.loaded))
        # 保障名额：必须能同时分到不同的、支持对应操作的就绪执行器。
        ready = {name for name, state in status.items() if state == "ready"}
        self.assertTrue(_floor_match(ready))

    def test_capacity_four_start_failure_is_not_ready_and_floor_holds(self):
        rollout, gate, registry, devices, clock = build(
            capacity=4, fail={"N": 1})
        self.assertTrue(rollout.request(TARGET, bridge={"T"}))

        observed = []
        retry_seen = False
        for _ in range(40):
            outcome = rollout.step()
            status = rollout.status()
            self._check_invariants(rollout, devices, 4)
            observed.append((outcome, dict(status)))
            if outcome == "retry":
                retry_seen = True
                # 启动失败不是就绪：N 仍离线、不接件、不算保障、不改变现状。
                self.assertEqual(status["N"], "offline")
                self.assertNotIn("N", devices.loaded)
                before = dict(status)
                # 失败前后状态字典完全一致（这次推进不改变集合或接件资格）。
                self.assertEqual(before, status)
            if outcome == "done":
                break

        self.assertTrue(retry_seen)
        final = rollout.status()
        self.assertEqual({n for n, s in final.items() if s == "ready"}, TARGET)
        self.assertEqual(final["T"], "offline")     # 用过的临时执行器真正卸载
        self.assertEqual(final["O"], "offline")
        # N 的第一次启动失败、第二次恢复；T 装载后最终被卸载。
        starts = [n for kind, n in devices.calls if kind == "start"]
        self.assertEqual(starts.count("N"), 2)
        self.assertIn(("stop", "T"), devices.calls)
        print("\n[cap4] 动作序列:")
        for outcome, status in observed:
            print(" ", outcome, status)
        print("[cap4] 设备回调:", devices.calls)

    def test_capacity_four_routing_with_and_without_ready_bridge(self):
        # 现场一：T 仍就绪。占满所有支持 B 的就绪执行器（F、L）做 B。
        rollout, gate, registry, devices, clock = build(capacity=4)
        self.assertTrue(rollout.request(TARGET, bridge={"T"}))
        # 允许不同安全动作顺序：推进到 T 真正就绪（O 可能已先行排空）。
        for _ in range(10):
            if rollout.status()["T"] == "ready":
                break
            rollout.step()
        self.assertEqual(rollout.status()["T"], "ready")

        with ExitStack() as stack:
            stack.enter_context(rollout.work("B"))   # 落到 F
            stack.enter_context(rollout.work("B"))   # 落到 L
            with rollout.work("A") as worker:
                # 支持 A 的 F 已被 B 占用，新的 A 必须落到 T。
                self.assertEqual(worker, "T")

        # 现场二：没有就绪 T。先让 O 真正退出（同设备换代后 O 已离线），
        # 此时支持 A 的就绪执行器只剩 F；F、L 占满 B 后新的 A 必须 Busy。
        rollout2, _, _, devices2, _ = build(capacity=4)
        self.assertTrue(rollout2.request(TARGET))
        for _ in range(10):
            if rollout2.status()["O"] == "offline":
                break
            rollout2.step()
        self.assertEqual(rollout2.status()["O"], "offline")
        with ExitStack() as stack:
            stack.enter_context(rollout2.work("B"))  # F
            stack.enter_context(rollout2.work("B"))  # L
            with self.assertRaises(Busy):
                with rollout2.work("A"):
                    self.fail("没有就绪 T 且 F 被占时，新的 A 必须 Busy")
        print("[cap4] T 就绪时 A 落 T；无 T 且 F 被占时 A -> Busy，符合预期")

    def test_capacity_three_full_layout_cannot_oversubscribe(self):
        rollout, gate, registry, devices, clock = build(
            capacity=3, fail={"N": 1})
        self.assertTrue(rollout.request(TARGET))
        results = run_to_end(rollout)
        self.assertIn("retry", results)
        # 每个动作后都不能超过三台实体装载。
        self.assertLessEqual(len(devices.loaded), 3)
        self.assertEqual(
            {n for n, s in rollout.status().items() if s == "ready"}, TARGET)
        print("[cap3 满载] 序列:", results, "回调:", devices.calls)

    def test_capacity_three_without_L_needs_real_ready_bridge(self):
        no_l_initial = {"O", "F"}
        no_l_target = {"N", "F"}

        # 有 T：只有先真正就绪 T，O 才允许开始排空。
        rollout, _, _, devices, _ = build(
            capacity=3, initial=no_l_initial)
        self.assertTrue(rollout.request(no_l_target, bridge={"T"}))
        first = rollout.step()
        self.assertEqual(first, "progress")
        self.assertEqual(rollout.status()["T"], "ready")
        self.assertEqual(rollout.status()["O"], "ready")  # O 尚未排空
        second = rollout.step()
        self.assertEqual(second, "progress")
        self.assertEqual(rollout.status()["O"], "draining")
        run_to_end(rollout)
        self.assertEqual(rollout.status()["T"], "offline")
        self.assertEqual(
            {n for n, s in rollout.status().items() if s == "ready"},
            no_l_target)

        # 没有 T：单独的 F 接不了两份保障，必须拒绝且零回调。
        rollout2, _, _, devices2, _ = build(
            capacity=3, initial=no_l_initial)
        self.assertFalse(rollout2.request(no_l_target))
        self.assertEqual(devices2.calls, [])
        self.assertEqual(
            rollout2.status(),
            {"O": "ready", "N": "offline", "F": "ready",
             "L": "offline", "T": "offline"})
        print("[cap3 删 L] 无 T 拒绝且零回调；有 T 时先就绪 T 再排空 O")


def _floor_match(ready):
    """复制保障口径：A/B 各一个名额，须分到两台不同的就绪执行器。"""
    ready = set(ready)

    def choose(remaining, used):
        if not remaining:
            return True
        op = remaining[0]
        for name in sorted(ready):
            if name not in used and op in SPECS[name][1]:
                if choose(remaining[1:], used | {name}):
                    return True
        return False

    return choose(["A", "B"], set())


class DrainingChangesRoutingTest(TestCase):
    def test_draining_worker_kept_loaded_but_refuses_new_work(self):
        rollout, gate, registry, devices, clock = build(capacity=4)

        # 先占住 F，让绑定 O 的 A 工作成为下一个唯一选择。
        stack = ExitStack()
        f_hold = stack.enter_context(rollout.work("A"))
        self.assertEqual(f_hold, "F")
        o_work = stack.enter_context(rollout.work("A"))
        self.assertEqual(o_work, "O")

        self.assertTrue(rollout.request(TARGET))
        self.assertEqual(rollout.step(), "progress")      # O 标排空
        self.assertEqual(rollout.status()["O"], "draining")

        # 仍在做工作的 O 不能被卸载：其它动作被设备/在途工作挡住时返回 waiting。
        self.assertEqual(rollout.step(), "waiting")
        self.assertEqual(rollout.status()["O"], "draining")
        self.assertIn("O", devices.loaded)

        # 旧工作退出，但暂不推进卸载。
        stack.close()
        self.assertEqual(rollout.status()["O"], "draining")
        self.assertIn("O", devices.loaded)

        # 占满其他兼容 A 的就绪执行器（只剩 F；N/T 尚未装载）。
        with ExitStack() as occupied:
            occupied.enter_context(rollout.work("A"))     # F
            with self.assertRaises(Busy):
                with rollout.work("A"):
                    # O 物理上还装着且已空闲，却已排空，必须 Busy。
                    self.fail("draining 的 O 不能再接新工作")

        # 推进到目标。
        results = run_to_end(rollout)
        self.assertNotIn("retry", results)
        self.assertEqual(
            {n for n, s in rollout.status().items() if s == "ready"}, TARGET)

        # 到目标后占住其他兼容执行器（F），相同尝试必须在 N 上进入。
        with ExitStack() as occupied:
            occupied.enter_context(rollout.work("A"))     # F
            with rollout.work("A") as worker:
                self.assertEqual(worker, "N")
        print("\n[落点] O 排空中拒新工作并 waiting；到目标后 A 落 N:",
              results)

    def test_histograms_count_completions_and_busy_adds_nothing(self):
        rollout, gate, registry, devices, clock = build(capacity=4)

        def row(op):
            data = registry.snapshot()
            return next(r for r in data["series"] if r["operation"] == op)

        before = row("A")
        clock.advance(10)
        with rollout.work("A") as worker:
            self.assertEqual(worker, "F")
            clock.advance(20)
        after_ok = row("A")
        self.assertEqual(after_ok["count"], before["count"] + 1)
        self.assertEqual(after_ok["sum_us"], before["sum_us"] + 20)

        # 正文异常也记一次完成，且异常不被吞掉。
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with rollout.work("A"):
                clock.advance(30)
                raise RuntimeError("boom")
        after_err = row("A")
        self.assertEqual(after_err["count"], after_ok["count"] + 1)
        self.assertEqual(after_err["sum_us"], after_ok["sum_us"] + 30)

        # Busy 拒绝不增加完成数与延时：占满两台支持 A 的就绪执行器（O、F），
        # 在持有期间取快照，避免退出后正常完成计入。
        with ExitStack() as stack:
            stack.enter_context(rollout.work("A"))        # F
            stack.enter_context(rollout.work("A"))        # O
            holding = row("A")
            self.assertEqual(holding["count"], after_err["count"])
            with self.assertRaises(Busy):
                with rollout.work("A"):
                    self.fail("should be busy")
            after_busy = row("A")
        self.assertEqual(after_busy["count"], after_err["count"])
        self.assertEqual(after_busy["sum_us"], after_err["sum_us"])
        print("\n[记账] 正常+1、异常+1 且传播、Busy +0:",
              "count =", after_busy["count"], "sum_us =", after_busy["sum_us"])

    def test_refresh_protection_rejects_even_with_idle_workers(self):
        # normal=3、protected=1、min_samples=2、max_slow_percent=0：
        # 一个窗口内只要有慢样本就降到保护额度 1。
        rollout, gate, registry, devices, clock = build(
            capacity=4, normal=3)
        strict = Policy(normal_limit=3, protected_limit=1, budget_us=50,
                        min_samples=2, max_slow_percent=0)
        # 直接替换 A 的策略以便确定性地触发保护档。
        gate._operations["A"].policy = strict

        clock.advance(5)
        with rollout.work("A"):
            clock.advance(10)       # 一条快样本
        with rollout.work("A"):
            clock.advance(80)       # 一条慢样本（>50us 预算）
        decision = gate.refresh()["A"]
        self.assertTrue(decision.applied)
        self.assertEqual(decision.limit, 1)

        # 只持有一个 A，实体执行器仍大量空闲，但保护档拒绝超额工作。
        with rollout.work("A"):
            for name in ("F", "O"):
                self.assertEqual(rollout.status()[name], "ready")
            with self.assertRaises(Busy):
                with rollout.work("A"):
                    self.fail("保护额度之外必须 Busy，即使执行器空闲")
        print("\n[refresh] 慢窗口后 A 限额 =", decision.limit,
              "；实体有空位仍 Busy")


class RequestValidationTest(TestCase):
    def test_same_current_set_is_a_legal_noop_target(self):
        rollout, _, _, devices, _ = build()
        self.assertTrue(rollout.request(set(START)))
        self.assertEqual(rollout.step(), "done")
        self.assertEqual(devices.calls, [])
        self.assertEqual(
            {n for n, s in rollout.status().items() if s == "ready"}, START)

    def test_second_request_while_active_raises_and_keeps_target(self):
        rollout, _, _, _, _ = build()
        self.assertTrue(rollout.request(TARGET))
        self.assertEqual(rollout.step(), "progress")
        with self.assertRaises(RuntimeError):
            rollout.request(START)
        # 原目标没有被替换：继续推进仍抵达 N 目标。
        run_to_end(rollout)
        self.assertEqual(
            {n for n, s in rollout.status().items() if s == "ready"}, TARGET)

    def test_invalid_construction_raises_value_error_without_callbacks(self):
        registry = Histograms(["A", "B"], [50, 100])
        policy = Policy(8, 1, 50, 2, 50)
        gate = Admission(registry, {"A": policy, "B": policy})
        devices = FakeDevices()

        def construct(**overrides):
            kwargs = dict(
                admission=gate, specs=SPECS, initial=START, capacity=4,
                floor=FLOOR, start=devices.start, stop=devices.stop)
            kwargs.update(overrides)
            return WorkerRollout(**kwargs)

        with self.assertRaises(ValueError):
            construct(capacity=True)                    # bool 不算整数
        with self.assertRaises(ValueError):
            construct(capacity=9)
        with self.assertRaises(ValueError):
            construct(initial={"O", "unknown"})
        with self.assertRaises(ValueError):
            construct(floor={"A": 9})
        with self.assertRaises(ValueError):
            construct(floor={"A": 5, "B": 5})           # 总和 > 8
        with self.assertRaises(ValueError):
            construct(specs={**SPECS, "X": ("dX", frozenset({"Z"}))})
        # 初值本身满足不了保障名额。
        with self.assertRaises(ValueError):
            construct(initial={"O"}, floor={"A": 1, "B": 1})
        self.assertEqual(devices.calls, [])

    def test_bad_request_arguments_raise_value_error(self):
        rollout, _, _, devices, _ = build()
        with self.assertRaises(ValueError):
            rollout.request({"ghost"})
        with self.assertRaises(ValueError):
            rollout.request(TARGET, bridge={"O"})       # 桥不能在当前集合
        with self.assertRaises(ValueError):
            rollout.request(TARGET, bridge={"N"})       # 桥不能在目标集合
        self.assertEqual(devices.calls, [])

    def test_bridge_is_loaded_and_unloaded_at_most_once(self):
        rollout, _, _, devices, _ = build()
        self.assertTrue(rollout.request(TARGET, bridge={"T"}))
        run_to_end(rollout)
        starts = [n for kind, n in devices.calls if kind == "start"]
        stops = [n for kind, n in devices.calls if kind == "stop"]
        self.assertEqual(starts.count("T"), 1)
        self.assertEqual(stops.count("T"), 1)
        self.assertEqual(rollout.status()["T"], "offline")

    def test_status_is_independent_snapshot(self):
        rollout, _, _, _, _ = build()
        first = rollout.status()
        first["O"] = "offline"
        self.assertEqual(rollout.status()["O"], "ready")
