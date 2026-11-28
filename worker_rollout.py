"""在线换代的异构执行器接入层。

``WorkerRollout`` 包在一个真实、非分段的 ``Admission`` 之外：准入档位仍由
``Admission`` 裁决，本层只决定一项获准工作该落到哪台就绪、空闲且兼容的执行器，
并在运维 ``step()`` 推进换代时守住实体上限、设备独占与保障名额。所有状态都在
进程内，不增加真实设备、后台调度或外部服务。
"""
from contextlib import contextmanager
from threading import RLock

from admission import Admission, Busy

READY = "ready"
DRAINING = "draining"
OFFLINE = "offline"

_MAX_WORKERS = 8
_MAX_STRATEGY_OPERATIONS = 4
_MAX_NAME = 64
_MAX_FLOOR_PER_OP = 8
_MAX_FLOOR_TOTAL = 8


def _real_int(value):
    # bool 是 int 的子类，容量与名额配置一律不接受。
    return type(value) is int


def _valid_name(value):
    return isinstance(value, str) and 1 <= len(value) <= _MAX_NAME


class WorkerRollout:
    """执行器换代控制器与应用接件入口。

    执行器处于三种独立状态之一：

    - ``ready``：已装载、可接新工作；
    - ``draining``：仍装载（占实体与设备容量、仍算保障结构容量），但不再接新工作；
    - ``offline``：未装载，不占任何容量。
    """

    def __init__(self, admission, specs, initial, *, capacity, floor, start, stop):
        if not isinstance(admission, Admission) or admission._staged:
            raise ValueError("a real, non-staged Admission instance is required")
        if not hasattr(specs, "items"):
            raise ValueError("specs must be a mapping of name to (device, operations)")
        specs = dict(specs)
        if not 1 <= len(specs) <= _MAX_WORKERS:
            raise ValueError("specs must contain 1 to 8 executor entries")
        devices = {}
        operations = {}
        strategy_ops = set()
        for name, spec in specs.items():
            if not _valid_name(name):
                raise ValueError("executor names must be 1..64 character strings")
            if not isinstance(spec, tuple) or len(spec) != 2:
                raise ValueError(f"spec for {name!r} must be a (device, operations) tuple")
            device, ops = spec
            if not _valid_name(device):
                raise ValueError(
                    f"device name for {name!r} must be a 1..64 character string")
            if not isinstance(ops, (set, frozenset)) or not ops:
                raise ValueError(f"operations for {name!r} must be a nonempty set")
            if any(not _valid_name(op) for op in ops):
                raise ValueError(f"operation names for {name!r} must be nonempty strings")
            unknown = set(ops).difference(admission._operations)
            if unknown:
                raise ValueError(
                    "operations not configured on the Admission gate: "
                    f"{sorted(unknown)!r}")
            devices[name] = device
            operations[name] = frozenset(ops)
            strategy_ops.update(ops)
        if not 1 <= len(strategy_ops) <= _MAX_STRATEGY_OPERATIONS:
            raise ValueError("specs must cover 1 to 4 distinct strategy operations")
        if not _real_int(capacity) or not 1 <= capacity <= _MAX_WORKERS:
            raise ValueError("capacity must be an integer from 1 to 8")
        if not hasattr(floor, "items"):
            raise ValueError("floor must be a mapping of operation to guaranteed slots")
        floor = dict(floor)
        if any(op not in strategy_ops for op in floor):
            raise ValueError("floor keys must be strategy operations declared in specs")
        if any(not _real_int(n) or not 1 <= n <= _MAX_FLOOR_PER_OP
               for n in floor.values()):
            raise ValueError("each floor entry must be a positive integer no greater than 8")
        if sum(floor.values()) > _MAX_FLOOR_TOTAL:
            raise ValueError("the sum of floor entries must not exceed 8")
        if not callable(start) or not callable(stop):
            raise ValueError("start and stop must be callable")
        try:
            initial_tuple = tuple(initial)
            ready = frozenset(initial_tuple)
        except TypeError as exc:
            raise ValueError("initial must be an iterable of executor names") from exc
        unknown = ready.difference(specs)
        if unknown:
            raise ValueError(f"unknown executors in initial: {sorted(unknown)!r}")
        if len(ready) != len(initial_tuple):
            raise ValueError("initial contains duplicate executors")
        if len(ready) > capacity:
            raise ValueError("initial executors exceed capacity")
        initial_devices = [devices[name] for name in ready]
        if len(set(initial_devices)) != len(initial_devices):
            raise ValueError("two initial executors share one device")

        self._admission = admission
        self._policy_ops = frozenset(admission._operations)
        self._specs = specs
        self._device = devices
        self._operations = operations
        self._capacity = capacity
        self._floor = floor
        self._start = start
        self._stop = stop
        self._lock = RLock()

        self._ready = ready
        self._draining = frozenset()
        self._used = frozenset()
        self._busy = set()
        self._target = None
        self._bridges = frozenset()
        self._active = False

        if not self._floor_feasible(self._ready):
            raise ValueError("initial ready set cannot satisfy the configured floor")

    # ------------------------------------------------------------------ 接件

    @contextmanager
    def work(self, operation):
        """准入一项工作并绑定到具体执行器，退出时由原 registry 记一次完成。"""
        if operation not in self._policy_ops:
            # 让真实准入器按原口径抛 KeyError：未知操作不占名额、不记完成。
            with self._admission.work(operation):
                pass
        with self._lock:
            chosen = None
            for name in sorted(self._ready):
                if operation in self._operations[name] and name not in self._busy:
                    chosen = name
                    break
            if chosen is not None:
                self._busy.add(chosen)
        if chosen is None:
            # 没有就绪、空闲且兼容的执行器：抛原 Busy，不进准入器、不留占用。
            raise Busy(operation)
        try:
            # 档位限制与完成记账完全沿用真实 Admission；锁不留在用户正文里。
            with self._admission.work(operation):
                yield chosen
        finally:
            with self._lock:
                self._busy.discard(chosen)

    # ------------------------------------------------------------------ 换代

    def request(self, target, *, bridge=()):
        """提交最终就绪集合与可选临时执行器；只校验与规划，不动设备。"""
        with self._lock:
            if self._active:
                raise RuntimeError("a rollout is already in progress")
            target_set = self._check_names(target, "target")
            bridge_set = self._check_names(bridge, "bridge")
            loaded = self._ready | self._draining
            overlap = bridge_set & (target_set | loaded)
            if overlap:
                raise ValueError(
                    "bridge executors must not be current or target: "
                    f"{sorted(overlap)!r}")
            if not self._has_path(
                    self._ready, self._draining, self._used, target_set, bridge_set):
                return False
            self._target = target_set
            self._bridges = bridge_set
            self._used = frozenset()
            self._active = True
            return True

    def step(self):
        """最多完成一个可观察动作，返回 progress/waiting/retry/done。"""
        with self._lock:
            if not self._active:
                return "done"
            if self._ready == self._target and not self._draining:
                self._active = False
                return "done"

            # 1) 先标排空：立刻停止新接件，但不中断在途工作，也不受在途工作阻止。
            for name in sorted(self._ready - self._target):
                next_ready = self._ready - {name}
                if not self._floor_feasible(next_ready):
                    continue
                next_draining = self._draining | {name}
                if not self._has_path(next_ready, next_draining, self._used,
                                      self._target, self._bridges):
                    continue
                self._ready = next_ready
                self._draining = next_draining
                return "progress"

            # 2) 再尝试装载目标或临时执行器；启动请求先要有实体空位和空闲设备。
            loaded = self._ready | self._draining
            candidates = (self._target | self._bridges) - loaded - self._used
            for name in sorted(candidates):
                if len(loaded) >= self._capacity:
                    break
                device = self._device[name]
                if any(self._device[other] == device for other in loaded):
                    continue
                next_ready = self._ready | {name}
                if not self._has_path(next_ready, self._draining, self._used,
                                      self._target, self._bridges):
                    continue
                if not self._start(name):
                    # 启动失败：未装载、可重试，本次不改变任何集合或接件资格。
                    return "retry"
                self._ready = next_ready
                return "progress"

            # 3) 最后卸载已排空且无在途工作的执行器。
            for name in sorted(self._draining):
                if name in self._busy:
                    continue
                next_draining = self._draining - {name}
                next_used = (self._used | {name}
                             if name in self._bridges else self._used)
                if not self._has_path(self._ready, next_draining, next_used,
                                      self._target, self._bridges):
                    continue
                self._stop(name)
                self._draining = next_draining
                self._used = next_used
                return "progress"

            # 剩下的推进只被在途工作（或其腾出的槽位）挡住，不能阻塞。
            return "waiting"

    def status(self):
        """返回每个执行器名到 ready/draining/offline 的独立字典。"""
        with self._lock:
            result = {}
            for name in self._specs:
                if name in self._ready:
                    result[name] = READY
                elif name in self._draining:
                    result[name] = DRAINING
                else:
                    result[name] = OFFLINE
            return result

    # ------------------------------------------------------------------ 内部

    def _check_names(self, names, label):
        try:
            names_tuple = tuple(names)
            result = frozenset(names_tuple)
        except TypeError as exc:
            raise ValueError(f"{label} must be an iterable of executor names") from exc
        unknown = result.difference(self._specs)
        if unknown:
            raise ValueError(f"unknown executors in {label}: {sorted(unknown)!r}")
        if len(result) != len(names_tuple):
            raise ValueError(f"{label} contains duplicate executors")
        return result

    def _floor_feasible(self, ready):
        """保障名额能否同时分到不同的、支持对应操作的就绪执行器上。

        正在处理工作的就绪执行器仍计入结构容量；排空中或未装载的不计入。
        """
        demands = []
        for operation, count in self._floor.items():
            demands.extend([operation] * count)

        def assign(index, used):
            if index == len(demands):
                return True
            operation = demands[index]
            for name in sorted(ready):
                if name not in used and operation in self._operations[name]:
                    used.add(name)
                    if assign(index + 1, used):
                        return True
                    used.remove(name)
            return False

        return assign(0, set())

    def _has_path(self, ready, draining, used, target, bridges):
        """离线搜索一条从当前状态到目标的安全换代路径。

        只允许装载目标或临时执行器：已退出的旧执行器永远不在候选动作里，
        因此通过本检查的每一步都不可能落入“只能恢复已退出旧执行器”的死路。
        每个装载、排空、卸载在整个搜索里至多发生一次，状态空间有限。
        """
        memo = set()

        def dfs(ready, draining, used):
            if not self._floor_feasible(ready):
                return False
            if ready == target and not draining:
                return True
            signature = (ready, draining, used)
            if signature in memo:
                return False
            loaded = ready | draining

            # 排空一个不再属于目标的就绪执行器。
            for name in sorted(ready - target):
                if dfs(ready - {name}, draining | {name}, used):
                    return True

            # 卸载一个已排空的执行器；临时执行器记入 used，不可再次装载。
            for name in sorted(draining):
                next_used = used | {name} if name in bridges else used
                if dfs(ready, draining - {name}, next_used):
                    return True

            # 装载目标或尚未使用过的临时执行器，须有空实体位与空闲设备。
            for name in sorted((target | bridges) - loaded - used):
                if len(loaded) >= self._capacity:
                    continue
                device = self._device[name]
                if any(self._device[other] == device for other in loaded):
                    continue
                if dfs(ready | {name}, draining, used):
                    return True

            memo.add(signature)
            return False

        return dfs(frozenset(ready), frozenset(draining), frozenset(used))
