# 延时采集与按操作准入

应用注册有限的操作名及递增的微秒桶界，用 `Histograms.observe(operation, duration_us)`
记录完成延时。挂载 `scrape_app(registry)` 后，GET /metrics 仍返回
`latency-histogram-v1`：所有操作在同一时点的累计快照。
`le_counts[i]` 包含恰好等于 `bounds_us[i]` 的样本；`count` 包含超过最大桶界的
样本，`sum_us` 是累计微秒。未知操作拒绝，不隐式扩张标签。

## 诊断留样与值班读取

累计直方图仍不保存单个请求。需要现场片段时，可显式开启最多 64 个共享槽的诊断
留样；不传三个 exemplar 参数时行为和旧版本完全一致。片段必须是 `None` 或长度
1–4096 的 `bytes`，由应用脱敏后提交。采集器只保存操作名、延时和槽位元数据，
片段字节只通过应用提供的进程内 `exemplar_store.put(slot, evidence)` /
`get(slot)` 存取，不另藏副本、不落盘。

```python
from metrics import Histograms


class ListSlotStore:
    def __init__(self, capacity):
        self._slots = [None] * capacity

    def put(self, slot, evidence):
        self._slots[slot] = evidence       # 允许原位覆盖

    def get(self, slot):
        return self._slots[slot]


store = ListSlotStore(4)
registry = Histograms(
    ["checkout", "lookup"], [50, 100],
    exemplar_rules={
        # threshold_us 与桶界无关；只有 duration_us > threshold_us 才留样。
        "checkout": (200, 2),
        "lookup": (100, 1),
    },
    exemplar_capacity=4,                  # 所有 reserve 之和不能超过 4
    exemplar_store=store,
)

registry.observe("checkout", 200, evidence=b"equal-threshold-is-not-kept")
registry.observe("checkout", 201, evidence=b'{"request":"redacted"}')

reader = registry.open_exemplars()        # 未开启时抛 RuntimeError
try:
    for operation, duration_us, evidence in reader:
        print(operation, duration_us, evidence)
finally:
    reader.close()
```

`exemplar_rules` 必须是 1–32 个**已注册**操作到 `(threshold_us, reserve)` 的
映射；阈值和容量必须是整数（不接受 `bool`），阈值非负，`reserve >= 1`，保底总和
不超过容量。只提供部分 exemplar 配置、容量越界、保底超额或 store 缺少可调用的
`put/get` 都抛 `ValueError`。

`open_exemplars()` 只形成打开瞬间仍在槽中的有限视图，并按留样当时的先后排列；
打开时不调用 `get`，第一次/每次 `next(reader)` 才从真实槽位读取一条
`(operation, duration_us, evidence)`。打开后新留的样本不进入旧 reader，新
reader 才能看到当前槽位。读尽、关闭后再推进都会结束迭代；`close()` 可重复调用，
也可用 `with registry.open_exemplars() as reader:`。最多同时有 4 个仍有待读片段
的 reader；空视图不占额度，读尽或关闭后立即归还额度。

每个打开的 reader 独立保护自己尚未读到的槽位：只要任一 reader 还没读到该条，
该槽就不能覆盖。一次 `next()` 成功从 store 读回字节后，只释放当前 reader 对这
一条的保护；`close()` 释放它剩余的全部保护。因此值班员读完一条后，即使没有读尽
整个视图，这条槽也可能立刻重新参与留样。`store.get()` 在采集器锁外调用，缓慢
读取不会阻塞工作完成、`snapshot()`、`GET /metrics` 或增量导出；两次 `next()`
之间也不持有采集器锁。

### 写满时的固定策略

有空槽时，符合“有规则、有片段、延时严格大于阈值”的完成一定留样。槽满后：

- 本次操作当前数量低于自己的 `reserve`：只从当前数量**超过自身 reserve** 的其他
  操作借槽；在这些 donor 操作所有无读取保护的样本中覆盖留样最早的一条。donor
  的当前数量大于 reserve，所以借走一条后仍满足保底数量；没有无保护合格槽就不留
  本次。
- 本次操作已达到 reserve：不能占用其他操作的槽。本实现选择“覆盖自己最早的可覆盖
  样本”策略：跳过仍被 reader 保护的样本，覆盖本操作最早的无保护样本；如果本操作
  所有样本都仍有待读 reader，则放弃本次新现场。

选择后一种策略意味着压满时，值班员在新视图中会看不到那个“最早的可覆盖同操作旧
现场”，但能看到最新现场；若自己的整组样本都被暂停的 reader 保护，则会看不到
“本次最新现场”，而旧 reader 仍会完整走完打开时的 A/B 等内容。跨操作借还不会抢走
其他操作满足当前保底数量所需的槽位。放弃留样不影响正式直方图：合法完成仍恰好记
一次累计样本，`GET /metrics`、refresh、SLO 窗口和增量出口都继续使用原计数，且
输出中没有 evidence。

### 从 Admission.work 提交片段

普通 `gate.work(operation, evidence=...)` 接受同样的可选片段。非法片段在取得准入
名额前抛 `ValueError`；`Busy`、未进入的工作不记数也不写槽。已进入的工作无论正常
返回还是正文抛错，都在退出时把片段交给 `observe`，异常继续原样传播。两段式准入
下的普通 `work()` 行为相同；`flow()` 和 `ticket.work()` 不增加 evidence 参数，
首段/续段的原有计量行为不变。

```python
with gate.work("checkout", evidence=redacted_bytes):
    result = checkout(cart)
```

## SLO 窗口裁决与限额复核

`slo.py` 的 `evaluate_window(...)` 接收 1–32 个进程的 `(before, after)` 累计快照，
只做无状态的单窗口裁决。它使用桶增量、完成数和微秒和求每个进程“严格大于
`cutoff_us`”的可达最小/最大条数，不猜桶内分布，也不逐样本展开。百分比始终用
整数乘法比较；例如 4 条中 1 条超时正好是 25%，不算严格超标。

```python
from metrics import Histograms
from slo import evaluate_window


def snapshot_pair():
    h = Histograms(["checkout"], [10, 20])
    h.observe("checkout", 999)       # 旧累计样本，不属于本窗口
    before = h.snapshot()
    h.observe("checkout", 13)
    h.observe("checkout", 15)        # 两条都在 (10, 20]，窗口和为 28
    return before, h.snapshot()


slices = {"worker-a": snapshot_pair(), "worker-b": snapshot_pair()}
events = []


def notify(window_id, operation, action):
    events.append((window_id, operation, action))


def review(window_id, operation, cutoff_us, worker_ids):
    # worker_ids 是长度 1 或 2 的元组；这里向精细数据源批量查询。
    exact = {"worker-a": 0, "worker-b": 1}
    return {worker_id: exact[worker_id] for worker_id in worker_ids}


result = evaluate_window(
    "window-2026-11-26T10:00Z", "checkout", 15, 25, slices,
    review_budget=2,
    review=review,
    incident_open=True,   # 集成方自己保存当前告警状态
    notify=notify,
)
print(result)
# {'samples': 4, 'slow_min': 1, 'slow_max': 1,
#  'verdict': 'healthy', 'review_used': 2}
print(events)
# [('window-2026-11-26T10:00Z', 'checkout', 'resolve')]
```

本实现会按“复核前区间宽度降序、进程 ID 升序”排队，并在余额和候选都足够时一次
批送两台；余额为 1 或只剩一个候选时送一台。`review_used` 是实际发出的进程条数，
不是回调往返次数。回调可只返回本批部分进程的合法整数；缺项、越界值、布尔值、
非映射返回或 `OSError` 都不会把复核失败当成健康，相关进程本轮也不会重发。

输入 ID/长度、整数上界、桶界、快照可行性、累计倒退和窗口差量可行性都会在任何
`review`/`notify` 回调前核完；整轮不合法时抛 `ValueError`。零完成数固定为
`unknown`，不复核也不通知。`evaluate_window` 不写盘、不保存告警状态；是否已有
告警由集成方通过 `incident_open` 传入。

## 让延时观测影响实际准入

```python
from admission import Admission, Busy, Policy
from metrics import Histograms

registry = Histograms(["parse"], [50, 100])
gate = Admission(registry, {
    "parse": Policy(normal_limit=4, protected_limit=1, budget_us=50,
                    min_samples=8, max_slow_percent=25),
})

def handle(payload):
    try:
        with gate.work("parse"):
            return parse_document(payload)  # 调用方自己的业务函数
    except Busy:
        return busy_response()              # 调用方决定如何拒绝，例如 HTTP 429
```

- `work(operation)` 尝试立即占用该操作的一个并发名额。满额抛出 `Busy`，
  不执行请求、不记录延时。已获准工作无论成功还是抛错，都记录耗时并归还名额；
  缩小额度不会撤销已经获准的工作。
- 由应用按自己的节奏调用 `gate.refresh()`。每个操作从控制器创建时的计数起算，
  用新增样本数和预算桶的新增计数求超预算比例。只有达到 `min_samples` 才应用
  新决定：比例严格超过阈值则降至保护额度，否则恢复正常额度。预算必须恰好是
  一个已注册的桶界。空样本或样本不足保留当前额度，未消费样本留到下次。
- 管理采集和准入共用同一个本地 `Histograms`。准入更新不清空 `/metrics` 的累计
  值。实际应用通常用 `work` 作为这些操作唯一的观测入口；不要对同一请求再次调用
  `observe`。测量范围是 `with` 内的工作，不是排队时间或未在此作用域内完成的流式传输。

## 两段式请求：先声明续段，再开始首段

有些请求的首段会占着资源直到第二段提交完成（例如解码工作区要等下游提交结束才
释放），而且不同请求可能使用方向相反的操作组合。用 `Admission(..., staged=True)`
打开两段式准入，然后在进入首段前就把续段操作声明出来：

```python
from admission import Admission, Busy, Policy
from metrics import Histograms

registry = Histograms(["decode", "commit"], [50, 100])
gate = Admission(registry, {
    "decode": Policy(normal_limit=4, protected_limit=1, budget_us=50,
                     min_samples=8, max_slow_percent=25),
    "commit": Policy(normal_limit=4, protected_limit=1, budget_us=50,
                     min_samples=8, max_slow_percent=25),
}, staged=True)

def handle(payload):
    try:
        # 立即尝试首段；续段 commit 在进入前就已声明（一个“承诺”，不实占名额）。
        with gate.flow("decode", "commit") as ticket:
            document = decode_document(payload)   # 持有一个 decode 名额
            try:
                with ticket.work():                # 至多成功进入一次续段
                    commit_downstream(document)    # 内层只持有 commit 名额
            except Busy:
                return busy_retry_later()          # Busy 不消耗续段机会，可重试
            return ok_response()
    except Busy:
        return busy_response()                      # 首段被拒：不运行、不留承诺、不记样本
```

- 启用约束：`staged=True` 时要求 1–32 个已配置操作，且每个 `normal_limit` 不大于
  32。不启用时旧配置和旧行为完全不变，调用 `flow` 会抛 `RuntimeError`。两个阶段
  必须是不同的、都在策略内的操作，否则分别抛 `ValueError`、`KeyError`。
- `with gate.flow(first, next_op) as ticket` 立即占一个 `first` 名额，直到外层退出；
  同时对 `next_op` 记一个“承诺”。承诺不实占名额，但会进入保护档位的额度判断。
  `ticket.work()` 尝试占用 `next_op` 名额，成功后名额只在内层持有；退出内层不会
  提前释放首段。所有入口都不排队，暂时进不去就抛原有 `Busy`，对应 `with` 正文不执行。
- 每张票只能成功进入一次续段（续段正文抛错也算用过）；`Busy` 不消耗这次机会，外层
  仍打开时可以重试。对已用过或已结束的票再调 `ticket.work()` 抛 `RuntimeError`。
  票与创建它的线程绑定；同线程可以同时持有多个互不要求按序退出的请求作用域。跳过
  续段或首段抛错时，未使用的承诺被取消，名额照常归还。
- 记账沿用 `clock_ns`、微秒桶和累计 `/metrics` 格式：首段样本量的是 `first` 名额的
  整个持有时间（包含在内层执行续段的时间），续段只量自己的持有时间；只有成功进入
  的作用域离开时才记样本，异常离开也记，工作异常不会被吞掉，未进入的续段不记样本。

### 共享准入的三条规则

1. **延时保护。** 每个操作的 `normal_limit` 同时是不可突破的实占上限（普通 `work`、
   首段、已进入续段正在持有的名额）。某操作处于保护额度时，任何会增加它“承诺量”
   （实占加未进入续段数）的新 `work`/`flow`，只能在增加后不超过 `protected_limit`
   时进入；`flow` 对首段和续段目标各加一个承诺，两端都检查。降额不撤销已进入的
   工作或承诺：已接收的票兑现续段时只看目标实占硬上限，有空位就进，否则 `Busy`，
   不再重新接受降额审查。
2. **保留完成路径。** 新 `flow` 不能让已接收的一组请求陷入无法逐张完成的死锁；
   普通 `work` 或新 `flow` 也不能抢断眼下最后一个可立即兑现的续段机会（必须保证
   进入后至少还有一张此前可兑现的旧票仍能立即兑现，不能用新票或另一个恰好能结束
   的请求顶替）。
3. **按需使用空余量。** 在不违反前两条和硬上限时，只要本次入口涉及的操作进入后
   承诺量都不超过各自 `normal_limit`，就必须准入；只有某个涉及操作会超出正常额度
   时才允许提前返回 `Busy`。不相关操作的承诺量不能单独成为拒绝理由。拒绝不占名额、
   不留承诺、不产生样本。

## 延时增量外发与无重计换接

`delta_export.py` 的 `DeltaExporter` 与 `Histograms` 共用同一个真实 registry，但不清空累计值。构造时的 `snapshot()` 是外发起点；之后每次 `step()` 在没有待确认批时取一次新快照，把相对上次已确认边界的增量组成一批。`delta["le_counts"]` 是各累计 `le_counts` 计数器的差值，因此在本批内仍是累计桶数；等号样本和超过末桶的样本继续沿用 `Histograms` 口径。所有操作行（包括零增量行）都会保留。

进程内可直接使用 `InProcessMeter` 作为接收端；生产集成时把 `send`/`fence` 换成真实 RPC 回调即可：

```python
from delta_export import DeltaExporter, InProcessMeter
from metrics import Histograms

registry = Histograms(["checkout"], [100, 250])
meter = InProcessMeter(("old", "new"))
exporter = DeltaExporter(
    registry,
    stream_id="checkout-host-a",   # 每个 exporter 实例唯一
    sinks=("old", "new"),
    initial_sink="old",
    send=meter.send,
    fence=meter.fence,
)

registry.observe("checkout", 42)
exporter.step()                    # "advanced"：seq=1 已在 old 入账

# 运维提交换接；该调用本身不发 RPC。
exporter.switch_to("new")

registry.observe("checkout", 300)
exporter.step()                    # "advanced"：seq=2 直接进入 new
print(meter.totals("old"))         # {'checkout': {'count': 1, 'sum_us': 42}}
print(meter.totals("new"))         # {'checkout': {'count': 1, 'sum_us': 300}}
```

应由一个控制线程按固定节奏调用 `step()`。它每次最多调用一个外部回调，不睡眠、不内部循环：

- `"idle"`：没有待确认批，也没有新增完成量，本次不调用外端；
- `"pending"`：批仍未得到“已计入”的确定答复（包括 `TimeoutError`），下次继续推进同一 `seq`；
- `"advanced"`：本次确定了一个已入账批，registry 边界才前进到该批快照。

换接采用封边路径：若旧端结果不明，下一次推进对旧 `(stream_id, seq)` 调 `fence`。得到 `"applied"` 表示旧端已入账，该批留在旧端；得到 `"sealed"` 表示旧端保证永远不能再计入，随后才把**同一 `seq`、同一不可变 delta** 发给新端。`fence` 超时同样不提供证据，只会原样重试幂等 `fence`，不会猜测性地发新端。序号只属于非空批，换端和重送都不重置或另开序号。发送回调里可以运行业务代码并记录新样本；delta 在回调前已固定，这些新完成量只会进入下一批。`test_delta_export.py` 用真实 `Admission`/`Histograms`、可控时钟和可注入丢回复的进程内计量器覆盖了这一验收场景。

## 离线运行

Python 3.9+，仅标准库。两个演示都使用本仓自制数据和真实回环 HTTP：
第一个验证原采集接口；第二个用可控时钟重放操作耗时，不依赖等待实际毫秒数。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m pip check
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
.venv/bin/python admission_demo.py
```

原采集演示仍输出 `bounds_us=[10,50]`、`le_counts=[2,4]`、`count=5`、
`sum_us=123`。准入演示应输出：

```text
slow window: 2/3; parse limit=1; parse HTTP 429; lookup HTTP 200
empty window: limit=1; fresh decision=False
healthy window: 0/2; parse limit=2; parse HTTP 200; scrape parse count=6
```

`test_slo.py` 提供 SLO 区间、批量复核、通知和整轮校验的确定性测试；
`test_exemplars.py` 覆盖配置校验、同操作写满冲突、跨操作借还、四个 reader 额度、
暂停 `get()` 时采集不阻塞、普通/两段式 `Admission.work(..., evidence=...)`，以及
容量被读保护压满时的真实 `/metrics` 回环和增量导出；
`test_delta_export.py` 覆盖增量桶、丢回复、封边换接、连续序号和真实
`Admission` 两阶段完成时点；`test_metrics.py` 仍包含 GET /metrics 的真实本机
HTTP 回环测试。

实现限于单进程、固定策略、同一个持续累积的 registry；没有自动定时器、队列、
跨进程协调、重启恢复或 Prometheus 格式。增量导出同样只保存一个待确认批，
不保存历史批，也不提供进程重启后的序号/去重恢复。调用方负责控制 `/metrics`
的访问范围。
