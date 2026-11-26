# 延时采集与按操作准入

应用注册有限的操作名及递增的微秒桶界，用 `Histograms.observe(operation, duration_us)`
记录完成延时。挂载 `scrape_app(registry)` 后，GET /metrics 仍返回
`latency-histogram-v1`：所有操作在同一时点的累计快照。
`le_counts[i]` 包含恰好等于 `bounds_us[i]` 的样本；`count` 包含超过最大桶界的
样本，`sum_us` 是累计微秒。未知操作拒绝，不隐式扩张标签。

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
`test_metrics.py` 仍包含 GET /metrics 的真实本机 HTTP 回环测试。

实现限于单进程、固定策略、同一个持续累积的 registry；没有自动定时器、队列、
跨进程协调、重启恢复或 Prometheus 格式。调用方负责控制 `/metrics` 的访问范围。
