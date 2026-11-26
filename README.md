# 延时采集与按操作准入

应用注册有限的操作名及递增的微秒桶界，用 `Histograms.observe(operation, duration_us)`
记录完成延时。挂载 `scrape_app(registry)` 后，GET /metrics 仍返回
`latency-histogram-v1`：所有操作在同一时点的累计快照。
`le_counts[i]` 包含恰好等于 `bounds_us[i]` 的样本；`count` 包含超过最大桶界的
样本，`sum_us` 是累计微秒。未知操作拒绝，不隐式扩张标签。

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

实现限于单进程、固定策略、同一个持续累积的 registry；没有自动定时器、队列、
跨进程协调、重启恢复或 Prometheus 格式。调用方负责控制 `/metrics` 的访问范围。
