# 服务延时采集端点

应用启动时注册有限的操作名及递增的微秒桶界，随后用
`observe(operation, duration_us)` 记录真实或测试请求。未知操作拒绝，
不会隐式扩张标签。挂载 `scrape_app(registry)` 后，采集端用 GET /metrics
拉取所有操作在同一时点的累计快照，无须读取日志或预生成报表。
接口是自定义 JSON `latency-histogram-v1`：`le_counts[i]` 为小于等于
`bounds_us[i]` 的总数，`count` 包含超过最大桶界的样本，`sum_us` 为累计微秒。

自制的五个观测经真实回环 HTTP 采集：

Python 3.9+，仅标准库。从仓库根目录运行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m pip check
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

响应格式为 `latency-histogram-v1`，示例中 `bounds_us=[10,50]`，
`parse` 的 `le_counts=[2,4]`、`count=5`、`sum_us=123`。
操作名按字典顺序输出，轻量端点只允许 GET /metrics。
计数自进程启动累计，未做跨进程合并、历史存储或 Prometheus 格式；
按应用的管理端点策略配置访问范围。
