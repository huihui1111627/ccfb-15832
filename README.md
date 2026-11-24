# 无人机机库拓扑调度器（Go 模拟）

一个零外部依赖的 Go 容器调度 Filter/Score 插件模拟：节点、机库拓扑域、工作负载、滚动版本、缩容和重平衡全部在进程内建模，并提供副本分布与调度明细页面。

## 运行

```bash
go run .
# 打开 http://localhost:18088
```

可选参数（当前机器的 8080/18080 可能被其他服务占用，因此演示默认使用 18088）：

```bash
go run . -addr 127.0.0.1:19090 -seed 20261125
```

## 核实

```bash
go test ./... -count=1
go test -race ./... -count=1
```

核心随机测试使用固定种子生成 **72 组集群拓扑和工作负载**。每组包含多个机库、每机库 1–3 个节点、随机 nodeSelector、资源水位、硬/软约束、`minDomains`、滚动版本和缩容。

每次调度前，测试都会：

1. 用生产代码 `PlanOne` 计算过滤、偏斜和打分；
2. 用测试中独立重写的暴力实现枚举全部节点；
3. 对比可行节点集合、基准值、`minDomains` 零基准、每个节点分数和最终选中节点；
4. 提交调度后检查所有硬约束和节点资源容量。

另外有定向测试覆盖：

- nodeSelector 排除的机库不参与最小副本数；
- 没有 Ready/Schedulable 节点的域不参与；
- 合格域少于 `minDomains` 时基准强制为 0；
- 滚动更新新旧版本分别计数；
- 软约束不过滤、只按偏斜打分；
- 缩容先选副本最多的域，平局按节点空闲度；
- 重平衡驱逐遵守 `available-1 >= minAvailable`；
- 机库停电后该域被排除，必要时触发 `minDomains` 零基准和驱逐保护；
- 批量顺序调度与逐次单独调度结果一致。

## 规则实现位置

| 需求 | 实现 |
| --- | --- |
| 放入后偏斜 = 当前域同版本副本数 + 1 - 合格域最小值 | `internal/scheduler/domain.go`、`scheduler.go` |
| selector 不匹配 / 无可用节点的域不参与 | `InspectDomains`、`SelectionFailures` |
| 合格域少于 `minDomains` 时最小值按 0 | `InspectDomains` |
| 硬约束 Filter，软约束 Score + 资源分加权 | `PlanOne` |
| 滚动更新只统计同一版本 | `InspectDomains` 的 `workloadID + version` 计数 |
| 缩容优先最大域，平局看资源空闲度 | `ScaleDownOne` |
| 后台重平衡驱逐并遵守 minAvailable | `EvictOne`、`RebalanceAll` |
| 批量逐个放置并更新计数 | `ScheduleBatch` |
| 固定种子随机拓扑 + 暴力核对 | `internal/scheduler/scheduler_test.go` |

## 打分与平局规则

资源空闲分：

```text
ResourceScore = round(100 * (CPUFreeRatio + MemoryFreeRatio) / 2, 2)
```

软拓扑偏斜分：

```text
SpreadScore = 100 * maxSkew / (maxSkew + postSkew)
```

加权总分：

```text
WeightedScore = SpreadScore * spreadWeight + ResourceScore * resourceWeight
```

硬约束不进入软打分；它只负责 Filter。总分相同时选择节点 ID 字典序最小的节点，保证模拟和暴力程序都可复现。

## “合格域”的口径

拓扑合格域要求至少有一个节点：

- 带有声明的拓扑标签（默认 `topology.kubernetes.io/zone`，值为机库 ID）；
- Ready 且 Schedulable；
- 匹配工作负载 nodeSelector。

当前 CPU/内存是否放得下属于独立的资源 Filter；资源已满但仍 Ready、匹配选择器的节点不会把整个拓扑域从偏斜统计中移除。这与调度框架中“拓扑域资格”和 `NodeResourcesFit` 分离的做法一致，也避免其他工作负载造成的瞬时资源水位悄悄改变拓扑基准。页面的每次调度明细仍会明确展示 `insufficient_cpu` / `insufficient_memory`。

## 页面内容

页面分为：

1. **各拓扑域里每个工作负载/版本的副本数、当前偏斜、maxSkew/minDomains 和满足状态**；
2. **每次调度的逐节点 Filter 原因、当前/放入后偏斜、资源分、拓扑软分、权重总分和选中原因**；
3. **缩容与重平衡驱逐记录**，包括来源域、偏斜前后、可用副本前后、minAvailable、补位节点和拦截原因；
4. 拓扑事件。

页面操作包括：重新生成固定种子剧本、新增机库、让指定机库停电/恢复、执行重平衡、选择工作负载版本缩容。

## HTTP API

```text
GET  /api/dashboard
POST /api/reset          {"seed":20261125}
POST /api/add-hangar     {}
POST /api/power-hangar   {"hangar":"h-07","on":false}
POST /api/rebalance      {}
POST /api/scale-down     {"workload_id":"route-planner","version":"v2.4"}
```

## 目录

```text
main.go                         启动 HTTP 服务
internal/model                  节点、工作负载、副本、审计记录
internal/scheduler              拓扑统计、Filter、Score、缩容、驱逐、不变量校验
internal/sim                    固定演示剧本、Dashboard DTO、机库事件
internal/web                    HTTP API 与内嵌单页页面
```
