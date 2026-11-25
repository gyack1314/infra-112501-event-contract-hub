# 状态事件的契约协商与更正续投

一个仅依赖 Python 3.11 标准库的事件契约中间件。它把“生产端契约/修订”“当前有效转换”“接收方能力与确认位置”和“运营更正”放在同一个可持久化状态机中，而不是只生成一份转换样例。

## 它保证什么

- 事件按 `device + seq` 严格排序，同一 `seq` 的更高 `rev` 表示生产源更正；所有修订原件均保留。
- 每个 `subscriber + device` 有独立游标。某条事件无法可靠转换时，只阻塞该接收方对该设备的后续原件；其他接收方、其他设备继续消费。
- 选择版本时同时检查：
  - 接收方声明的 `schemas`；
  - `must_keep` 语义是否被目标契约和转换可靠保留；
  - `unknown_ok` 是否允许该语义显式为 `null` 或 enum `"unknown"`；
  - 转换的 `requires`、枚举映射、可空性和类型。
- 不会把未知值猜测成 `normal`/`warm` 等正常状态。源语义确实未知且接收方允许时，才输出 `null`、`"unknown"` 或交付信封中的 `unknown_semantics`。
- 转换注册后立即可用于未来交付；`disable` 后不能再用于新交付或新更正，但已交付记录中的规则快照和确认证据不变。
- 每条交付记录源契约、目标契约、转换编号及完整规则/契约快照，可回答“当时由哪份契约、哪条规则交付”。
- 已发送但未 ack 的交付保持同一 `delivery:N` 和同一正文；重试只返回原结果及更正关联，不会替换正文。
- 生产源修订已交付事件时，生成引用原交付的 `source_correction`。
- 运营可按转换编号和/或事件位置申请 `repair`，生成引用原业务事件、原修订、原交付和原转换的 `repair_correction`。转换更正默认针对**曾送达的原修订**，不会把后来的修订冒充新事件抢先投递。
- 接收方升级或新增兼容转换后，下一次 `poll` 从自己卡住的 `seq` 继续，并通过 `catchup.corrected_events` 报告阻塞期间哪些原件发生过修订。
- `inspect` 可按设备、接收方、事件位置、语义或交付号查询阻塞诊断、已外部确认的修订/转换，以及更正通知对象和状态。

## 运行检查

```sh
python3 trace_io.py data
python3 -m unittest -v
```

回放项目自带的两个演练（精简输出）：

```sh
python3 run_scenario.py data/unknown-before-upgrade.jsonl --brief
python3 run_scenario.py data/obsolete-in-flight.jsonl --brief
```

也可以用持久化 CLI 状态回放：

```sh
python3 -m contract_hub --state .contract_hub_state.json \
  replay data/unknown-before-upgrade.jsonl
```

## 实际项目操作

所有状态默认写入 `.contract_hub_state.json`，可用 `--state` 指定其他文件。

### 1. 生产端登记新旧契约

```sh
python3 -m contract_hub register-contract '{
  "schema":"state1",
  "fields":{"temp":{"type":"int","nullable":true,"semantics":"cooling_measured"}}
}'

python3 -m contract_hub register-contract '{
  "schema":"state2",
  "fields":{
    "degrees":{"type":"int","nullable":false,"semantics":"cooling_measured"},
    "status":{"type":"enum","nullable":false,"semantics":"cooling_outcome"}
  }
}'
```

### 2. 发布方登记转换

```sh
python3 -m contract_hub register-transform '{
  "id":"to1",
  "source":"state2",
  "target":"state1",
  "operation":{"copy_fields":{"temp":"degrees"}},
  "requires":[],
  "loss":["cooling_outcome"]
}'
```

`requires` 支持字段事实和 `all/any/not` 组合，例如：

```json
[
  {"field": "quality", "in": ["measured", "good"]},
  {"all": [{"field": "degrees", "present": true}, {"field": "mode", "not_in": ["off"]}]}
]
```

字符串形式 `"field:status"` 或裸字段名表示该事实必须存在且不是 `null`/`"unknown"`。

### 3. 接收方登记能力，之后可变更版本

```sh
python3 -m contract_hub subscribe \
  --id lab --schemas state1 \
  --must-keep cooling_outcome --unknown-ok ""

# 升级为同时支持新旧版本；游标和历史不会重置
python3 -m contract_hub subscribe \
  --id lab --schemas state2,state1 \
  --must-keep cooling_outcome
```

旧调度端可以只声明自己需要的语义和未知范围：

```sh
python3 -m contract_hub subscribe \
  --id old --schemas state1 \
  --must-keep cooling_measured \
  --unknown-ok cooling_measured
```

### 4. 生产端发布事件或修订

```sh
python3 -m contract_hub publish \
  --device m1 --seq 1 --rev 1 --schema state2 \
  --body-json '{"degrees":8,"status":"unknown"}'

# 同一 device+seq 的更高 rev 是更正
python3 -m contract_hub publish \
  --device m1 --seq 1 --rev 2 --schema state2 \
  --body-json '{"degrees":6,"status":"cool"}'
```

### 5. 接收方领取、重试和确认

```sh
python3 -m contract_hub poll --id lab --device m1
python3 -m contract_hub ack --id lab --device m1 --delivery delivery:1
```

`poll` 的主要状态：

- `delivered`：返回 `delivery_id`、不可变 `body`、`provenance`；若未 ack 就再次 poll，会得到同一交付且 `retry=true`。
- `blocked`：返回 `semantic`/`code`、完整 `diagnosis` 和保留的 `original`。
- `correction_blocked`：更正尚无可可靠转换，更正请求和原因保留。
- `waiting` / `gap`：当前序号还没有原件或有序号缺口。

### 6. 运营停用错误转换并发送更正

```sh
python3 -m contract_hub disable rename-1 --reason "loaded 被错误映射为 running"

python3 -m contract_hub register-transform '{
  "id":"rename-2",
  "source":"state2",
  "target":"state1",
  "operation":{"map_values":{"action":{
    "field":"phase",
    "map":{"loaded":"ready","finished":"done"}
  }}},
  "requires":[],
  "loss":[]
}'

# 可按转换、device+seq 事件位置、delivery 或它们的组合申请更正
python3 -m contract_hub repair --transform rename-1 --device m2 --seq 1

# 原交付未 ack 时，重试仍得到 delivery:1 的原正文和 correction:1 关联；
# ack 原交付后，下一次 poll 得到新的 delivery:2 更正正文。
python3 -m contract_hub poll --id line --device m2
python3 -m contract_hub ack --id line --device m2 --delivery delivery:1
python3 -m contract_hub poll --id line --device m2
```

### 7. 运营定位阻塞、确认证据和更正通知

```sh
python3 -m contract_hub inspect --device m1 --seq 1
python3 -m contract_hub inspect --id line --device m2
python3 -m contract_hub inspect --semantic cooling_outcome
python3 -m contract_hub inspect --delivery delivery:1 --device m2
```

报告中重点字段：

- `event.revisions`：该事件保留的全部原件修订。
- `diagnoses`：每次阻塞的语义类别、候选版本失败原因和原件快照。
- `subscriptions.<id>.devices.<device>.deliveries`：交付正文、尝试次数、ack 状态和契约/转换溯源。
- `externally_acknowledged`：已被接收方 ack 的修订、交付号和转换编号。
- `corrections` / `correction_recipients`：更正引用了谁、通知状态是 `queued`、`delivered` 还是 `acked`。

也可以直接应用与 JSONL 相同的操作信封：

```sh
python3 -m contract_hub apply '{"type":"poll","data":{"id":"line","device":"m2"}}'
```

## Python API

```python
from contract_hub import ContractHub

hub = ContractHub(".contract_hub_state.json")  # 路径存在时自动读取
hub.apply({"type": "contract", "data": {...}})
hub.register_transform({"id": "t1", ...})
hub.upsert_subscriber({"id": "qc", "schemas": ["state2"], "must_keep": ["cooling_outcome"]})
hub.publish({"device": "m1", "seq": 1, "rev": 1, "schema": "state2", "body": {...}})

blocked = hub.poll({"id": "qc", "device": "m1"})
hub.acknowledge({"id": "qc", "device": "m1", "delivery": "delivery:1"})
hub.request_repair({"transform": "bad-rule", "device": "m1", "seq": 1})
report = hub.inspect({"device": "m1", "seq": 1})

hub.save()
```

常用方法也有近义别名：`register_subscriber`、`update_subscriber`、`fetch`、`ack`、`repair`。根目录的 `middleware.py`、`main.py`、`solution.py` 和 `event_contract_middleware.py` 提供常见导入/启动命名兼容。

## 资料事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 `timestamp`、事件名称 `type` 和对象 `data`。同一秒的行按文件顺序排列，时间从零起算。文件里的业务标识只用于建立输入之间的引用。

- **contract**：声明 schema，其 `fields` 定义字段类型、可空性和 semantics。语义 `cooling_measured` 和 `cooling_outcome` 不等价。enum 可以用 `unknown` 表示该语义确实未知。
- **transform**：登记转换 id、source、target、operation 和 requires。`copy_fields` 用于字段继承，`map_values` 为有输入枚举的映射，`requires` 表达依赖可判定事实。`loss` 表示该规则不保留的语义。
- **disable**：撤销转换规则的未来可用性。此前已经交付的使用证据保留。
- **publish**：发出 device、seq 对应的事件 rev，携带 schema、body。当同一位置收到更高 rev 时，属于同一事实更正。
- **subscriber**：接收方 id 表明支持版本、`must_keep` 中的语义及 `unknown_ok`。`unknown_ok` 允许的语义可以显式未知。
- **poll**：接收方对 device 获取下一次交付。
- **ack**：接收方确认某次提交标识；示例用 `delivery:N` 代表该接收方对该设备收到的第 N 个交付。
- **repair**：运营明确请求为受影响的既有交付产生更正，引用转换标识或该业务事件位置。
- **inspect**：按设备、接收方或事件位置查询。

资料文件：

- `data/unknown-before-upgrade.jsonl`
- `data/obsolete-in-flight.jsonl`

资料检查：

```sh
python3 trace_io.py data
python3 -m unittest -v
```

资料工具输出读取的文件、事件数量及首尾秒点。业务状态由本服务维护。

## 设计说明

- 交付编号 `delivery:N` 的作用域是“一个接收方对一个设备”，与资料示例一致。
- 更正本身也是一次新的交付，因此会得到新的 `delivery:N`；它通过 `corrects_delivery_id`、`corrects_rev` 和 `correction_id` 指向历史。
- `unknown_ok` 不是“允许把已知信息丢掉后随便写 unknown”。如果源语义已知但转换 `loss`，仍按 `semantic_lost` 阻塞；只有源值确实未知，或规则显式产生 `null`/`unknown` 且目标可表达未知时，才按接收方声明放行。
- 检查操作是只读的，不推进逻辑操作序号。

## 目录

- `contract_hub/core.py`：状态机、协商算法、严格顺序、修订和更正。
- `contract_hub/cli.py` / `__main__.py`：持久化命令行操作端。
- `run_scenario.py`：使用项目原有 `trace_io` 校验并回放演练。
- `tests/test_contract_hub.py`：覆盖两个资料场景及顺序、独立订阅、修订、确认、Fan-out 更正和 CLI 持久化。
- `data/`：项目资料与合成演练。
