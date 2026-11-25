# 状态事件的契约协商与更正续投

状态流使用逻辑契约标识。事件样例只表示数据契约及合成动作，不连接设备。

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 timestamp、事件名称 type 和对象 data。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于建立输入之间的引用。所有场景均可在本地用合成角色重新运行。

- **contract**　声明 schema，其 fields 定义字段类型、可空性和 semantics。语义 cooling_measured 和 cooling_outcome 不等价。 enum 可以用 unknown 表示该语义确实未知。
- **transform**　登记转换 id、source、target、operation 和 requires。copy_fields 用于字段继承，map_values 为有输入枚举的映射，requires 表达依赖可判定事实。loss 表示该规则不保留的语义。
- **disable**　撤销转换规则的未来可用性。此前已经交付的使用证据保留。
- **publish**　发出 device、seq 对应的事件 rev，携带 schema、body。当同一位置收到更高 rev 时，属于同一事实更正。
- **subscriber**　接收方 id 表明支持版本、must_keep 中的语义及 unknown_ok。unknown_ok 允许的语义可以显式未知。
- **poll**　接收方对 device 获取下一次交付。
- **ack**　接收方确认某次提交标识，示例用 delivery:N 代表该接收方对该设备收到的第 N 个交付。
- **repair**　运营明确请求为受影响的既有交付产生更正，引用转换标识或该业务事件位置。
- **inspect**　按设备、接收方或事件位置查询。

## 资料文件

- unknown-before-upgrade.jsonl
- obsolete-in-flight.jsonl

## 本地资料检查

需 Python 3.11。资料工具使用标准库，无需联网。下列命令读取事件文件并检查封装格式。

```sh
python3 trace_io.py data
python3 -m unittest -v
```

资料工具输出读取的文件、事件数量及首尾秒点。业务状态由使用该事件契约的服务维护。
