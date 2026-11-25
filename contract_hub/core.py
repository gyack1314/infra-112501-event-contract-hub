"""核心契约协商与可靠续投引擎。

本模块只使用 Python 标准库。``ContractHub`` 同时是状态容器和领域操作入口；
状态可 JSON 序列化，也可通过 :meth:`ContractHub.load` / :meth:`ContractHub.save`
落到项目操作状态文件中。
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Iterable


class HubError(Exception):
    """所有可预期领域错误的基类。"""


class NotFoundError(HubError):
    """引用的契约、转换、接收方或交付不存在。"""


class ConflictError(HubError):
    """状态冲突，例如修订号冲突或确认了非当前交付。"""


class ContractValidationError(HubError):
    """操作信封或登记资料不合法。"""


_MISSING = object()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fresh_state() -> dict[str, Any]:
    """创建一份空的、可 JSON 持久化的中间件状态。"""

    return {
        "version": 1,
        "op_count": 0,
        "counters": {
            "delivery": 0,
            "correction": 0,
            "repair": 0,
            "diagnosis": 0,
        },
        "contracts": {},
        "transforms": {},
        "subscribers": {},
        "subscriptions": {},
        "devices": {},
        "diagnoses": {},
        "corrections": {},
        "repairs": {},
        "audit": [],
        "idempotency": {},
    }


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return [str(value)]


def _is_explicit_unknown(value: Any) -> bool:
    return value is None or value == "unknown"


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False)


class ContractHub:
    """事件契约中间件状态机。

    典型用法::

        hub = ContractHub()
        hub.apply({"type": "contract", "data": {...}})
        hub.apply({"type": "publish", "data": {...}})
        print(hub.poll({"id": "line", "device": "m2"}))
    """

    def __init__(self, state: dict[str, Any] | str | Path | None = None,
                 path: str | Path | None = None):
        if isinstance(state, (str, Path)) and path is None:
            path = Path(state)
            state = None
        self.path = Path(path) if path is not None else None
        if state is None and self.path is not None and self.path.exists():
            with self.path.open("r", encoding="utf-8") as source:
                state = json.load(source)
            if not isinstance(state, dict) or state.get("version") != 1:
                raise ContractValidationError("状态文件格式不受支持")
        self.state = state if state is not None else fresh_state()

    # ------------------------------------------------------------------
    # 持久化与操作入口
    # ------------------------------------------------------------------
    @classmethod
    def load(cls, path: str | Path) -> "ContractHub":
        path = Path(path)
        if not path.exists():
            return cls(fresh_state(), path=path)
        with path.open("r", encoding="utf-8") as source:
            state = json.load(source)
        if not isinstance(state, dict) or state.get("version") != 1:
            raise ContractValidationError("状态文件格式不受支持")
        return cls(state, path=path)

    def save(self, path: str | Path | None = None) -> Path:
        target = Path(path) if path is not None else self.path
        if target is None:
            raise HubError("未指定状态文件")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.tmp-{os.getpid()}")
        with temporary.open("w", encoding="utf-8") as output:
            json.dump(self.state, output, ensure_ascii=False, indent=2)
            output.write("\n")
        os.replace(temporary, target)
        self.path = target
        return target

    def _prepare(self, at: Any = None) -> tuple[Any, int]:
        self.state["op_count"] = int(self.state.get("op_count", 0)) + 1
        op_index = self.state["op_count"]
        if at is None:
            at = _now()
        return at, op_index

    def _audit(self, operation: str, data: dict[str, Any], result: dict[str, Any],
               at: Any, op_index: int) -> None:
        # 审计只保存摘要，避免完整诊断/原件在 audit 与业务表中重复过多。
        self.state["audit"].append({
            "op_index": op_index,
            "at": at,
            "operation": operation,
            "data": deepcopy(data),
            "result_summary": {
                k: v for k, v in result.items()
                if k in {"status", "id", "schema", "transform_id", "device", "seq",
                         "rev", "delivery_id", "repair_id", "diagnosis_id", "correction_id"}
            } if isinstance(result, dict) else {},
        })

    def apply(self, event: dict[str, Any]) -> dict[str, Any]:
        """应用一条 ``{"timestamp", "type", "data"}`` 演练/操作事件。"""

        if not isinstance(event, dict):
            raise ContractValidationError("事件必须是对象")
        operation = event.get("type") or event.get("operation")
        data = event.get("data", {})
        if not operation or not isinstance(data, dict):
            raise ContractValidationError("操作必须包含 type 和对象 data")
        at = event.get("timestamp", event.get("at"))

        handlers = {
            "contract": self.register_contract,
            "register_contract": self.register_contract,
            "transform": self.register_transform,
            "register_transform": self.register_transform,
            "disable": self.disable_transform,
            "disable_transform": self.disable_transform,
            "subscriber": self.upsert_subscriber,
            "register_subscriber": self.upsert_subscriber,
            "update_subscriber": self.upsert_subscriber,
            "publish": self.publish,
            "poll": self.poll,
            "ack": self.acknowledge,
            "acknowledge": self.acknowledge,
            "repair": self.request_repair,
            "inspect": self.inspect,
        }
        handler = handlers.get(operation)
        if handler is None:
            raise ContractValidationError(f"未知操作类型: {operation}")
        return handler(data, at=at)

    # ------------------------------------------------------------------
    # ID 和状态辅助
    # ------------------------------------------------------------------
    def _new_id(self, counter: str, prefix: str) -> str:
        counters = self.state.setdefault("counters", {})
        counters[counter] = int(counters.get(counter, 0)) + 1
        return f"{prefix}:{counters[counter]}"

    @staticmethod
    def _require(data: dict[str, Any], *names: str) -> Any:
        for name in names:
            if name in data and data[name] is not None:
                return data[name]
        raise ContractValidationError(f"缺少必填字段: {'/'.join(names)}")

    def _subscriber_id(self, data: dict[str, Any]) -> str:
        return str(self._require(data, "id", "subscriber_id", "subscriber"))

    def _device_id(self, data: dict[str, Any]) -> str:
        return str(self._require(data, "device", "device_id"))

    @staticmethod
    def _event_selector(data: dict[str, Any]) -> tuple[str | None, int | None]:
        """从 device/seq、event 对象或 ``m1#1`` 这类位置字符串解析事件位置。"""

        device = data.get("device") or data.get("device_id")
        seq = data.get("seq")
        event_ref = data.get("event") or data.get("location") or data.get("event_location")
        if isinstance(event_ref, dict):
            device = device or event_ref.get("device") or event_ref.get("device_id")
            seq = seq if seq is not None else event_ref.get("seq")
        elif isinstance(event_ref, str):
            text = event_ref
            for separator in ("#", ":", "/"):
                if separator in text:
                    maybe_device, maybe_seq = text.rsplit(separator, 1)
                    if maybe_seq.isdigit():
                        device = device or maybe_device
                        seq = seq if seq is not None else int(maybe_seq)
                    break
        return (str(device) if device is not None else None,
                int(seq) if seq is not None else None)

    def _sub_device_state(self, subscriber_id: str, device: str) -> dict[str, Any]:
        runtime = self.state["subscriptions"].setdefault(subscriber_id, {"devices": {}})
        return runtime["devices"].setdefault(device, {
            "next_seq": 1,
            "active_delivery_id": None,
            "delivery_counter": 0,
            "deliveries": {},
            "correction_queue": [],
            "block": None,
            "block_history": [],
        })

    def _seq_record(self, device: str, seq: int, create: bool = False) -> dict[str, Any] | None:
        device_record = self.state["devices"].get(device)
        if device_record is None:
            if not create:
                return None
            device_record = {"device": device, "seqs": {}}
            self.state["devices"][device] = device_record
        key = str(seq)
        record = device_record["seqs"].get(key)
        if record is None and create:
            record = {"device": device, "seq": seq, "revisions": []}
            device_record["seqs"][key] = record
        return record

    @staticmethod
    def _current_revision(seq_record: dict[str, Any]) -> dict[str, Any] | None:
        revisions = seq_record.get("revisions", [])
        return revisions[-1] if revisions else None

    # ------------------------------------------------------------------
    # 契约、转换和接收方登记
    # ------------------------------------------------------------------
    def register_contract(self, data: dict[str, Any], at: Any = None) -> dict[str, Any]:
        at, op_index = self._prepare(at)
        schema = str(self._require(data, "schema", "contract", "contract_id", "id"))
        fields = data.get("fields")
        if not isinstance(fields, dict):
            raise ContractValidationError("contract.fields 必须是对象")

        normalized_fields: dict[str, Any] = {}
        for field_name, definition in fields.items():
            if not isinstance(definition, dict):
                raise ContractValidationError(f"字段 {field_name} 定义必须是对象")
            normalized = deepcopy(definition)
            normalized["type"] = definition.get("type", "any")
            normalized["nullable"] = bool(definition.get("nullable", False))
            normalized_fields[field_name] = normalized

        contract = {
            "schema": schema,
            "fields": normalized_fields,
            "registered_at": at,
            "op_index": op_index,
        }
        existing = self.state["contracts"].get(schema)
        if existing is not None:
            if existing.get("fields") != normalized_fields:
                raise ConflictError(f"契约 {schema} 已以不同字段定义登记；契约编号不可原地改写")
            result = {"status": "unchanged", "schema": schema}
        else:
            self.state["contracts"][schema] = contract
            result = {"status": "registered", "schema": schema, "field_count": len(normalized_fields)}
        self._audit("contract", data, result, at, op_index)
        return result

    def register_transform(self, data: dict[str, Any], at: Any = None) -> dict[str, Any]:
        at, op_index = self._prepare(at)
        transform_id = str(self._require(data, "id", "transform_id"))
        source = str(self._require(data, "source", "source_contract"))
        target = str(self._require(data, "target", "target_contract"))
        operation = data.get("operation", {})
        if not isinstance(operation, dict):
            raise ContractValidationError("transform.operation 必须是对象")
        if not operation:
            raise ContractValidationError("transform.operation 不能为空")

        transform = {
            "id": transform_id,
            "source": source,
            "target": target,
            "operation": deepcopy(operation),
            "requires": deepcopy(data.get("requires", [])),
            "loss": _as_list(data.get("loss", [])),
            "status": "active",
            "registered_at": at,
            "op_index": op_index,
            "disabled_at": None,
            "disable_reason": None,
        }
        existing = self.state["transforms"].get(transform_id)
        if existing is not None:
            comparable = ("source", "target", "operation", "requires", "loss")
            if any(existing.get(k) != transform[k] for k in comparable):
                raise ConflictError(
                    f"转换 {transform_id} 已存在且规则不同；错误规则下架后应使用新的转换编号"
                )
            result = {"status": "unchanged", "transform_id": transform_id}
        else:
            self.state["transforms"][transform_id] = transform
            result = {"status": "registered", "transform_id": transform_id,
                      "source": source, "target": target}
        self._audit("transform", data, result, at, op_index)
        return result

    def disable_transform(self, data: dict[str, Any] | str, at: Any = None,
                          reason: str | None = None) -> dict[str, Any]:
        if isinstance(data, str):
            data = {"id": data, "reason": reason}
        at, op_index = self._prepare(at)
        transform_id = str(self._require(data, "id", "transform_id"))
        transform = self.state["transforms"].get(transform_id)
        if transform is None:
            raise NotFoundError(f"转换不存在: {transform_id}")
        if transform["status"] != "disabled":
            transform["status"] = "disabled"
            transform["disabled_at"] = at
            transform["disabled_op_index"] = op_index
            transform["disable_reason"] = data.get("reason", reason)
            status = "disabled"
        else:
            status = "already_disabled"
        result = {"status": status, "transform_id": transform_id,
                  "disabled_at": transform.get("disabled_at")}
        self._audit("disable", data, result, at, op_index)
        return result

    def upsert_subscriber(self, data: dict[str, Any], at: Any = None) -> dict[str, Any]:
        at, op_index = self._prepare(at)
        subscriber_id = self._subscriber_id(data)
        schemas = _as_list(self._require(
            data, "schemas", "versions", "supported_versions",
            "available_versions", "contracts",
        ))
        existing = self.state["subscribers"].get(subscriber_id)

        # 已存在接收方允许只变更 schemas；未显式携带的语义策略保持不变。
        if existing is not None:
            must_keep = _as_list(data.get(
                "must_keep", data.get("required_semantics", existing.get("must_keep", []))
            ))
            unknown_ok = _as_list(data.get(
                "unknown_ok", data.get("allowed_unknown", existing.get("unknown_ok", []))
            ))
        else:
            must_keep = _as_list(data.get("must_keep", data.get("required_semantics", [])))
            unknown_ok = _as_list(data.get("unknown_ok", data.get("allowed_unknown", [])))

        new_config = {
            "id": subscriber_id,
            "schemas": schemas,
            "must_keep": must_keep,
            "unknown_ok": unknown_ok,
            "updated_at": at,
            "op_index": op_index,
        }
        if existing is None:
            new_config["history"] = []
            self.state["subscribers"][subscriber_id] = new_config
            self.state["subscriptions"].setdefault(subscriber_id, {"devices": {}})
            status = "registered"
            changed = True
        else:
            before = {k: existing.get(k) for k in ("schemas", "must_keep", "unknown_ok")}
            after = {"schemas": schemas, "must_keep": must_keep, "unknown_ok": unknown_ok}
            changed = before != after
            if changed:
                existing.setdefault("history", []).append({"at": at, "op_index": op_index, "before": before, "after": after})
            existing.update(new_config)
            existing["history"] = existing.get("history", [])
            status = "updated" if changed else "unchanged"

        result = {"status": status, "id": subscriber_id, "changed": changed,
                  "schemas": schemas, "must_keep": must_keep, "unknown_ok": unknown_ok}
        self._audit("subscriber", data, result, at, op_index)
        return result

    # ------------------------------------------------------------------
    # 发布与生产源更正
    # ------------------------------------------------------------------
    def publish(self, data: dict[str, Any], at: Any = None) -> dict[str, Any]:
        at, op_index = self._prepare(at)
        device = self._device_id(data)
        seq = int(self._require(data, "seq", "sequence"))
        rev = int(self._require(data, "rev", "revision"))
        schema = str(self._require(data, "schema", "contract", "contract_id"))
        body = data.get("body")
        if not isinstance(body, dict):
            raise ContractValidationError("publish.body 必须是对象")
        if seq < 1 or rev < 1:
            raise ContractValidationError("seq 和 rev 必须为正整数")

        seq_record = self._seq_record(device, seq, create=True)
        assert seq_record is not None
        device_record = self.state["devices"][device]
        if device_record.get("first_seq") is None:
            device_record["first_seq"] = seq
            # 允许记录流不从 1 开始；尚未发生任何领取的订阅位置对齐到流首号。
            for runtime in self.state["subscriptions"].values():
                candidate_state = runtime.get("devices", {}).get(device)
                if (candidate_state and int(candidate_state.get("delivery_counter", 0)) == 0
                        and not candidate_state.get("active_delivery_id")
                        and not candidate_state.get("block")
                        and int(candidate_state.get("next_seq", 1)) == 1):
                    candidate_state["next_seq"] = seq
        revisions = seq_record["revisions"]

        for existing in revisions:
            if existing["rev"] == rev:
                if existing["schema"] == schema and existing["body"] == body:
                    result = {"status": "unchanged", "device": device, "seq": seq, "rev": rev}
                    self._audit("publish", data, result, at, op_index)
                    return result
                raise ConflictError(f"{device} seq={seq} rev={rev} 已存在且正文不同；修订号不可改写")

        previous = self._current_revision(seq_record)
        if previous is not None and rev <= int(previous["rev"]):
            raise ConflictError(
                f"{device} seq={seq} 的新修订号 {rev} 必须大于当前修订 {previous['rev']}"
            )

        revision = {
            "rev": rev,
            "schema": schema,
            "body": deepcopy(body),
            "published_at": at,
            "op_index": op_index,
        }
        revisions.append(revision)

        correction_ids: list[str] = []
        if previous is not None:
            correction_ids = self._create_source_corrections(
                device=device,
                seq=seq,
                old_rev=int(previous["rev"]),
                new_rev=rev,
                at=at,
            )

        result = {
            "status": "published",
            "device": device,
            "seq": seq,
            "rev": rev,
            "schema": schema,
            "source_corrections": correction_ids,
        }
        self._audit("publish", data, result, at, op_index)
        return result

    def _create_source_corrections(self, *, device: str, seq: int,
                                   old_rev: int, new_rev: int, at: Any) -> list[str]:
        ids: list[str] = []
        for subscriber_id, runtime in self.state["subscriptions"].items():
            device_state = runtime.get("devices", {}).get(device)
            if not device_state:
                continue
            for delivery_id, delivery in device_state.get("deliveries", {}).items():
                if delivery.get("kind") != "event":
                    continue
                if delivery.get("device") != device or delivery.get("seq") != seq:
                    continue
                if int(delivery.get("rev", -1)) != old_rev:
                    continue

                correction_id = self._new_id("correction", "correction")
                correction = {
                    "id": correction_id,
                    "kind": "source_revision",
                    "type": "source_revision",
                    "reason": "source_published_new_revision",
                    "subscriber_id": subscriber_id,
                    "device": device,
                    "seq": seq,
                    "original_delivery_id": delivery_id,
                    "original_rev": old_rev,
                    "target_rev": new_rev,
                    "from_rev": old_rev,
                    "to_rev": new_rev,
                    "requested_at": at,
                    "status": "queued",
                    "delivery_id": None,
                    "notifications": [{
                        "state": "queued",
                        "at": at,
                        "subscriber_id": subscriber_id,
                    }],
                }
                self.state["corrections"][correction_id] = correction
                device_state.setdefault("correction_queue", []).append(correction_id)
                delivery.setdefault("correction_ids", []).append(correction_id)
                ids.append(correction_id)
        return ids

    # ------------------------------------------------------------------
    # 契约检查与转换选择
    # ------------------------------------------------------------------
    @staticmethod
    def _semantic_index(contract: dict[str, Any] | None) -> dict[str, list[str]]:
        index: dict[str, list[str]] = {}
        if not contract:
            return index
        for field_name, definition in contract.get("fields", {}).items():
            semantic = definition.get("semantics")
            if semantic:
                index.setdefault(semantic, []).append(field_name)
        return index

    @staticmethod
    def _type_matches(value: Any, field_type: str) -> bool:
        if field_type in ("int", "integer"):
            return isinstance(value, int) and not isinstance(value, bool)
        if field_type in ("number", "float", "double"):
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if field_type in ("bool", "boolean"):
            return isinstance(value, bool)
        if field_type == "string" or field_type == "str":
            return isinstance(value, str)
        if field_type == "enum":
            return isinstance(value, str)
        if field_type in ("object", "dict"):
            return isinstance(value, dict)
        if field_type in ("array", "list"):
            return isinstance(value, list)
        return True

    def _validate_body(self, schema: str | None, body: dict[str, Any]) -> list[dict[str, Any]]:
        contract = self.state["contracts"].get(schema) if schema else None
        if contract is None:
            return [{"code": "unknown_contract", "schema": schema}]
        errors: list[dict[str, Any]] = []
        for field_name, definition in contract.get("fields", {}).items():
            present = field_name in body
            value = body.get(field_name)
            if not present or value is None:
                if value is None and definition.get("nullable"):
                    continue
                if not present and definition.get("nullable"):
                    continue
                errors.append({
                    "code": "missing_or_null_non_nullable_field",
                    "field": field_name,
                    "type": definition.get("type"),
                })
                continue
            if not self._type_matches(value, definition.get("type", "any")):
                errors.append({
                    "code": "field_type_mismatch",
                    "field": field_name,
                    "expected": definition.get("type"),
                    "actual": type(value).__name__,
                })
            allowed_values = definition.get("values") or definition.get("enum")
            if definition.get("type") == "enum" and allowed_values and value != "unknown":
                if value not in allowed_values:
                    errors.append({
                        "code": "enum_value_not_declared",
                        "field": field_name,
                        "value": value,
                        "allowed": allowed_values,
                    })
        return errors

    def _semantic_state(self, contract: dict[str, Any] | None, body: dict[str, Any],
                        semantic: str) -> tuple[str, str | None, Any]:
        if contract is None:
            return "unknown_contract", None, None
        fields = self._semantic_index(contract).get(semantic, [])
        if not fields:
            return "absent", None, None
        found_unknown: tuple[str, Any] | None = None
        found_missing: str | None = None
        for field_name in fields:
            definition = contract.get("fields", {}).get(field_name, {})
            if field_name not in body:
                if definition.get("nullable"):
                    found_unknown = (field_name, None)
                else:
                    found_missing = field_name
                continue
            value = body[field_name]
            if _is_explicit_unknown(value):
                found_unknown = (field_name, value)
            else:
                return "known", field_name, value
        if found_unknown is not None:
            return "unknown", found_unknown[0], found_unknown[1]
        if found_missing is not None:
            return "missing", found_missing, None
        return "absent", None, None

    def _eval_requires(self, requirements: Any, body: dict[str, Any],
                       schema: str) -> list[dict[str, Any]]:
        failures: list[dict[str, Any]] = []
        if isinstance(requirements, dict):
            requirements = [requirements]

        def evaluate_one(req: Any) -> tuple[bool, dict[str, Any]]:
            if isinstance(req, list):
                return all(evaluate_one(item)[0] for item in req), {"all": req}
            if not isinstance(req, dict):
                # 字符串条件的宽容语法："field:status" 或裸字段名表示该事实必须已知。
                if isinstance(req, str):
                    field_name = req.split(":", 1)[1] if req.startswith("field:") else req
                    value = body.get(field_name, _MISSING)
                    ok = value is not _MISSING and not _is_explicit_unknown(value)
                    return ok, {"field": field_name, "expected": "known", "actual": None if value is _MISSING else value}
                return False, {"requirement": req, "problem": "unsupported_requirement"}

            if "all" in req:
                results = [evaluate_one(item) for item in _as_list(req["all"])]
                failed = [detail for ok, detail in results if not ok]
                return not failed, {"all_failures": failed}
            if "any" in req:
                results = [evaluate_one(item) for item in _as_list(req["any"])]
                return any(ok for ok, _ in results), {"any": req["any"]}
            if "not" in req:
                ok, detail = evaluate_one(req["not"])
                return not ok, {"not": detail}

            field_name = str(req.get("field") or req.get("fact") or "")
            if not field_name:
                return False, {"requirement": req, "problem": "missing_field_in_requirement"}
            value = body.get(field_name, _MISSING)
            detail: dict[str, Any] = {"field": field_name, "actual": None if value is _MISSING else value}

            if "equals" in req or "value" in req:
                expected = req.get("equals", req.get("value"))
                detail["expected"] = expected
                return value == expected, detail
            if "not_equals" in req:
                expected = req["not_equals"]
                detail["not_equals"] = expected
                return value != expected, detail
            if "in" in req:
                allowed = list(req["in"])
                detail["in"] = allowed
                return value in allowed, detail
            if "not_in" in req:
                blocked = list(req["not_in"])
                detail["not_in"] = blocked
                return value not in blocked, detail
            if req.get("is_null") is True:
                return value is None, detail
            if req.get("is_null") is False:
                return value is not None, detail
            if req.get("known") is True or req.get("is_known") is True:
                return value is not _MISSING and not _is_explicit_unknown(value), detail
            if req.get("present") is True:
                return value is not _MISSING, detail

            # 没有指定判断符时，按“可判定且非 unknown”处理。
            return value is not _MISSING and not _is_explicit_unknown(value), detail

        for requirement in requirements or []:
            ok, detail = evaluate_one(requirement)
            if not ok:
                field_name = detail.get("field") if isinstance(detail, dict) else None
                semantic = None
                if field_name:
                    contract = self.state["contracts"].get(schema)
                    semantic = (contract or {}).get("fields", {}).get(field_name, {}).get("semantics")
                failures.append({
                    "code": "requirement_failed",
                    "requirement": requirement,
                    "detail": detail,
                    "field": field_name,
                    "semantic": semantic,
                })
        return failures

    def _find_paths(self, source: str, target: str) -> list[list[str]]:
        if source == target:
            return [[]]
        edges: dict[str, list[dict[str, Any]]] = {}
        for transform in self.state["transforms"].values():
            if transform.get("status") == "active":
                edges.setdefault(transform["source"], []).append(transform)
        for items in edges.values():
            items.sort(key=lambda tr: tr["id"])

        paths: list[list[str]] = []

        def walk(schema: str, path: list[str], seen: set[str]) -> None:
            if len(path) > 4:
                return
            for transform in edges.get(schema, []):
                tid = transform["id"]
                next_schema = transform["target"]
                if tid in path or next_schema in seen:
                    continue
                new_path = path + [tid]
                if next_schema == target:
                    paths.append(new_path)
                else:
                    walk(next_schema, new_path, seen | {next_schema})

        walk(source, [], {source})
        paths.sort(key=lambda path: (len(path), path))
        return paths

    def _apply_one_transform(
        self, transform: dict[str, Any], body: dict[str, Any],
        source_contract: dict[str, Any] | None = None,
        target_contract: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        output: dict[str, Any] = {}
        issues: list[dict[str, Any]] = []
        field_links: list[dict[str, Any]] = []
        operation = transform.get("operation", {})

        for target_field, source_field in operation.get("copy_fields", {}).items():
            if source_field not in body:
                source_definition = (source_contract or {}).get("fields", {}).get(source_field, {})
                if source_definition.get("nullable"):
                    output[target_field] = None
                    field_links.append({"target_field": target_field, "source_field": source_field,
                                        "kind": "copy", "source_unknown": True})
                    continue
                issues.append({"code": "missing_source_field", "transform_id": transform["id"],
                              "field": source_field, "target_field": target_field})
                continue
            value = body[source_field]
            output[target_field] = deepcopy(value)
            field_links.append({"target_field": target_field, "source_field": source_field,
                                "kind": "copy", "source_unknown": _is_explicit_unknown(value)})

        for target_field, specification in operation.get("map_values", {}).items():
            if not isinstance(specification, dict):
                issues.append({"code": "invalid_map_values", "target_field": target_field})
                continue
            source_field = str(specification.get("field", ""))
            mapping = specification.get("map", {})
            if source_field not in body:
                source_definition = (source_contract or {}).get("fields", {}).get(source_field, {})
                if source_definition.get("nullable"):
                    source_value = None
                else:
                    issues.append({"code": "missing_source_field", "transform_id": transform["id"],
                                  "field": source_field, "target_field": target_field})
                    continue
            else:
                source_value = body[source_field]
            mapped = _MISSING
            if source_value in mapping:
                mapped = mapping[source_value]
            elif isinstance(source_value, str) and source_value in mapping:
                mapped = mapping[source_value]
            elif str(source_value) in mapping:
                mapped = mapping[str(source_value)]
            elif source_value is None and "null" in mapping:
                mapped = mapping["null"]

            if mapped is _MISSING and "default" in specification:
                mapped = specification["default"]
            if mapped is _MISSING and _is_explicit_unknown(source_value):
                target_definition = (target_contract or {}).get("fields", {}).get(target_field, {})
                if target_definition.get("nullable"):
                    mapped = None
                elif target_definition.get("type", "enum") == "enum":
                    # README 约定：enum 可统一用 "unknown" 表达确实未知。
                    mapped = "unknown"
            if mapped is _MISSING:
                issues.append({
                    "code": "unmapped_value",
                    "transform_id": transform["id"],
                    "field": source_field,
                    "target_field": target_field,
                    "value": source_value,
                })
                continue

            source_unknown = _is_explicit_unknown(source_value)
            target_unknown = _is_explicit_unknown(mapped)
            if source_unknown and not target_unknown:
                issues.append({
                    "code": "unknown_promoted_to_known",
                    "transform_id": transform["id"],
                    "field": source_field,
                    "target_field": target_field,
                    "value": source_value,
                    "mapped_value": mapped,
                })
            output[target_field] = deepcopy(mapped)
            field_links.append({"target_field": target_field, "source_field": source_field,
                                "kind": "map", "source_unknown": source_unknown,
                                "target_unknown": target_unknown})

        supported = {"copy_fields", "map_values"}
        unsupported = set(operation) - supported
        for key in unsupported:
            issues.append({"code": "unsupported_operation", "operation": key,
                           "transform_id": transform["id"]})

        return output, issues, field_links

    def _semantic_reason(self, code: str, semantic: str, **extra: Any) -> dict[str, Any]:
        messages = {
            "semantic_lost": "必需语义在目标版本中没有可靠保留",
            "unknown_semantic_not_allowed": "该语义为显式未知，但接收方未允许此未知范围",
            "semantic_absent": "生产契约未声明该语义，不能构造可靠值",
            "unknown_not_representable": "目标契约没有可表达显式未知的可空/enum 字段",
            "missing_semantic_output": "转换未生成该语义对应的目标字段",
        }
        reason = {"code": code, "semantic": semantic, "message": messages.get(code, code)}
        reason.update(extra)
        return reason

    def _finalize_semantics(
        self,
        *,
        semantic_names: Iterable[str],
        unknown_allowed: set[str],
        source_contract: dict[str, Any] | None,
        source_body: dict[str, Any],
        target_contract: dict[str, Any] | None,
        output_body: dict[str, Any],
        loss: set[str],
        technical_issues: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str], dict[str, str]]:
        reasons: list[dict[str, Any]] = []
        unknown_semantics: list[str] = []
        states: dict[str, str] = {}
        target_index = self._semantic_index(target_contract)
        allow_all_unknown = bool({"*", "all"} & unknown_allowed)

        fatal_target_fields = {
            issue.get("target_field")
            for issue in technical_issues
            if issue.get("code") in {
                "unmapped_value", "unknown_promoted_to_known",
                "missing_source_field", "unsupported_operation",
            } and issue.get("target_field")
        }

        for semantic in semantic_names:
            source_state, source_field, source_value = self._semantic_state(
                source_contract, source_body, semantic
            )
            target_fields = target_index.get(semantic, [])
            target_state, target_field, target_value = self._semantic_state(
                target_contract, output_body, semantic
            )

            if semantic in loss:
                final_state = "lost"
            elif not target_contract:
                final_state = "unknown_contract"
            elif not target_fields:
                final_state = "lost"
            elif target_field in fatal_target_fields:
                final_state = "unreliable"
            elif target_state == "known":
                final_state = "known"
            elif target_state == "unknown":
                final_state = "unknown"
            else:
                # 目标契约有语义字段，但转换没有给出值。
                final_state = "missing_output"

            states[semantic] = final_state

            if final_state == "known":
                continue

            allowed = allow_all_unknown or semantic in unknown_allowed
            source_is_unknown = source_state == "unknown"
            source_absent = source_state in {"absent", "unknown_contract"}

            # 只有源语义确实未知，或转换已经显式给出 unknown/null 时，
            # 才允许把“未知”作为可靠结果；不能把已知但丢失的信息伪装成未知。
            can_be_explicit_unknown = (
                final_state == "unknown"
                or (source_is_unknown and final_state in {"lost", "missing_output"})
            )

            if allowed and can_be_explicit_unknown and not source_absent:
                if target_fields:
                    field_name = target_field if target_field in target_fields else target_fields[0]
                    definition = (target_contract or {}).get("fields", {}).get(field_name, {})
                    current_value = output_body.get(field_name, _MISSING)
                    if _is_explicit_unknown(current_value):
                        if current_value is None and not definition.get("nullable"):
                            reasons.append(self._semantic_reason(
                                "unknown_not_representable", semantic, field=field_name))
                            continue
                        unknown_semantics.append(semantic)
                        states[semantic] = "unknown_accepted"
                        continue
                    if definition.get("nullable"):
                        output_body[field_name] = None
                    elif definition.get("type") == "enum":
                        output_body[field_name] = "unknown"
                    else:
                        reasons.append(self._semantic_reason(
                            "unknown_not_representable", semantic, field=field_name,
                            field_type=definition.get("type")))
                        continue
                # 目标契约没有该语义字段时，通过交付信封的 unknown_semantics
                # 作显式声明，而不是猜测一个“正常”业务值。
                unknown_semantics.append(semantic)
                states[semantic] = "unknown_accepted_envelope"
                continue

            if source_absent:
                reasons.append(self._semantic_reason(
                    "semantic_absent", semantic, source_state=source_state))
            elif final_state in {"lost", "missing_output"}:
                reasons.append(self._semantic_reason(
                    "semantic_lost", semantic,
                    source_state=source_state,
                    source_field=source_field,
                    target_fields=target_fields,
                    unknown_allowed=allowed,
                ))
            elif final_state == "unreliable":
                reasons.append(self._semantic_reason(
                    "unreliable_conversion", semantic, target_field=target_field))
            else:
                reasons.append(self._semantic_reason(
                    "unknown_semantic_not_allowed", semantic,
                    source_state=source_state, target_state=target_state))

        return reasons, unknown_semantics, states

    def _evaluate_native(
        self, revision: dict[str, Any], subscriber: dict[str, Any]
    ) -> dict[str, Any]:
        schema = revision["schema"]
        body = deepcopy(revision["body"])
        validation_errors = self._validate_body(schema, body)
        source_contract = self.state["contracts"].get(schema)
        reasons = list(validation_errors)
        semantic_reasons, unknown_semantics, states = self._finalize_semantics(
            semantic_names=subscriber.get("must_keep", []),
            unknown_allowed=set(subscriber.get("unknown_ok", [])),
            source_contract=source_contract,
            source_body=revision["body"],
            target_contract=source_contract,
            output_body=body,
            loss=set(),
            technical_issues=[],
        )
        reasons.extend(semantic_reasons)
        viable = not reasons
        return {
            "viable": viable,
            "target_schema": schema,
            "transform_path": [],
            "body": body,
            "unknown_semantics": unknown_semantics,
            "semantic_states": states,
            "reasons": reasons,
            "provenance": {
                "selection": "native_contract",
                "source_contract": schema,
                "target_contract": schema,
                "transform_ids": [],
                "loss": [],
                "contracts": {
                    "source": deepcopy(source_contract),
                    "target": deepcopy(source_contract),
                },
                "transforms": [],
            },
        }

    def _evaluate_path(
        self, revision: dict[str, Any], subscriber: dict[str, Any],
        target_schema: str, path: list[str]
    ) -> dict[str, Any]:
        source_schema = revision["schema"]
        source_body = deepcopy(revision["body"])
        reasons: list[dict[str, Any]] = []
        loss: set[str] = set()
        transform_snapshots: list[dict[str, Any]] = []
        current_schema = source_schema
        current_body = source_body

        source_errors = self._validate_body(current_schema, current_body)
        reasons.extend(source_errors)

        for transform_id in path:
            transform = self.state["transforms"].get(transform_id)
            if transform is None:
                reasons.append({"code": "transform_not_found", "transform_id": transform_id})
                continue
            if transform.get("status") != "active":
                reasons.append({"code": "transform_disabled", "transform_id": transform_id,
                                "disabled_at": transform.get("disabled_at")})
            transform_snapshots.append(deepcopy(transform))
            loss.update(transform.get("loss", []))

            requirement_failures = self._eval_requires(
                transform.get("requires", []), current_body, current_schema
            )
            reasons.extend(requirement_failures)
            output, issues, _links = self._apply_one_transform(
                transform,
                current_body,
                source_contract=self.state["contracts"].get(current_schema),
                target_contract=self.state["contracts"].get(transform["target"]),
            )
            for issue in issues:
                issue.setdefault("source_schema", current_schema)
            reasons.extend(issues)
            current_body = output
            current_schema = transform["target"]

        target_contract = self.state["contracts"].get(target_schema)
        target_errors = self._validate_body(target_schema, current_body)
        reasons.extend(target_errors)

        semantic_reasons, unknown_semantics, states = self._finalize_semantics(
            semantic_names=subscriber.get("must_keep", []),
            unknown_allowed=set(subscriber.get("unknown_ok", [])),
            source_contract=self.state["contracts"].get(source_schema),
            source_body=revision["body"],
            target_contract=target_contract,
            output_body=current_body,
            loss=loss,
            technical_issues=reasons,
        )
        reasons.extend(semantic_reasons)

        # 技术原因和语义原因都可能导致不可交付。
        viable = not reasons
        return {
            "viable": viable,
            "target_schema": target_schema,
            "transform_path": path,
            "body": current_body,
            "unknown_semantics": unknown_semantics,
            "semantic_states": states,
            "reasons": reasons,
            "provenance": {
                "selection": "active_transform_path",
                "source_contract": source_schema,
                "target_contract": target_schema,
                "transform_ids": list(path),
                "loss": sorted(loss),
                "contracts": {
                    "source": deepcopy(self.state["contracts"].get(source_schema)),
                    "target": deepcopy(target_contract),
                },
                "transforms": transform_snapshots,
            },
        }

    def select_candidate(self, revision: dict[str, Any],
                         subscriber: dict[str, Any]) -> dict[str, Any]:
        """按接收方声明的版本顺序选择第一个可靠候选。"""

        evaluations: list[dict[str, Any]] = []
        flat_reasons: list[dict[str, Any]] = []
        source_schema = revision.get("schema")

        for requested_schema in subscriber.get("schemas", []):
            if requested_schema == source_schema:
                evaluation = self._evaluate_native(revision, subscriber)
                evaluations.append(evaluation)
                flat_reasons.extend(evaluation["reasons"])
                if evaluation["viable"]:
                    evaluation["selected"] = True
                    return evaluation
                continue

            paths = self._find_paths(source_schema, requested_schema)
            if not paths:
                disabled_matches = [
                    tr for tr in self.state["transforms"].values()
                    if tr.get("source") == source_schema and tr.get("target") == requested_schema
                ]
                reason = {
                    "code": "no_active_transform_path",
                    "source": source_schema,
                    "target": requested_schema,
                    "disabled_transforms": [
                        {"id": tr["id"], "disabled_at": tr.get("disabled_at")}
                        for tr in disabled_matches if tr.get("status") == "disabled"
                    ],
                }
                flat_reasons.append(reason)
                evaluations.append({"viable": False, "target_schema": requested_schema,
                                    "transform_path": [], "reasons": [reason]})
                continue

            for path in paths:
                evaluation = self._evaluate_path(revision, subscriber, requested_schema, path)
                evaluations.append(evaluation)
                flat_reasons.extend(evaluation["reasons"])
                if evaluation["viable"]:
                    evaluation["selected"] = True
                    return evaluation

        must_keep = subscriber.get("must_keep", [])
        primary_semantic = None
        primary_code = "no_compatible_contract"
        for semantic in must_keep:
            matching = [r for r in flat_reasons if r.get("semantic") == semantic]
            if matching:
                primary_semantic = semantic
                primary_code = matching[0].get("code", primary_code)
                break
        if primary_semantic is None:
            semantic_reasons = [r for r in flat_reasons if r.get("semantic")]
            if semantic_reasons:
                primary_semantic = semantic_reasons[0]["semantic"]
                primary_code = semantic_reasons[0].get("code", primary_code)
            elif flat_reasons:
                primary_code = flat_reasons[0].get("code", primary_code)

        return {
            "viable": False,
            "selected": False,
            "code": primary_code,
            "primary_semantic": primary_semantic,
            "reasons": flat_reasons,
            "evaluations": evaluations,
        }

    # ------------------------------------------------------------------
    # 诊断、轮询、确认
    # ------------------------------------------------------------------
    def _record_diagnosis(self, *, subscriber_id: str, device: str, seq: int,
                          revision: dict[str, Any], candidate: dict[str, Any],
                          at: Any, op_index: int, kind: str = "event",
                          correction_id: str | None = None) -> dict[str, Any]:
        diagnosis_id = self._new_id("diagnosis", "diag")
        diagnosis = {
            "id": diagnosis_id,
            "kind": kind,
            "correction_id": correction_id,
            "at": at,
            "op_index": op_index,
            "subscriber_id": subscriber_id,
            "device": device,
            "seq": seq,
            "rev": revision.get("rev"),
            "source_contract": revision.get("schema"),
            "code": candidate.get("code", "no_compatible_version"),
            "semantic": candidate.get("primary_semantic"),
            "message": candidate.get("message") or (
                f"{device} seq={seq} rev={revision.get('rev')} 被 {candidate.get('code')} 阻塞"
            ),
            "reasons": candidate.get("reasons", []),
            "available_schemas": self.state["subscribers"][subscriber_id].get("schemas", []),
            "original": deepcopy(revision),
        }
        self.state["diagnoses"][diagnosis_id] = diagnosis
        return diagnosis

    def _build_catchup(self, device: str, block: dict[str, Any], at: Any) -> dict[str, Any]:
        device_record = self.state["devices"].get(device, {"seqs": {}})
        since_op = int(block.get("since_op", 0))
        blocked_seq = int(block["seq"])
        all_revised: list[dict[str, Any]] = []
        during: list[dict[str, Any]] = []

        for key in sorted(device_record.get("seqs", {}), key=lambda x: int(x)):
            seq_record = device_record["seqs"][key]
            seq = int(seq_record["seq"])
            if seq < blocked_seq:
                continue
            revisions = seq_record.get("revisions", [])
            if len(revisions) <= 1:
                continue
            item = {
                "device": device,
                "seq": seq,
                "revision_numbers": [int(r["rev"]) for r in revisions],
                "from_rev": int(revisions[0]["rev"]),
                "to_rev": int(revisions[-1]["rev"]),
                "new_revisions_during_block": [
                    int(r["rev"]) for r in revisions if int(r.get("op_index", 0)) > since_op
                ],
                "revisions": [
                    {"rev": int(r["rev"]), "schema": r["schema"], "published_at": r.get("published_at")}
                    for r in revisions
                ],
            }
            item["changed_during_block"] = bool(item["new_revisions_during_block"])
            all_revised.append(item)
            if item["changed_during_block"]:
                during.append(item)

        return {
            "blocked_seq": blocked_seq,
            "block_started_at": block.get("since"),
            "resolved_at": at,
            "corrected_events": during,
            "revised_while_undelivered": all_revised,
        }

    def _correction_summaries(self, correction_ids: list[str]) -> list[dict[str, Any]]:
        summaries = []
        for cid in correction_ids:
            correction = self.state["corrections"].get(cid)
            if not correction:
                continue
            summaries.append({
                "correction_id": cid,
                "kind": correction.get("kind"),
                "reason": correction.get("reason"),
                "status": correction.get("status"),
                "original_delivery_id": correction.get("original_delivery_id"),
                "original_rev": correction.get("original_rev"),
                "target_rev": correction.get("target_rev"),
                "requested_transform_id": correction.get("requested_transform_id"),
                "delivery_id": correction.get("delivery_id"),
            })
        return summaries

    def _delivery_response(self, delivery: dict[str, Any],
                           device_state: dict[str, Any], *, retry: bool) -> dict[str, Any]:
        result = dict(delivery)
        result["delivery_status"] = delivery.get("status")
        result["status"] = "delivered"
        result["outcome"] = "delivered"
        result["retry"] = retry
        result["delivery"] = delivery
        result["corrections"] = self._correction_summaries(delivery.get("correction_ids", []))
        result["queued_corrections"] = self._correction_summaries(
            device_state.get("correction_queue", [])
        )
        active_id = device_state.get("active_delivery_id")
        result["active_delivery_id"] = active_id
        result["queued_correction_ids"] = list(device_state.get("correction_queue", []))
        if retry:
            result["message"] = "同一交付尝试的幂等重试结果；正文和交付编号均未改变"
        return result

    def poll(self, data: dict[str, Any], at: Any = None) -> dict[str, Any]:
        at, op_index = self._prepare(at)
        subscriber_id = self._subscriber_id(data)
        device = self._device_id(data)
        subscriber = self.state["subscribers"].get(subscriber_id)
        if subscriber is None:
            raise NotFoundError(f"接收方尚未登记: {subscriber_id}")
        device_state = self._sub_device_state(subscriber_id, device)

        # 1. 已有未确认交付：重试必须返回同一次尝试，不能换正文或编号。
        active_id = device_state.get("active_delivery_id")
        if active_id:
            delivery = device_state["deliveries"].get(active_id)
            if delivery is None:
                raise ConflictError(f"活动交付编号损坏: {active_id}")
            delivery["attempts"] = int(delivery.get("attempts", 1)) + 1
            result = self._delivery_response(delivery, device_state, retry=True)
            self._audit("poll", data, {"status": "delivered", "delivery_id": active_id, "retry": True}, at, op_index)
            return result

        # 2. 已确认原交付后，按顺序发送明确更正。更正不会被后来的原件替代。
        while device_state.get("correction_queue"):
            correction_id = device_state["correction_queue"][0]
            correction = self.state["corrections"].get(correction_id)
            if correction is None or correction.get("status") in {"acked", "cancelled"}:
                device_state["correction_queue"].pop(0)
                continue

            seq_record = self._seq_record(device, int(correction["seq"]))
            target_rev_number = int(correction["target_rev"])
            revision = None
            if seq_record:
                revision = next((r for r in seq_record["revisions"]
                                 if int(r["rev"]) == target_rev_number), None)
            if revision is None:
                diagnosis = {
                    "code": "correction_revision_missing",
                    "primary_semantic": None,
                    "reasons": [{"code": "correction_revision_missing",
                                 "rev": target_rev_number}],
                }
                stored = self._record_diagnosis(
                    subscriber_id=subscriber_id, device=device, seq=int(correction["seq"]),
                    revision={"rev": target_rev_number, "schema": correction.get("original_schema"),
                              "body": {}},
                    candidate=diagnosis, at=at, op_index=op_index,
                    kind="correction", correction_id=correction_id,
                )
                correction["status"] = "blocked"
                correction["last_diagnosis_id"] = stored["id"]
                return {
                    "status": "correction_blocked",
                    "outcome": "blocked",
                    "subscriber_id": subscriber_id,
                    "id": subscriber_id,
                    "device": device,
                    "seq": correction["seq"],
                    "correction_id": correction_id,
                    "diagnosis": stored,
                }

            candidate = self.select_candidate(revision, subscriber)
            if not candidate.get("viable"):
                stored = self._record_diagnosis(
                    subscriber_id=subscriber_id, device=device, seq=int(correction["seq"]),
                    revision=revision, candidate=candidate, at=at, op_index=op_index,
                    kind="correction", correction_id=correction_id,
                )
                correction["status"] = "blocked"
                correction["last_diagnosis_id"] = stored["id"]
                return {
                    "status": "correction_blocked",
                    "outcome": "blocked",
                    "subscriber_id": subscriber_id,
                    "id": subscriber_id,
                    "device": device,
                    "seq": correction["seq"],
                    "rev": revision["rev"],
                    "correction_id": correction_id,
                    "code": stored["code"],
                    "semantic": stored["semantic"],
                    "diagnosis_id": stored["id"],
                    "diagnosis": stored,
                    "original": deepcopy(revision),
                }

            device_state["delivery_counter"] = int(device_state.get("delivery_counter", 0)) + 1
            delivery_id = f"delivery:{device_state['delivery_counter']}"
            delivery = {
                "delivery_id": delivery_id,
                "kind": "repair_correction" if correction["kind"] == "transform_repair" else "source_correction",
                "correction_id": correction_id,
                "corrects_delivery_id": correction.get("original_delivery_id"),
                "corrects_rev": correction.get("original_rev"),
                "subscriber_id": subscriber_id,
                "device": device,
                "seq": int(correction["seq"]),
                "rev": int(revision["rev"]),
                "schema": candidate["target_schema"],
                "contract": candidate["target_schema"],
                "source_schema": revision["schema"],
                "source_contract": revision["schema"],
                "body": candidate["body"],
                "unknown_semantics": candidate.get("unknown_semantics", []),
                "semantic_states": candidate.get("semantic_states", {}),
                "provenance": candidate["provenance"],
                "status": "pending",
                "attempts": 1,
                "created_at": at,
                "acked_at": None,
                "correction_ids": [],
            }
            device_state["deliveries"][delivery_id] = delivery
            device_state["active_delivery_id"] = delivery_id
            device_state["correction_queue"].pop(0)
            correction["status"] = "notified"
            correction["delivery_id"] = delivery_id
            correction.setdefault("notifications", []).append({
                "state": "delivered",
                "at": at,
                "subscriber_id": subscriber_id,
                "delivery_id": delivery_id,
            })
            result = self._delivery_response(delivery, device_state, retry=False)
            self._audit("poll", data, {"status": "correction", "delivery_id": delivery_id,
                                       "correction_id": correction_id}, at, op_index)
            return result

        # 3. 严格顺序的下一条原件。
        seq = int(device_state.get("next_seq", 1))
        seq_record = self._seq_record(device, seq)
        revision = self._current_revision(seq_record) if seq_record else None

        if revision is None:
            later = []
            device_record = self.state["devices"].get(device)
            if device_record:
                later = [int(k) for k in device_record["seqs"] if int(k) > seq]
            result = {
                "status": "gap" if later else "waiting",
                "outcome": "no_delivery",
                "subscriber_id": subscriber_id,
                "id": subscriber_id,
                "device": device,
                "next_seq": seq,
                "missing_seq": seq,
                "later_sequences": sorted(later),
            }
            self._audit("poll", data, {"status": result["status"]}, at, op_index)
            return result

        candidate = self.select_candidate(revision, subscriber)
        if not candidate.get("viable"):
            diagnosis = self._record_diagnosis(
                subscriber_id=subscriber_id, device=device, seq=seq, revision=revision,
                candidate=candidate, at=at, op_index=op_index,
            )
            block = device_state.get("block")
            if not block or int(block.get("seq", -1)) != seq:
                block = {
                    "seq": seq,
                    "rev": int(revision["rev"]),
                    "since": at,
                    "since_op": op_index,
                    "first_diagnosis_id": diagnosis["id"],
                    "diagnosis_ids": [],
                    "poll_count": 0,
                }
                device_state["block"] = block
            block["rev"] = int(revision["rev"])
            block["last_at"] = at
            block["last_op"] = op_index
            block["diagnosis_ids"].append(diagnosis["id"])
            block["poll_count"] = int(block.get("poll_count", 0)) + 1

            result = {
                "status": "blocked",
                "outcome": "blocked",
                "blocked": True,
                "subscriber_id": subscriber_id,
                "id": subscriber_id,
                "device": device,
                "seq": seq,
                "rev": int(revision["rev"]),
                "next_seq": seq,
                "delivery_id": None,
                "code": diagnosis["code"],
                "reason_code": diagnosis["code"],
                "semantic": diagnosis["semantic"],
                "blocked_semantic": diagnosis["semantic"],
                "diagnosis_id": diagnosis["id"],
                "diagnosis": diagnosis,
                "original": deepcopy(revision),
            }
            self._audit("poll", data, {"status": "blocked", "diagnosis_id": diagnosis["id"]}, at, op_index)
            return result

        catchup = None
        if device_state.get("block") and int(device_state["block"].get("seq", -1)) == seq:
            catchup = self._build_catchup(device, device_state["block"], at)
            device_state.setdefault("block_history", []).append({
                **deepcopy(device_state["block"]),
                "resolved_at": at,
                "catchup": catchup,
            })
            device_state["block"] = None
        else:
            # 即使没有留下一次 blocked poll（例如先更正、后首次轮询/升级），
            # 也要把未交付原件的修订历史明确带给接收方。
            provisional_block = {"seq": seq, "since_op": 0, "since": None}
            revision_report = self._build_catchup(device, provisional_block, at)
            if revision_report.get("revised_while_undelivered"):
                catchup = revision_report

        device_state["delivery_counter"] = int(device_state.get("delivery_counter", 0)) + 1
        delivery_id = f"delivery:{device_state['delivery_counter']}"
        delivery = {
            "delivery_id": delivery_id,
            "kind": "event",
            "correction_id": None,
            "corrects_delivery_id": None,
            "subscriber_id": subscriber_id,
            "device": device,
            "seq": seq,
            "rev": int(revision["rev"]),
            "schema": candidate["target_schema"],
            "contract": candidate["target_schema"],
            "source_schema": revision["schema"],
            "source_contract": revision["schema"],
            "body": candidate["body"],
            "unknown_semantics": candidate.get("unknown_semantics", []),
            "semantic_states": candidate.get("semantic_states", {}),
            "provenance": candidate["provenance"],
            "catchup": catchup,
            "status": "pending",
            "attempts": 1,
            "created_at": at,
            "acked_at": None,
            "correction_ids": [],
        }
        device_state["deliveries"][delivery_id] = delivery
        device_state["active_delivery_id"] = delivery_id
        result = self._delivery_response(delivery, device_state, retry=False)
        self._audit("poll", data, {"status": "delivered", "delivery_id": delivery_id}, at, op_index)
        return result

    def acknowledge(self, data: dict[str, Any], at: Any = None) -> dict[str, Any]:
        at, op_index = self._prepare(at)
        subscriber_id = self._subscriber_id(data)
        device = self._device_id(data)
        delivery_id = str(self._require(data, "delivery", "delivery_id", "ack"))
        runtime = self.state["subscriptions"].get(subscriber_id)
        device_state = (runtime or {}).get("devices", {}).get(device)
        if not device_state or delivery_id not in device_state.get("deliveries", {}):
            raise NotFoundError(f"交付不存在: {subscriber_id}/{device}/{delivery_id}")

        delivery = device_state["deliveries"][delivery_id]
        if delivery.get("status") == "acked":
            result = {"status": "already_acked", "id": subscriber_id, "device": device,
                      "delivery_id": delivery_id, "acked_at": delivery.get("acked_at")}
            self._audit("ack", data, result, at, op_index)
            return result

        if device_state.get("active_delivery_id") != delivery_id:
            raise ConflictError(
                f"{delivery_id} 不是当前未确认交付；不能越过未确认尝试确认其他正文"
            )

        delivery["status"] = "acked"
        delivery["acked_at"] = at

        if delivery.get("kind") in {"source_correction", "repair_correction"} and delivery.get("correction_id"):
            correction = self.state["corrections"][delivery["correction_id"]]
            correction["status"] = "acked"
            correction["acked_at"] = at
            for notification in correction.get("notifications", []):
                if notification.get("delivery_id") == delivery_id:
                    notification["state"] = "acked"
                    notification["acked_at"] = at
        elif delivery.get("kind") == "event":
            expected_seq = int(device_state.get("next_seq", 1))
            if int(delivery["seq"]) != expected_seq:
                raise ConflictError(
                    f"严格顺序冲突：期望确认 seq={expected_seq}，实际为 seq={delivery['seq']}"
                )
            device_state["next_seq"] = int(delivery["seq"]) + 1

        device_state["active_delivery_id"] = None
        result = {"status": "acked", "id": subscriber_id, "subscriber_id": subscriber_id,
                  "device": device, "delivery_id": delivery_id,
                  "next_seq": device_state.get("next_seq"), "acked_at": at}
        self._audit("ack", data, result, at, op_index)
        return result

    # ------------------------------------------------------------------
    # 运营更正
    # ------------------------------------------------------------------
    def request_repair(self, data: dict[str, Any], at: Any = None) -> dict[str, Any]:
        at, op_index = self._prepare(at)
        idempotency_key = data.get("request_id") or data.get("idempotency_key")
        if idempotency_key and idempotency_key in self.state["idempotency"]:
            repair_id = self.state["idempotency"][idempotency_key]
            return deepcopy(self.state["repairs"][repair_id]["result"])

        transform_id = data.get("transform") or data.get("transform_id")
        if transform_id is not None:
            transform_id = str(transform_id)
        device, seq = self._event_selector(data)
        if seq is not None:
            seq = int(seq)
        delivery_filter = data.get("delivery") or data.get("delivery_id")
        subscriber_filter = data.get("id") or data.get("subscriber_id")
        if subscriber_filter is not None:
            subscriber_filter = str(subscriber_filter)

        if not any([transform_id, device is not None and seq is not None, delivery_filter]):
            raise ContractValidationError("repair 必须引用 transform 标识、device+seq 事件位置或 delivery")

        repair_id = self._new_id("repair", "repair")
        correction_ids: list[str] = []
        targets: list[dict[str, Any]] = []

        for subscriber_id, runtime in self.state["subscriptions"].items():
            if subscriber_filter and subscriber_id != subscriber_filter:
                continue
            for target_device, device_state in runtime.get("devices", {}).items():
                if device is not None and target_device != device:
                    continue
                for delivery_id, delivery in device_state.get("deliveries", {}).items():
                    if delivery_filter and delivery_id != delivery_filter:
                        continue
                    if seq is not None and int(delivery.get("seq", -1)) != seq:
                        continue
                    if device is not None and delivery.get("device") != device:
                        continue
                    if transform_id:
                        provenance_ids = list(delivery.get("provenance", {}).get("transform_ids", []))
                        original_ids = list(delivery.get("original_transform_ids", []))
                        if transform_id not in provenance_ids and transform_id not in original_ids:
                            continue

                    # 同一运营请求重复扫描时不会发生；每个匹配交付生成一条更正。
                    correction_id = self._new_id("correction", "correction")
                    original_rev = int(delivery["rev"])
                    correction = {
                        "id": correction_id,
                        "kind": "transform_repair",
                        "type": "transform_repair",
                        "reason": data.get("reason") or "operator_requested_transform_repair",
                        "repair_id": repair_id,
                        "subscriber_id": subscriber_id,
                        "device": delivery["device"],
                        "seq": int(delivery["seq"]),
                        "original_delivery_id": delivery_id,
                        "original_rev": original_rev,
                        # 转换更正针对“已送达的原修订”，不能悄悄换成更新的生产修订。
                        "target_rev": original_rev,
                        "from_rev": original_rev,
                        "to_rev": original_rev,
                        "original_schema": delivery.get("source_schema"),
                        "original_contract": delivery.get("source_contract"),
                        "original_output_contract": delivery.get("schema"),
                        "original_transform_ids": list(
                            delivery.get("provenance", {}).get("transform_ids", [])
                        ),
                        "requested_transform_id": transform_id,
                        "requested_at": at,
                        "status": "queued",
                        "delivery_id": None,
                        "notifications": [{
                            "state": "queued",
                            "at": at,
                            "subscriber_id": subscriber_id,
                            "original_delivery_id": delivery_id,
                        }],
                    }
                    self.state["corrections"][correction_id] = correction
                    device_state.setdefault("correction_queue", []).append(correction_id)
                    delivery.setdefault("correction_ids", []).append(correction_id)
                    correction_ids.append(correction_id)

                    # 运营申请时做一次预检；真正领取时仍按当时有效的契约/转换重新判断。
                    compatibility = "unknown"
                    preflight: dict[str, Any] | None = None
                    subscriber = self.state["subscribers"].get(subscriber_id)
                    target_seq_record = self._seq_record(delivery["device"], int(delivery["seq"]))
                    target_revision = None
                    if target_seq_record:
                        target_revision = next(
                            (r for r in target_seq_record["revisions"]
                             if int(r["rev"]) == original_rev),
                            None,
                        )
                    if subscriber and target_revision:
                        candidate = self.select_candidate(target_revision, subscriber)
                        preflight = {
                            "viable": bool(candidate.get("viable")),
                            "code": candidate.get("code"),
                            "semantic": candidate.get("primary_semantic"),
                            "target_schema": candidate.get("target_schema"),
                            "transform_ids": candidate.get("provenance", {}).get("transform_ids", [])
                                if candidate.get("viable") else [],
                            "body": candidate.get("body") if candidate.get("viable") else None,
                            "reasons": candidate.get("reasons", []),
                        }
                        compatibility = "ready" if candidate.get("viable") else "blocked"
                        if not candidate.get("viable"):
                            correction["status"] = "blocked"
                        correction["preflight"] = preflight

                    targets.append({
                        "subscriber_id": subscriber_id,
                        "device": delivery["device"],
                        "seq": delivery["seq"],
                        "original_rev": original_rev,
                        "original_delivery_id": delivery_id,
                        "correction_id": correction_id,
                        "original_transform_ids": correction["original_transform_ids"],
                        "original_acked": delivery.get("status") == "acked",
                        "compatibility": compatibility,
                    })

        repair = {
            "id": repair_id,
            "transform_id": transform_id,
            "selector": {
                "device": device,
                "seq": seq,
                "delivery": delivery_filter,
                "subscriber_id": subscriber_filter,
            },
            "requested_at": at,
            "correction_ids": correction_ids,
            "targets": targets,
            "status": "created" if correction_ids else "no_matches",
        }
        result = {
            "status": repair["status"],
            "repair_id": repair_id,
            "correction_ids": correction_ids,
            "targets": targets,
            "match_count": len(targets),
        }
        repair["result"] = deepcopy(result)
        self.state["repairs"][repair_id] = repair
        if idempotency_key:
            self.state["idempotency"][idempotency_key] = repair_id
        self._audit("repair", data, result, at, op_index)
        return result

    # ------------------------------------------------------------------
    # 运营检查
    # ------------------------------------------------------------------
    def inspect(self, data: dict[str, Any] | None = None, at: Any = None) -> dict[str, Any]:
        # inspect 是只读操作；不增加 op_count，避免影响 block op 顺序。
        data = data or {}
        subscriber_filter = data.get("id") or data.get("subscriber_id")
        if subscriber_filter is not None:
            subscriber_filter = str(subscriber_filter)
        device_filter, seq_from_location = self._event_selector(data)
        if data.get("device") or data.get("device_id"):
            device_filter = str(data.get("device") or data.get("device_id"))
        seq_filter = data.get("seq")
        if seq_filter is None:
            seq_filter = seq_from_location
        if seq_filter is not None:
            seq_filter = int(seq_filter)
        semantic_filter = data.get("semantic")
        delivery_filter = data.get("delivery") or data.get("delivery_id")

        report: dict[str, Any] = {"filters": {
            "subscriber_id": subscriber_filter,
            "device": device_filter,
            "seq": seq_filter,
            "semantic": semantic_filter,
            "delivery": delivery_filter,
        }}

        if not any([subscriber_filter, device_filter, seq_filter is not None,
                    delivery_filter, semantic_filter]):
            report["summary"] = {
                "contracts": len(self.state["contracts"]),
                "transforms": len(self.state["transforms"]),
                "active_transforms": sum(
                    t.get("status") == "active" for t in self.state["transforms"].values()
                ),
                "subscribers": len(self.state["subscribers"]),
                "devices": len(self.state["devices"]),
                "diagnoses": len(self.state["diagnoses"]),
                "corrections": len(self.state["corrections"]),
                "repairs": len(self.state["repairs"]),
            }
            report["contracts"] = deepcopy(self.state["contracts"])
            report["transforms"] = deepcopy(self.state["transforms"])
            report["subscribers"] = deepcopy(self.state["subscribers"])
            report["devices"] = deepcopy(self.state["devices"])
            report["subscriptions"] = deepcopy(self.state["subscriptions"])
            report["diagnoses"] = list(self.state["diagnoses"].values())
            report["dead_letters"] = report["diagnoses"]
            report["corrections"] = list(self.state["corrections"].values())
            report["repairs"] = list(self.state["repairs"].values())
            return report

        if device_filter is not None:
            device_record = self.state["devices"].get(device_filter)
            report["device"] = device_record
            if device_record:
                report["events"] = list(device_record.get("seqs", {}).values())
            if seq_filter is not None and device_record:
                report["event"] = device_record["seqs"].get(str(seq_filter))

        subscribers_view: dict[str, Any] = {}
        acknowledged: list[dict[str, Any]] = []
        for subscriber_id, runtime in self.state["subscriptions"].items():
            if subscriber_filter and subscriber_id != subscriber_filter:
                continue
            devices_view: dict[str, Any] = {}
            for device_name, device_state in runtime.get("devices", {}).items():
                if device_filter and device_name != device_filter:
                    continue
                view = deepcopy(device_state)
                if seq_filter is not None:
                    matching = {}
                    for delivery_id, delivery in device_state.get("deliveries", {}).items():
                        if int(delivery.get("seq", -1)) == seq_filter:
                            matching[delivery_id] = deepcopy(delivery)
                    view["deliveries"] = matching
                    if view.get("block") and int(view["block"].get("seq", -1)) != seq_filter:
                        view.pop("block", None)
                    view["block_history"] = [
                        item for item in view.get("block_history", [])
                        if int(item.get("seq", -1)) == seq_filter
                    ]
                if delivery_filter:
                    view["deliveries"] = {
                        k: deepcopy(v) for k, v in device_state.get("deliveries", {}).items()
                        if k == delivery_filter or v.get("delivery_id") == delivery_filter
                    }
                devices_view[device_name] = view

                for delivery in device_state.get("deliveries", {}).values():
                    if seq_filter is not None and int(delivery.get("seq", -1)) != seq_filter:
                        continue
                    if delivery_filter and delivery.get("delivery_id") != delivery_filter:
                        continue
                    if delivery.get("status") == "acked":
                        acknowledged.append({
                            "subscriber_id": subscriber_id,
                            "device": delivery.get("device"),
                            "seq": delivery.get("seq"),
                            "rev": delivery.get("rev"),
                            "delivery_id": delivery.get("delivery_id"),
                            "kind": delivery.get("kind"),
                            "schema": delivery.get("schema"),
                            "source_contract": delivery.get("source_contract"),
                            "transform_ids": delivery.get("provenance", {}).get("transform_ids", []),
                            "acked_at": delivery.get("acked_at"),
                        })
            subscribers_view[subscriber_id] = {
                "config": deepcopy(self.state["subscribers"].get(subscriber_id)),
                "devices": devices_view,
            }
        report["subscriptions"] = subscribers_view
        report["externally_acknowledged"] = acknowledged

        diagnoses = []
        for diagnosis in self.state["diagnoses"].values():
            if subscriber_filter and diagnosis.get("subscriber_id") != subscriber_filter:
                continue
            if device_filter and diagnosis.get("device") != device_filter:
                continue
            if seq_filter is not None and int(diagnosis.get("seq", -1)) != seq_filter:
                continue
            if semantic_filter and diagnosis.get("semantic") != semantic_filter:
                continue
            diagnoses.append(deepcopy(diagnosis))
        report["diagnoses"] = diagnoses
        report["dead_letters"] = diagnoses

        corrections = []
        notification_recipients = []
        for correction in self.state["corrections"].values():
            if subscriber_filter and correction.get("subscriber_id") != subscriber_filter:
                continue
            if device_filter and correction.get("device") != device_filter:
                continue
            if seq_filter is not None and int(correction.get("seq", -1)) != seq_filter:
                continue
            corrections.append(deepcopy(correction))
            for notification in correction.get("notifications", []):
                notification_recipients.append({
                    "correction_id": correction.get("id"),
                    "subscriber_id": correction.get("subscriber_id"),
                    "device": correction.get("device"),
                    "seq": correction.get("seq"),
                    "status": notification.get("state"),
                    "delivery_id": notification.get("delivery_id") or correction.get("delivery_id"),
                    "at": notification.get("at"),
                    "acked_at": notification.get("acked_at"),
                })
        report["corrections"] = corrections
        report["correction_recipients"] = notification_recipients

        repairs = []
        for repair in self.state["repairs"].values():
            if device_filter and (repair.get("selector") or {}).get("device") != device_filter:
                # 选择器可能只给 transform；若按设备过滤，检查 targets。
                if not any(t.get("device") == device_filter for t in repair.get("targets", [])):
                    continue
            repairs.append(deepcopy(repair))
        report["repairs"] = repairs
        report["acknowledged"] = report["externally_acknowledged"]
        report["blocked_subscriptions"] = [
            {
                "subscriber_id": subscriber_id,
                "device": device_name,
                **deepcopy(device_state.get("block")),
            }
            for subscriber_id, runtime in report["subscriptions"].items()
            for device_name, device_state in (runtime.get("devices") or {}).items()
            if device_state.get("block")
        ]
        return report

    def dumps(self) -> str:
        return _json_dump(self.state)

    def dead_letters(self, **filters: Any) -> list[dict[str, Any]]:
        """返回保留原件和阻塞原因的诊断记录；过滤参数与 :meth:`inspect` 相同。"""

        return self.inspect(filters or {}).get("diagnoses", [])

    def blocked_events(self, **filters: Any) -> list[dict[str, Any]]:
        """返回当前仍阻塞的订阅位置。"""

        return self.inspect(filters or {}).get("blocked_subscriptions", [])


# 常见近义 API 名称，方便不同调用习惯和验收脚本。
ContractHub.register_subscriber = ContractHub.upsert_subscriber  # type: ignore[attr-defined]
ContractHub.update_subscriber = ContractHub.upsert_subscriber  # type: ignore[attr-defined]
ContractHub.fetch = ContractHub.poll  # type: ignore[attr-defined]
ContractHub.ack = ContractHub.acknowledge  # type: ignore[attr-defined]
ContractHub.repair = ContractHub.request_repair  # type: ignore[attr-defined]
