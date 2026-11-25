"""命令行操作入口。

示例::

    python -m contract_hub --state hub.json replay data/unknown-before-upgrade.jsonl
    python -m contract_hub --state hub.json poll --id lab --device m1
    python -m contract_hub --state hub.json ack --id line --device m2 --delivery delivery:1
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .core import ContractHub, HubError


DEFAULT_STATE = Path(".contract_hub_state.json")


def _load_json_argument(value: str | None) -> dict[str, Any]:
    if value is None:
        return {}
    candidate = Path(value)
    if value.startswith("@"):
        value = value[1:]
        candidate = Path(value)
    if candidate.exists() and value.strip().startswith(("{", "[")) is False:
        parsed = json.loads(candidate.read_text(encoding="utf-8"))
    else:
        parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("JSON 参数必须是对象")
    return parsed


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _split_csv(values: list[str] | None) -> list[str]:
    result: list[str] = []
    for value in values or []:
        result.extend(item.strip() for item in value.split(",") if item.strip())
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="事件契约协商与更正中间件操作端")
    parser.add_argument("--state", default=str(DEFAULT_STATE), help="JSON 状态文件路径")
    subparsers = parser.add_subparsers(dest="command", required=True)

    apply_parser = subparsers.add_parser("apply", help="应用一条完整事件或操作 JSON")
    apply_parser.add_argument("event_json", help='例如 \'{"type":"poll","data":{...}}\'，也可用 @file.json')

    replay_parser = subparsers.add_parser("replay", help="按顺序回放项目 JSONL 演练")
    replay_parser.add_argument("file", type=Path)
    replay_parser.add_argument("--quiet-ops", action="store_true", help="只输出 poll/ack/repair/inspect")
    replay_parser.add_argument("--stop-on-error", action="store_true")

    contract_parser = subparsers.add_parser("register-contract", help="生产端登记契约")
    contract_parser.add_argument("json", help="contract data JSON 或 @contract.json")

    transform_parser = subparsers.add_parser("register-transform", help="登记转换")
    transform_parser.add_argument("json", help="transform data JSON 或 @transform.json")

    disable_parser = subparsers.add_parser("disable", help="运营停用转换")
    disable_parser.add_argument("transform_id")
    disable_parser.add_argument("--reason")

    subscribe_parser = subparsers.add_parser("subscribe", help="登记或变更接收方可用版本")
    subscribe_parser.add_argument("json", nargs="?", help="subscriber data JSON 或 @file")
    subscribe_parser.add_argument("--id")
    subscribe_parser.add_argument("--schemas", action="append", help="逗号分隔或重复传入")
    subscribe_parser.add_argument("--must-keep", action="append")
    subscribe_parser.add_argument("--unknown-ok", action="append")

    publish_parser = subparsers.add_parser("publish", help="生产端发布事件/修订")
    publish_parser.add_argument("json", nargs="?", help="publish data JSON 或 @file")
    publish_parser.add_argument("--device")
    publish_parser.add_argument("--seq", type=int)
    publish_parser.add_argument("--rev", type=int)
    publish_parser.add_argument("--schema")
    publish_parser.add_argument("--body-json", help="body JSON 对象或 @body.json")

    poll_parser = subparsers.add_parser("poll", help="接收方领取下一次交付")
    poll_parser.add_argument("--id", "--subscriber", dest="id", required=True)
    poll_parser.add_argument("--device", required=True)

    ack_parser = subparsers.add_parser("ack", help="接收方确认交付")
    ack_parser.add_argument("--id", "--subscriber", dest="id", required=True)
    ack_parser.add_argument("--device", required=True)
    ack_parser.add_argument("--delivery", required=True)

    repair_parser = subparsers.add_parser("repair", help="运营申请明确更正")
    repair_parser.add_argument("json", nargs="?", help="repair data JSON 或 @file")
    repair_parser.add_argument("--transform")
    repair_parser.add_argument("--device")
    repair_parser.add_argument("--seq", type=int)
    repair_parser.add_argument("--delivery")
    repair_parser.add_argument("--id", "--subscriber", dest="id")
    repair_parser.add_argument("--reason")
    repair_parser.add_argument("--request-id")

    inspect_parser = subparsers.add_parser("inspect", help="运营检查阻塞、确认和更正通知")
    inspect_parser.add_argument("--id", "--subscriber", dest="id")
    inspect_parser.add_argument("--device")
    inspect_parser.add_argument("--seq", type=int)
    inspect_parser.add_argument("--semantic")
    inspect_parser.add_argument("--delivery")

    return parser


def _command_data(args: argparse.Namespace) -> dict[str, Any]:
    if getattr(args, "json", None):
        data = _load_json_argument(args.json)
    else:
        data = {}

    if args.command == "subscribe":
        if args.id:
            data["id"] = args.id
        if args.schemas is not None:
            data["schemas"] = _split_csv(args.schemas)
        if args.must_keep is not None:
            data["must_keep"] = _split_csv(args.must_keep)
        if args.unknown_ok is not None:
            data["unknown_ok"] = _split_csv(args.unknown_ok)
        if "id" not in data or "schemas" not in data:
            raise ValueError("subscribe 需要 id 和 schemas")

    elif args.command == "publish":
        for key in ("device", "seq", "rev", "schema"):
            value = getattr(args, key, None)
            if value is not None:
                data[key] = value
        if args.body_json:
            data["body"] = _load_json_argument(args.body_json)
        required = ("device", "seq", "rev", "schema", "body")
        missing = [key for key in required if key not in data]
        if missing:
            raise ValueError(f"publish 缺少参数: {', '.join(missing)}")

    elif args.command == "repair":
        for key in ("transform", "device", "seq", "delivery", "id", "reason", "request_id"):
            value = getattr(args, key.replace("-", "_"), None) if "-" in key else getattr(args, key, None)
            if value is not None:
                data[key] = value

    return data


def replay(hub: ContractHub, path: Path, *, quiet_ops: bool = False,
           stop_on_error: bool = False) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    path = Path(path)
    with path.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                result = hub.apply(event)
                record = {
                    "line": line_number,
                    "timestamp": event.get("timestamp"),
                    "type": event.get("type"),
                    "data": event.get("data"),
                    "result": result,
                }
                outputs.append(record)
                if not quiet_ops or event.get("type") in {"poll", "ack", "repair", "inspect"}:
                    # 回放输出使用 JSONL，便于其他项目操作逐行消费。
                    print(json.dumps(record, ensure_ascii=False))
            except Exception as error:  # noqa: BLE001 - CLI 需要报告行号后继续/退出
                error_record = {"line": line_number, "error": str(error)}
                outputs.append(error_record)
                print(json.dumps(error_record, ensure_ascii=False))
                if stop_on_error:
                    raise
    return outputs


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    state_path = Path(args.state)
    hub = ContractHub.load(state_path)

    try:
        if args.command == "replay":
            replay(hub, args.file, quiet_ops=args.quiet_ops, stop_on_error=args.stop_on_error)
            hub.save(state_path)
            return 0

        if args.command == "apply":
            event = _load_json_argument(args.event_json)
            result = hub.apply(event)
        elif args.command == "disable":
            result = hub.disable_transform(args.transform_id, reason=args.reason)
        elif args.command == "poll":
            result = hub.poll({"id": args.id, "device": args.device})
        elif args.command == "ack":
            result = hub.acknowledge({
                "id": args.id, "device": args.device, "delivery": args.delivery,
            })
        elif args.command == "inspect":
            result = hub.inspect({
                key: value for key, value in {
                    "id": args.id,
                    "device": args.device,
                    "seq": args.seq,
                    "semantic": args.semantic,
                    "delivery": args.delivery,
                }.items() if value is not None
            })
        else:
            data = _command_data(args)
            method = {
                "register-contract": hub.register_contract,
                "register-transform": hub.register_transform,
                "subscribe": hub.upsert_subscriber,
                "publish": hub.publish,
                "repair": hub.request_repair,
            }[args.command]
            result = method(data)

        hub.save(state_path)
        _print_json(result)
        return 0
    except (HubError, ValueError, json.JSONDecodeError) as error:
        _print_json({"status": "error", "error_type": type(error).__name__, "error": str(error)})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
