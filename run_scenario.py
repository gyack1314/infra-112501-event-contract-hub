#!/usr/bin/env python3
"""用项目资料读取器校验并回放一个或多个演练文件。

示例：
    python3 run_scenario.py data/unknown-before-upgrade.jsonl --brief
    python3 run_scenario.py data/obsolete-in-flight.jsonl --state /tmp/hub.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from contract_hub import ContractHub
from trace_io import read_trace


def brief(result: dict) -> dict:
    keys = ("status", "outcome", "id", "subscriber_id", "device", "seq", "rev",
            "delivery_id", "kind", "semantic", "code", "body", "corrects_delivery_id")
    item = {k: result[k] for k in keys if k in result}
    if result.get("corrections"):
        item["corrections"] = [c["correction_id"] for c in result["corrections"]]
    if result.get("catchup"):
        item["corrected_events"] = result["catchup"]["corrected_events"]
    return item


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", type=Path, nargs="+")
    parser.add_argument("--state", type=Path, help="持久化状态 JSON；默认仅内存回放")
    parser.add_argument("--brief", action="store_true", help="输出精简 poll/ack/repair/inspect 结果")
    args = parser.parse_args()

    allowed = set(json.loads((Path(__file__).parent / "data" / "event_types.json").read_text("utf-8")))
    hub = ContractHub.load(args.state) if args.state else ContractHub()

    for path in args.scenario:
        for event in read_trace(path, allowed):
            result = hub.apply(event)
            if event["type"] in {"poll", "ack", "repair", "inspect"}:
                record = {"timestamp": event["timestamp"], "type": event["type"], "data": event["data"]}
                record["result"] = brief(result) if args.brief else result
                print(json.dumps(record, ensure_ascii=False, indent=2))

    if args.state:
        hub.save(args.state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
