"""覆盖契约协商、严格顺序、修订、更正和运营检查的验收测试。"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from contract_hub import ContractHub
from contract_hub.cli import main as cli_main
from trace_io import read_trace


ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
ALLOWED_TYPES = set(json.loads((DATA / "event_types.json").read_text(encoding="utf-8")))


def load_scenario_hub(name: str) -> ContractHub:
    hub = ContractHub()
    for event in read_trace(DATA / name, ALLOWED_TYPES):
        hub.apply(event)
    return hub


def run_cli(argv: list[str]) -> int:
    with contextlib.redirect_stdout(io.StringIO()):
        return cli_main(argv)


class SuppliedScenarioTests(unittest.TestCase):
    def test_unknown_before_upgrade_blocks_semantic_then_resumes(self):
        hub = ContractHub()
        results = []
        for event in read_trace(DATA / "unknown-before-upgrade.jsonl", ALLOWED_TYPES):
            results.append((event, hub.apply(event)))

        polls = [result for event, result in results if event["type"] == "poll"]
        blocked_lab, old_delivery, lab_after_upgrade = polls

        self.assertEqual(blocked_lab["status"], "blocked")
        self.assertEqual(blocked_lab["semantic"], "cooling_outcome")
        self.assertEqual(blocked_lab["code"], "semantic_lost")
        self.assertIsNone(blocked_lab["delivery_id"])
        self.assertEqual(blocked_lab["original"]["body"]["status"], "unknown")

        self.assertEqual(old_delivery["status"], "delivered")
        self.assertEqual(old_delivery["delivery_id"], "delivery:1")
        self.assertEqual(old_delivery["body"], {"temp": 8})
        provenance = old_delivery["provenance"]
        self.assertEqual(provenance["source_contract"], "state2")
        self.assertEqual(provenance["target_contract"], "state1")
        self.assertEqual(provenance["transform_ids"], ["to1"])
        self.assertTrue(provenance["transforms"][0]["operation"])

        self.assertEqual(lab_after_upgrade["status"], "delivered")
        self.assertEqual(lab_after_upgrade["delivery_id"], "delivery:1")
        self.assertEqual(lab_after_upgrade["schema"], "state2")
        self.assertEqual(lab_after_upgrade["body"]["status"], "cool")
        catchup = lab_after_upgrade["catchup"]
        self.assertIsNotNone(catchup)
        corrected = catchup["corrected_events"]
        self.assertEqual([item["seq"] for item in corrected], [1])
        self.assertEqual(corrected[0]["from_rev"], 1)
        self.assertEqual(corrected[0]["to_rev"], 2)

        inspect_result = hub.inspect({"device": "m1", "seq": 1})
        self.assertTrue(any(d["semantic"] == "cooling_outcome" for d in inspect_result["diagnoses"]))
        event_report = inspect_result["event"]
        self.assertEqual([r["rev"] for r in event_report["revisions"]], [1, 2])

    def test_obsolete_transform_retry_ack_and_explicit_correction(self):
        hub = ContractHub()
        results = []
        for event in read_trace(DATA / "obsolete-in-flight.jsonl", ALLOWED_TYPES):
            results.append((event, hub.apply(event)))

        by_type = {}
        for event, result in results:
            by_type.setdefault(event["type"], []).append(result)

        first_poll, retry_poll, after_ack_poll = by_type["poll"]
        repair = by_type["repair"][0]
        ack = by_type["ack"][0]

        self.assertEqual(first_poll["body"], {"action": "running"})
        self.assertEqual(first_poll["provenance"]["transform_ids"], ["rename-1"])

        self.assertEqual(repair["status"], "created")
        target = repair["targets"][0]
        self.assertEqual(target["subscriber_id"], "line")
        self.assertEqual(target["original_delivery_id"], "delivery:1")
        self.assertEqual(target["original_transform_ids"], ["rename-1"])

        self.assertEqual(retry_poll["delivery_id"], "delivery:1")
        self.assertEqual(retry_poll["body"], {"action": "running"})
        self.assertTrue(retry_poll["retry"])
        self.assertGreaterEqual(retry_poll["delivery"]["attempts"], 2)
        self.assertEqual(retry_poll["corrections"][0]["correction_id"], "correction:1")

        self.assertEqual(ack["status"], "acked")
        self.assertEqual(after_ack_poll["status"], "delivered")
        self.assertEqual(after_ack_poll["delivery_id"], "delivery:2")
        self.assertEqual(after_ack_poll["kind"], "repair_correction")
        self.assertEqual(after_ack_poll["body"], {"action": "ready"})
        self.assertEqual(after_ack_poll["corrects_delivery_id"], "delivery:1")
        self.assertEqual(after_ack_poll["corrects_rev"], 1)
        self.assertEqual(after_ack_poll["provenance"]["transform_ids"], ["rename-2"])

        report = hub.inspect({"device": "m2", "seq": 1})
        acked = report["externally_acknowledged"]
        self.assertEqual(len(acked), 1)
        self.assertEqual(acked[0]["delivery_id"], "delivery:1")
        self.assertEqual(acked[0]["transform_ids"], ["rename-1"])
        recipients = report["correction_recipients"]
        self.assertTrue(any(r["subscriber_id"] == "line" and r["delivery_id"] == "delivery:2"
                           for r in recipients))


class OrderingAndIndependenceTests(unittest.TestCase):
    def make_phase_hub(self) -> ContractHub:
        hub = ContractHub()
        hub.register_contract({"schema": "old", "fields": {
            "action": {"type": "enum", "nullable": False, "semantics": "phase"}
        }})
        hub.register_contract({"schema": "new", "fields": {
            "phase": {"type": "enum", "nullable": False, "semantics": "phase"}
        }})
        hub.register_subscriber = hub.upsert_subscriber
        hub.upsert_subscriber({"id": "blocked", "schemas": ["old"], "must_keep": ["phase"]})
        hub.upsert_subscriber({"id": "native", "schemas": ["new"], "must_keep": ["phase"]})
        return hub

    def test_head_of_line_blocks_later_pulses_but_other_subscription_continues(self):
        hub = self.make_phase_hub()
        hub.publish({"device": "d1", "seq": 1, "rev": 1, "schema": "new",
                     "body": {"phase": "loaded"}})
        hub.publish({"device": "d1", "seq": 2, "rev": 1, "schema": "new",
                     "body": {"phase": "finished"}})

        blocked = hub.poll({"id": "blocked", "device": "d1"})
        self.assertEqual(blocked["status"], "blocked")
        self.assertEqual(blocked["seq"], 1)

        second_attempt = hub.poll({"id": "blocked", "device": "d1"})
        self.assertEqual(second_attempt["status"], "blocked")
        self.assertEqual(second_attempt["seq"], 1)

        native1 = hub.poll({"id": "native", "device": "d1"})
        self.assertEqual(native1["delivery_id"], "delivery:1")
        self.assertEqual(native1["body"]["phase"], "loaded")
        hub.acknowledge({"id": "native", "device": "d1", "delivery": "delivery:1"})
        native2 = hub.poll({"id": "native", "device": "d1"})
        self.assertEqual(native2["seq"], 2)
        self.assertEqual(native2["body"]["phase"], "finished")

        hub.register_transform({
            "id": "phase-map", "source": "new", "target": "old",
            "operation": {"map_values": {"action": {
                "field": "phase", "map": {"loaded": "running", "finished": "done"}
            }}},
            "requires": [], "loss": [],
        })

        resumed = hub.poll({"id": "blocked", "device": "d1"})
        self.assertEqual(resumed["status"], "delivered")
        self.assertEqual(resumed["seq"], 1)
        self.assertEqual(resumed["body"], {"action": "running"})
        hub.acknowledge({"id": "blocked", "device": "d1", "delivery": "delivery:1"})
        later = hub.poll({"id": "blocked", "device": "d1"})
        self.assertEqual(later["seq"], 2)
        self.assertEqual(later["body"], {"action": "done"})

    def test_devices_for_same_subscriber_are_independent(self):
        hub = self.make_phase_hub()
        hub.register_transform({
            "id": "phase-map", "source": "new", "target": "old",
            "operation": {"map_values": {"action": {
                "field": "phase", "map": {"loaded": "running", "finished": "done"}
            }}},
        })
        hub.publish({"device": "stuck-device", "seq": 1, "rev": 1, "schema": "new",
                     "body": {"phase": "unmapped-value"}})
        hub.publish({"device": "ok-device", "seq": 1, "rev": 1, "schema": "new",
                     "body": {"phase": "loaded"}})

        stuck = hub.poll({"id": "blocked", "device": "stuck-device"})
        self.assertEqual(stuck["status"], "blocked")
        ok = hub.poll({"id": "blocked", "device": "ok-device"})
        self.assertEqual(ok["status"], "delivered")
        self.assertEqual(ok["body"], {"action": "running"})


class UnknownAndRevisionTests(unittest.TestCase):
    def test_unknown_semantic_is_not_replaced_with_normal(self):
        hub = ContractHub()
        hub.register_contract({"schema": "state2", "fields": {
            "status": {"type": "enum", "nullable": False, "semantics": "cooling_outcome"}
        }})
        hub.upsert_subscriber({"id": "qc", "schemas": ["state2"],
                               "must_keep": ["cooling_outcome"], "unknown_ok": []})
        hub.publish({"device": "m", "seq": 1, "rev": 1, "schema": "state2",
                     "body": {"status": "unknown"}})

        blocked = hub.poll({"id": "qc", "device": "m"})
        self.assertEqual(blocked["status"], "blocked")
        self.assertEqual(blocked["semantic"], "cooling_outcome")
        self.assertEqual(blocked["code"], "unknown_semantic_not_allowed")
        self.assertNotIn("normal", json.dumps(blocked["diagnosis"]))

        hub.upsert_subscriber({"id": "qc", "schemas": ["state2"],
                               "must_keep": ["cooling_outcome"],
                               "unknown_ok": ["cooling_outcome"]})
        delivered = hub.poll({"id": "qc", "device": "m"})
        self.assertEqual(delivered["status"], "delivered")
        self.assertEqual(delivered["body"]["status"], "unknown")
        self.assertIn("cooling_outcome", delivered["unknown_semantics"])

    def test_delivered_unacked_body_and_id_are_immutable_and_revision_creates_linked_correction(self):
        hub = ContractHub()
        hub.register_contract({"schema": "s", "fields": {
            "phase": {"type": "enum", "nullable": False, "semantics": "phase"}
        }})
        hub.upsert_subscriber({"id": "line", "schemas": ["s"], "must_keep": ["phase"]})
        hub.publish({"device": "d", "seq": 1, "rev": 1, "schema": "s",
                     "body": {"phase": "loaded"}})
        first = hub.poll({"id": "line", "device": "d"})
        self.assertEqual(first["delivery_id"], "delivery:1")

        # Later sequence exists before correction is acknowledged.
        hub.publish({"device": "d", "seq": 2, "rev": 1, "schema": "s",
                     "body": {"phase": "finished"}})
        hub.publish({"device": "d", "seq": 1, "rev": 2, "schema": "s",
                     "body": {"phase": "finished"}})

        retry = hub.poll({"id": "line", "device": "d"})
        self.assertEqual(retry["delivery_id"], "delivery:1")
        self.assertEqual(retry["body"], {"phase": "loaded"})
        self.assertTrue(retry["retry"])
        self.assertEqual(len(retry["corrections"]), 1)

        hub.acknowledge({"id": "line", "device": "d", "delivery": "delivery:1"})
        correction = hub.poll({"id": "line", "device": "d"})
        self.assertEqual(correction["status"], "delivered")
        self.assertEqual(correction["delivery_id"], "delivery:2")
        self.assertEqual(correction["kind"], "source_correction")
        self.assertEqual(correction["body"], {"phase": "finished"})
        self.assertEqual(correction["corrects_delivery_id"], "delivery:1")
        self.assertEqual(correction["corrects_rev"], 1)

        hub.acknowledge({"id": "line", "device": "d", "delivery": "delivery:2"})
        next_event = hub.poll({"id": "line", "device": "d"})
        self.assertEqual(next_event["delivery_id"], "delivery:3")
        self.assertEqual(next_event["seq"], 2)

    def test_undelivered_revision_is_reevaluated_without_correction_delivery(self):
        hub = ContractHub()
        hub.register_contract({"schema": "old", "fields": {
            "action": {"type": "enum", "nullable": False, "semantics": "phase"}
        }})
        hub.register_contract({"schema": "new", "fields": {
            "phase": {"type": "enum", "nullable": False, "semantics": "phase"}
        }})
        hub.upsert_subscriber({"id": "line", "schemas": ["old"], "must_keep": ["phase"]})
        hub.publish({"device": "d", "seq": 1, "rev": 1, "schema": "new",
                     "body": {"phase": "mystery"}})
        blocked = hub.poll({"id": "line", "device": "d"})
        self.assertEqual(blocked["status"], "blocked")

        hub.register_transform({
            "id": "map", "source": "new", "target": "old",
            "operation": {"map_values": {"action": {
                "field": "phase", "map": {"loaded": "running"}
            }}},
            "requires": [], "loss": [],
        })
        hub.publish({"device": "d", "seq": 1, "rev": 2, "schema": "new",
                     "body": {"phase": "loaded"}})
        delivered = hub.poll({"id": "line", "device": "d"})
        self.assertEqual(delivered["status"], "delivered")
        self.assertEqual(delivered["rev"], 2)
        self.assertEqual(delivered["body"], {"action": "running"})
        self.assertIsNone(delivered["corrects_delivery_id"])
        self.assertEqual(hub.state["corrections"], {})
        self.assertTrue(delivered["catchup"]["corrected_events"])


class RepairFanoutAndCliTests(unittest.TestCase):
    def test_repair_by_transform_fans_out_and_inspect_shows_recipients(self):
        hub = ContractHub()
        hub.register_contract({"schema": "old", "fields": {
            "action": {"type": "enum", "nullable": False, "semantics": "phase"}
        }})
        hub.register_contract({"schema": "new", "fields": {
            "phase": {"type": "enum", "nullable": False, "semantics": "phase"}
        }})
        hub.register_transform({
            "id": "bad", "source": "new", "target": "old",
            "operation": {"map_values": {"action": {
                "field": "phase", "map": {"loaded": "wrong"}
            }}},
        })
        for subscriber in ("a", "b"):
            hub.upsert_subscriber({"id": subscriber, "schemas": ["old"],
                                   "must_keep": ["phase"]})
        hub.publish({"device": "d", "seq": 1, "rev": 1, "schema": "new",
                     "body": {"phase": "loaded"}})
        for subscriber in ("a", "b"):
            result = hub.poll({"id": subscriber, "device": "d"})
            self.assertEqual(result["body"], {"action": "wrong"})

        hub.disable_transform("bad")
        hub.register_transform({
            "id": "good", "source": "new", "target": "old",
            "operation": {"map_values": {"action": {
                "field": "phase", "map": {"loaded": "right"}
            }}},
        })
        repair = hub.request_repair({"transform": "bad", "device": "d", "seq": 1})
        self.assertEqual(repair["match_count"], 2)

        report = hub.inspect({"device": "d", "seq": 1})
        recipient_ids = {item["subscriber_id"] for item in report["correction_recipients"]}
        self.assertEqual(recipient_ids, {"a", "b"})

        for subscriber in ("a", "b"):
            retry = hub.poll({"id": subscriber, "device": "d"})
            self.assertEqual(retry["body"], {"action": "wrong"})
            hub.acknowledge({"id": subscriber, "device": "d", "delivery": "delivery:1"})
            correction = hub.poll({"id": subscriber, "device": "d"})
            self.assertEqual(correction["body"], {"action": "right"})
            self.assertEqual(correction["provenance"]["transform_ids"], ["good"])

        report = hub.inspect({"device": "d", "seq": 1})
        delivered_recipients = {
            item["subscriber_id"]: item["status"]
            for item in report["correction_recipients"]
            if item["delivery_id"] == "delivery:2"
        }
        self.assertEqual(delivered_recipients, {"a": "delivered", "b": "delivered"})

    def test_cli_persists_operations_between_invocations(self):
        with tempfile.TemporaryDirectory() as directory:
            state = str(Path(directory) / "hub.json")
            self.assertEqual(0, run_cli([
                "--state", state, "register-contract",
                json.dumps({"schema": "s", "fields": {"phase": {"type": "enum", "semantics": "phase"}}})
            ]))
            self.assertEqual(0, run_cli([
                "--state", state, "subscribe", "--id", "line",
                "--schemas", "s", "--must-keep", "phase"
            ]))
            self.assertEqual(0, run_cli([
                "--state", state, "publish", "--device", "d", "--seq", "1", "--rev", "1",
                "--schema", "s", "--body-json", json.dumps({"phase": "loaded"})
            ]))
            self.assertEqual(0, run_cli(["--state", state, "poll", "--id", "line", "--device", "d"]))
            hub = ContractHub.load(state)
            self.assertIn("line", hub.state["subscribers"])
            delivery = hub.state["subscriptions"]["line"]["devices"]["d"]["active_delivery_id"]
            self.assertEqual(delivery, "delivery:1")

    def test_cli_replay_supplied_trace(self):
        with tempfile.TemporaryDirectory() as directory:
            state = str(Path(directory) / "replay.json")
            rc = run_cli(["--state", state, "replay", str(DATA / "obsolete-in-flight.jsonl"),
                           "--quiet-ops"])
            self.assertEqual(rc, 0)
            hub = ContractHub.load(state)
            poll_result = hub.poll({"id": "line", "device": "m2"})
            # 状态文件中最后一次 poll 已产生 delivery:2 且仍活动；再次轮询应幂等。
            self.assertEqual(poll_result["status"], "delivered")
            self.assertTrue(poll_result["retry"])
            self.assertEqual(poll_result["delivery_id"], "delivery:2")


if __name__ == "__main__":
    unittest.main()
