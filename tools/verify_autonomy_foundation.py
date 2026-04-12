import argparse
import json
import sys
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "brain"))

from hydra_runtime import (  # noqa: E402
    SCHEMA_VERSION,
    ack_work_item,
    claim_next_work_item,
    db_connect,
    ensure_schema,
    enqueue_work_item,
    fail_work_item,
    queue_metrics,
    redis_client,
    requeue_item,
)


def purge_verification_rows(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM hydra_attempts WHERE lineage_id LIKE 'verify-%' OR lane='test'")
        cur.execute("DELETE FROM hydra_work_items WHERE lineage_id LIKE 'verify-%' OR lane='test'")
        cur.execute("DELETE FROM hydra_cycles WHERE lineage_id LIKE 'verify-%'")
    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic queue/runtime verification for HYDRA.")
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()

    conn = db_connect()
    cache = redis_client()
    ensure_schema(conn)
    purge_verification_rows(conn)

    lineage_id = f"verify-{uuid.uuid4().hex}"
    cycle_id = f"verify-{uuid.uuid4().hex}"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "lineage_id": lineage_id,
        "cycle_id": cycle_id,
        "lane": "test",
        "niche": {"name": "verification payload"},
    }

    item = enqueue_work_item(conn, cache, "test", lineage_id, cycle_id, payload, max_attempts=2)
    claimed = claim_next_work_item(conn, cache, "test", "verify-worker", lease_seconds=30)
    if not claimed or claimed["item_id"] != item["item_id"]:
        print("FAIL verify-claim: unable to claim newly enqueued item")
        return 1

    outcome_retry = fail_work_item(
        conn,
        cache,
        claimed,
        classification="provider_timeout",
        message="simulated timeout",
        retriable=True,
        blocked=False,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, next_attempt_at FROM hydra_work_items WHERE item_id=%s",
            (item["item_id"],),
        )
        retried_row = cur.fetchone()
        cur.execute(
            "UPDATE hydra_work_items SET next_attempt_at=NOW() WHERE item_id=%s",
            (item["item_id"],),
        )
    conn.commit()
    claimed_retry = claim_next_work_item(conn, cache, "test", "verify-worker", lease_seconds=30)
    if outcome_retry != "queued" or not retried_row or retried_row["status"] != "queued" or not claimed_retry:
        print("FAIL verify-retry: retriable item was not requeued")
        return 1

    outcome_dlq = fail_work_item(
        conn,
        cache,
        claimed_retry,
        classification="invalid_payload",
        message="simulated invalid payload",
        retriable=False,
        blocked=False,
    )
    if outcome_dlq != "dead_letter":
        print("FAIL verify-dlq: poison item did not move to dead_letter")
        return 1

    if not requeue_item(conn, cache, item["item_id"]):
        print("FAIL verify-replay: dead letter item did not requeue")
        return 1
    claimed_replayed = claim_next_work_item(conn, cache, "test", "verify-worker", lease_seconds=30)
    if not claimed_replayed:
        print("FAIL verify-replay: replayed item was not claimable")
        return 1
    ack_work_item(conn, claimed_replayed, response_payload={"verification": True})

    metrics = [row for row in queue_metrics(conn) if row["lane"] == "test"]
    print("PASS verify-claim: queued item claimed from durable queue")
    print("PASS verify-retry: retriable failure requeued without loss")
    print("PASS verify-dlq: poison item moved to dead_letter")
    print("PASS verify-replay: dead_letter item requeued and acked")
    print(f"QUEUE_METRICS={json.dumps(metrics, default=str)}")

    if args.cleanup:
        purge_verification_rows(conn)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
