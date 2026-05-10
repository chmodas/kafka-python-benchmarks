"""Topic creation/deletion via confluent-kafka's AdminClient.

Out-of-band relative to whichever client is being benchmarked, so the choice
of admin library has no fairness implication.
"""

from __future__ import annotations

import logging
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger(__name__)


def _admin(bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap})


def create_topic(bootstrap: str, name: str, partitions: int, replication: int = 1) -> None:
    admin = _admin(bootstrap)
    fut = admin.create_topics(
        [NewTopic(name, num_partitions=partitions, replication_factor=replication)],
        operation_timeout=30,
    )
    fut[name].result()
    # AdminClient is async-by-default; poll metadata until the topic is visible.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        md = admin.list_topics(timeout=5)
        if name in md.topics and md.topics[name].error is None:
            return
        time.sleep(0.1)
    raise TimeoutError(f"topic {name} did not appear in metadata within 15s")


def delete_topic(bootstrap: str, name: str) -> None:
    admin = _admin(bootstrap)
    fut = admin.delete_topics([name], operation_timeout=30)
    try:
        fut[name].result()
    except Exception as exc:
        # Collisions from re-runs would be visible here; log but don't fail
        # because the next repeat uses a fresh uuid-suffixed topic name.
        log.warning("delete_topic(%s) failed: %r", name, exc)


def topic_exists(bootstrap: str, name: str) -> bool:
    admin = _admin(bootstrap)
    md = admin.list_topics(timeout=5)
    return name in md.topics and md.topics[name].error is None


def delete_consumer_group(bootstrap: str, group_id: str) -> None:
    """Delete a consumer group so its member/offset metadata doesn't linger.

    Each benchmark repeat uses a uuid-suffixed group_id; without explicit
    cleanup, the broker's `__consumer_offsets` topic accumulates one group's
    worth of metadata per repeat. Over a long sweep this both bloats memory
    (the group coordinator keeps groups in-heap) and slows broker restarts.
    """
    admin = _admin(bootstrap)
    fut = admin.delete_consumer_groups([group_id], request_timeout=15)
    try:
        fut[group_id].result()
    except KafkaException as exc:
        # GROUP_ID_NOT_FOUND is expected when the broker auto-expired the group
        # (e.g. via short offsets.retention.minutes) or when the consumer never
        # successfully joined the group. Either way, nothing to clean up.
        if (
            exc.args
            and isinstance(exc.args[0], KafkaError)
            and exc.args[0].code() == KafkaError.GROUP_ID_NOT_FOUND
        ):
            return
        log.warning("delete_consumer_group(%s) failed: %r", group_id, exc)
    except Exception as exc:
        log.warning("delete_consumer_group(%s) failed: %r", group_id, exc)
