from __future__ import annotations

from datetime import datetime, timedelta, timezone

from langchain_core.tools import BaseTool, tool

from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.audit.sink import AuditSink
from dp_ops_agent.evidence.schema import Signal
from dp_ops_agent.tools.kafka.gateway import KafkaMetricsGateway


def _record_signal(
    audit: AuditSink, session_id: str, collected_signals: list[Signal], signal: Signal
) -> str:
    """Log the signal to the audit trail, add it to this session's collected
    signals (so submit_diagnosis can assemble a grounded Diagnosis without
    the model having to restate signal payloads), and return the tool result
    text the model sees."""
    collected_signals.append(signal)
    audit.append(
        AuditEvent(
            event_type="signal_collected",
            timestamp=datetime.now(timezone.utc),
            session_id=session_id,
            actor="tool",
            payload=signal.model_dump(mode="json"),
        )
    )
    return signal.model_dump_json()


def build_kafka_tools(
    gateway: KafkaMetricsGateway,
    audit: AuditSink,
    session_id: str,
    collected_signals: list[Signal],
) -> list[BaseTool]:
    @tool
    async def under_replicated_partitions(topics: list[str]) -> str:
        """Report under-replicated/offline partitions for the given topics in the
        incident window. Signals a broker failure, disk pressure, or network
        partition."""
        now = datetime.now(timezone.utc)
        meta = gateway.cluster_metadata(topics)
        partitions = [
            {
                "topic": p.topic,
                "partition": p.id,
                "replicas": p.replicas,
                "isr": p.isr,
                "under_replicated": len(p.isr) < len(p.replicas),
                "offline": p.leader == -1,
            }
            for p in meta.partitions
        ]
        critical = any(p["under_replicated"] or p["offline"] for p in partitions)
        signal = Signal(
            tool="kafka.under_replicated_partitions",
            signal_type="under_replicated_partitions",
            collected_at=now,
            window_start=now,
            window_end=now,
            scope={"topics": ",".join(topics)},
            observed={"partitions": partitions},
            severity="critical" if critical else "ok",
            raw_source_ref="AdminClient.list_topics(metadata)",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    @tool
    async def isr_churn(broker_id: int, window_minutes: int) -> str:
        """Report ISR shrink/expand churn rate for a broker over the incident
        window. High churn signals a flaky broker, GC pauses, or network
        instability."""
        now = datetime.now(timezone.utc)
        metrics = gateway.broker_jmx_metrics(
            broker_id, ["IsrShrinksPerSec", "IsrExpandsPerSec"], window_minutes
        )
        shrinks = [s.value for s in metrics.get("IsrShrinksPerSec", [])]
        expands = [s.value for s in metrics.get("IsrExpandsPerSec", [])]
        max_shrink_rate = max(shrinks, default=0.0)
        signal = Signal(
            tool="kafka.isr_churn",
            signal_type="isr_churn",
            collected_at=now,
            window_start=now - timedelta(minutes=window_minutes),
            window_end=now,
            scope={"broker_id": str(broker_id)},
            observed={
                "isr_shrinks_per_sec": shrinks,
                "isr_expands_per_sec": expands,
                "max_shrink_rate": max_shrink_rate,
            },
            severity="critical" if max_shrink_rate > 0.5 else ("warn" if max_shrink_rate > 0 else "ok"),
            raw_source_ref=f"jmx:kafka.server:type=ReplicaManager,broker={broker_id}",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    @tool
    async def consumer_lag_trend(group: str, topic: str) -> str:
        """Report consumer lag (high watermark minus committed offset) per
        partition for a consumer group/topic pair. Growing lag means processing
        can't keep up; flat-high means a stuck consumer; a sudden spike means an
        upstream burst."""
        now = datetime.now(timezone.utc)
        offsets = gateway.consumer_group_offsets(group)
        watermarks = gateway.topic_high_watermarks(topic)
        lag_by_partition = {
            str(pid): watermarks[pid] - offsets.get(pid, 0)
            for pid in watermarks
        }
        max_lag = max(lag_by_partition.values(), default=0)
        signal = Signal(
            tool="kafka.consumer_lag_trend",
            signal_type="consumer_lag_trend",
            collected_at=now,
            window_start=now,
            window_end=now,
            scope={"group": group, "topic": topic},
            observed={"lag_by_partition": lag_by_partition, "max_lag": max_lag},
            severity="critical" if max_lag > 10_000 else ("warn" if max_lag > 1_000 else "ok"),
            raw_source_ref="AdminClient consumer offsets + watermark offsets",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    @tool
    async def rebalance_frequency(group: str, window_minutes: int) -> str:
        """Report consumer group state transitions over the window to detect
        rebalance storms. Frequent rebalancing signals a session-timeout
        misconfig, a slow poll loop, or a crash-looping consumer."""
        now = datetime.now(timezone.utc)
        history = gateway.consumer_group_state_history(group, window_minutes)
        rebalance_states = {"PreparingRebalance", "CompletingRebalance"}
        rebalance_count = sum(1 for h in history if h.get("state") in rebalance_states)
        signal = Signal(
            tool="kafka.rebalance_frequency",
            signal_type="rebalance_frequency",
            collected_at=now,
            window_start=now - timedelta(minutes=window_minutes),
            window_end=now,
            scope={"group": group},
            observed={"state_history": history, "rebalance_count": rebalance_count},
            severity="critical" if rebalance_count > 5 else ("warn" if rebalance_count > 1 else "ok"),
            raw_source_ref="AdminClient.describe_consumer_groups",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    @tool
    async def hot_partition_skew(topic: str, window_minutes: int) -> str:
        """Report per-partition throughput to detect hot-partition/key skew.
        High skew signals a poor partition key choice or a single noisy
        producer."""
        now = datetime.now(timezone.utc)
        throughput = gateway.partition_throughput(topic, window_minutes)
        values = list(throughput.values())
        avg = sum(values) / len(values) if values else 0.0
        max_val = max(values, default=0.0)
        skew_ratio = (max_val / avg) if avg > 0 else 0.0
        signal = Signal(
            tool="kafka.hot_partition_skew",
            signal_type="hot_partition_skew",
            collected_at=now,
            window_start=now - timedelta(minutes=window_minutes),
            window_end=now,
            scope={"topic": topic},
            observed={
                "throughput_by_partition": {str(k): v for k, v in throughput.items()},
                "avg_throughput": avg,
                "skew_ratio": skew_ratio,
            },
            severity="critical" if skew_ratio > 5 else ("warn" if skew_ratio > 2 else "ok"),
            raw_source_ref="jmx:kafka_topic_partition_bytesinpersec",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    @tool
    async def schema_registry_compat(subject: str) -> str:
        """Check whether a schema-registry subject's latest version has a
        compatibility failure. A failure signals a producer shipped an
        incompatible schema change."""
        now = datetime.now(timezone.utc)
        result = gateway.schema_registry_subject(subject)
        is_compatible = result.get("is_compatible", True) if result else True
        signal = Signal(
            tool="kafka.schema_registry_compat",
            signal_type="schema_registry_compat",
            collected_at=now,
            window_start=now,
            window_end=now,
            scope={"subject": subject},
            observed={"raw": result, "is_compatible": is_compatible},
            severity="ok" if is_compatible else "critical",
            raw_source_ref=f"schema-registry:/compatibility/subjects/{subject}/versions/latest",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    return [
        under_replicated_partitions,
        isr_churn,
        consumer_lag_trend,
        rebalance_frequency,
        hot_partition_skew,
        schema_registry_compat,
    ]
