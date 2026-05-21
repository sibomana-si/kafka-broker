from dataclasses import dataclass
from typing import List, Optional


@dataclass
class RequestHeader:
    api_key: int
    api_version: int
    correlation_id: int
    client_id: Optional[str] = None


@dataclass
class DescribeTopicPartitionsRequestTopic:
    name: str


@dataclass
class DescribeTopicPartitionsRequest:
    topics: List[DescribeTopicPartitionsRequestTopic]


@dataclass
class FetchRequestPartition:
    partition_index: int
    current_leader_epoch: int
    fetch_offset: int
    last_fetched_epoch: int
    log_start_offset: int
    partition_max_bytes: int


@dataclass
class FetchRequestTopic:
    topic_id: bytes
    partitions: List[FetchRequestPartition]


@dataclass
class FetchRequest:
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: List[FetchRequestTopic]


@dataclass
class ProduceRequestPartition:
    partition_index: int
    record_batch: bytes


@dataclass
class ProduceRequestTopic:
    name: str
    partitions: List[ProduceRequestPartition]


@dataclass
class ProduceRequest:
    transactional_id: Optional[str]
    acks: int
    timeout_ms: int
    topics: List[ProduceRequestTopic]
