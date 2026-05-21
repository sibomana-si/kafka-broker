from src.protocol.reader import BufferReader
from src.protocol.messages import (
    RequestHeader,
    DescribeTopicPartitionsRequest,
    DescribeTopicPartitionsRequestTopic,
    FetchRequest,
    FetchRequestTopic,
    FetchRequestPartition,
    ProduceRequest,
    ProduceRequestTopic,
    ProduceRequestPartition
)


def parse_request_header(reader: BufferReader) -> RequestHeader:
    api_key = reader.read_int16()
    api_version = reader.read_int16()
    correlation_id = reader.read_int32()
    client_id_length = reader.read_int16()

    client_id = None
    if client_id_length > 0:
        client_id = reader.read_bytes(client_id_length).decode("utf-8")

    reader.read_tag_buffer()

    return RequestHeader(api_key, api_version, correlation_id, client_id)

def parse_describe_topic_partitions_request(reader: BufferReader) -> DescribeTopicPartitionsRequest:
    num_topics = reader.read_compact_array_length()
    topics = []
    for _ in range(num_topics):
        topic_name = reader.read_compact_string()
        reader.read_tag_buffer()
        topics.append(DescribeTopicPartitionsRequestTopic(topic_name))

    reader.read_int32() # response_partition_limit
    reader.read_int8() # cursor
    reader.read_tag_buffer()

    return DescribeTopicPartitionsRequest(topics)

def parse_fetch_request(reader: BufferReader) -> FetchRequest:
    max_wait_ms = reader.read_int32()
    min_bytes = reader.read_int32()
    max_bytes = reader.read_int32()
    isolation_level = reader.read_int8()
    session_id = reader.read_int32()
    session_epoch = reader.read_int32()

    num_topics = reader.read_compact_array_length()
    topics: list[FetchRequestTopic] = []
    for _ in range(num_topics):
        topic_id = reader.read_uuid()
        num_partitions = reader.read_compact_array_length()
        partitions: list[FetchRequestPartition] = []
        for _ in range(num_partitions):
            partition_index = reader.read_int32()
            current_leader_epoch = reader.read_int32()
            fetch_offset = reader.read_int64()
            last_fetched_epoch = reader.read_int32()
            log_start_offset = reader.read_int64()
            partition_max_bytes = reader.read_int32()
            reader.read_tag_buffer()
            partitions.append(FetchRequestPartition(
                partition_index, current_leader_epoch, fetch_offset,
                last_fetched_epoch, log_start_offset, partition_max_bytes
            ))
        reader.read_tag_buffer()
        topics.append(FetchRequestTopic(topic_id, partitions))

    return FetchRequest(max_wait_ms, min_bytes, max_bytes, isolation_level, session_id, session_epoch, topics)

def parse_produce_request(reader: BufferReader) -> ProduceRequest:
    transactional_id_len = reader.read_int8()
    transactional_id = None
    if transactional_id_len > 0:
        transactional_id = reader.read_bytes(transactional_id_len).decode("utf-8")

    acks = reader.read_int16()
    timeout_ms = reader.read_int32()

    num_topics = reader.read_compact_array_length()
    topics: list[ProduceRequestTopic] = []
    for _ in range(num_topics):
        topic_name_len = reader.read_compact_array_length()
        topic_name = reader.read_bytes(topic_name_len).decode("utf-8")

        num_partitions =  reader.read_compact_array_length()
        partitions: list[ProduceRequestPartition] = []
        for _ in range(num_partitions):
            partition_index = reader.read_int32()
            record_batch_len = reader.read_compact_array_length()
            record_batch = reader.read_bytes(record_batch_len)
            reader.read_tag_buffer()
            partitions.append(ProduceRequestPartition(partition_index, record_batch))

        reader.read_tag_buffer()
        topics.append(ProduceRequestTopic(topic_name, partitions))

    return ProduceRequest(transactional_id, acks, timeout_ms, topics)
