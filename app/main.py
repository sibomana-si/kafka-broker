import asyncio
import logging
import uuid
from asyncio import StreamReader, StreamWriter, Server
from pathlib import Path
from sys import byteorder
from typing import Any


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

PRODUCE_KEY = 0
FETCH_KEY = 1
API_VERSIONS_KEY = 18
DESCRIBE_TOPIC_PARTITIONS_KEY = 75
CLUSTER_METADATA_FILE = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
LOG_FILES_DIR = "/tmp/kraft-combined-logs"
LOG_FILE_NAME = "00000000000000000000.log"


def produce_topic_response(client_request: bytes,
                           topic_index: int,
                           topics: dict,
                           ) -> tuple[bytes, int]:

    tag_buffer = int(0).to_bytes(1, byteorder='big')
    topic_name_size = int.from_bytes(client_request[topic_index: topic_index + 1], byteorder='big')
    topic_name = client_request[topic_index + 1: topic_index + topic_name_size].decode("utf-8")

    partition_array_index = topic_index + topic_name_size
    partitions_array_length = client_request[partition_array_index: partition_array_index + 1]
    partitions_resp_array = partitions_array_length
    partitions_array_size = int.from_bytes(partitions_array_length, byteorder='big') - 1
    partition_id_index = partition_array_index + 1

    for _ in range(partitions_array_size):
        #logger.info(f"partition_array_index: {partition_id_index} | {partitions_array_size}")
        partition_resp, next_partition_id_index = produce_partition_response(client_request, partition_id_index, topic_name,
                                                                         topics)
        #logger.info(f"partition_resp: {partition_resp.hex()}")
        partitions_resp_array += partition_resp
        partition_id_index = next_partition_id_index

    #logger.info(f"partitions_resp_array: {partitions_resp_array.hex()}")

    topics_resp = topic_name_size.to_bytes(1, byteorder='big') + topic_name.encode('utf-8') \
                  + partitions_resp_array + tag_buffer
    #logger.info(f"topics resp: {len(topics_resp)}|{topics_resp.hex()}")

    topics_next_index = partition_id_index + 1

    return topics_resp, topics_next_index


def produce_partition_response(client_request: bytes,
                               partition_id_index: int,
                               topic_name: str,
                               topics: dict
                               ) -> tuple[bytes, int]:

    tag_buffer = int(0).to_bytes(1, byteorder='big')
    partition_id_size_length = 4
    partition_index = client_request[partition_id_index: partition_id_index + partition_id_size_length]
    #logger.info(f"partition_index: {int.from_bytes(partition_index, byteorder='big')}")

    record_batch_array_index = partition_id_index + partition_id_size_length
    record_batch_size_length = varint_encoding_size(client_request, record_batch_array_index + 1)
    #logger.info(f"record_batch_size_length: {record_batch_size_length}")
    record_batch_size = int.from_bytes(
        client_request[record_batch_array_index: record_batch_array_index + record_batch_size_length],
        byteorder='big')
    #logger.info(f"record_batch_size: {record_batch_size}")
    record_batch_index = record_batch_array_index + record_batch_size_length
    record_batch = client_request[record_batch_index: record_batch_index + record_batch_size]
    #logger.info(f"record_batch: {len(record_batch)}|{record_batch.hex()}")

    valid_topic_and_partition = False

    if topic_name in topics:
        partition_idx = int.from_bytes(partition_index, byteorder='big')
        for partition in topics[topic_name]["partitions"]:
            if partition_idx == partition["partition_index"]:
                valid_topic_and_partition = True
                #logger.info(f"Valid topic and partition. Topic: {topic_name}, Partition: {partition_idx}")
                break

    if valid_topic_and_partition:
        error_code = int(0).to_bytes(2, byteorder='big')
        base_offset = int(0).to_bytes(8, byteorder='big')
        log_start_offset = int(0).to_bytes(8, byteorder='big')

        record_batch_log_file = LOG_FILES_DIR \
                                + "/" + topic_name + "-" + str(int.from_bytes(partition_index, byteorder='big')) \
                                + "/" + LOG_FILE_NAME

        with open(record_batch_log_file, 'wb') as log_file:
            bytes_written = log_file.write(record_batch)
            #logger.info(f"Wrote {bytes_written} to record batch log file: {record_batch_log_file}")

    else:
        error_code = int(3).to_bytes(2, byteorder='big')
        base_offset = int(-1).to_bytes(8, byteorder='big', signed=True)
        log_start_offset = int(-1).to_bytes(8, byteorder='big', signed=True)

    log_append_time = int(-1).to_bytes(8, byteorder='big', signed=True)
    records_array = int(1).to_bytes(1, byteorder='big')
    error_message = int(0).to_bytes(1, byteorder='big')

    partition_resp = partition_index + error_code + base_offset + log_append_time \
                     + log_start_offset + records_array + error_message + tag_buffer
    next_index = partition_id_index + partition_id_size_length + record_batch_size_length + record_batch_size

    return partition_resp, next_index


def encode_kafka_unsigned_varint(value: int) -> bytes:
    """Encode an integer as Kafka UVARINT (unsigned LEB128)."""
    if value < 0:
        raise ValueError("UVARINT cannot encode negative values")
    encoded = bytearray()
    while True:
        to_write = value & 0x7F
        value >>= 7
        if value:
            encoded.append(to_write | 0x80)
        else:
            encoded.append(to_write)
            break
    return bytes(encoded)


def varint_encoding_size(request: bytes, index: int) -> int:
    num_bytes = 1
    while int.from_bytes(request[index: index + 1], byteorder='big') != 0:
        num_bytes += 1
        index += 1
    return num_bytes


def get_response_topic_data(request_topic: str, topics: dict[str, dict[str, Any]]) -> bytes:
    tag_buffer = int(0).to_bytes(1, byteorder='big')
    is_internal = int(0).to_bytes(1, byteorder='big')
    topic_authorized_operations = int(0).to_bytes(4, byteorder='big')
    partition_data = b""

    if request_topic not in topics:
        resp_topic_id = int(0).to_bytes(16, byteorder='big')
        resp_topic_error_code = int(3).to_bytes(2, byteorder='big')
        partition_array_size = int(1).to_bytes(1, byteorder='big')
        partition_array = partition_array_size
    else:
        resp_topic_details = topics[request_topic]
        resp_topic_id = resp_topic_details["topic_uuid"].bytes
        resp_topic_error_code = int(0).to_bytes(2, byteorder='big')

        broker = int(1).to_bytes(1, byteorder='big')
        elr = int(1).to_bytes(1, byteorder='big')
        last_elr = int(1).to_bytes(1, byteorder='big')
        offline_replicas = int(1).to_bytes(1, byteorder='big')

        partition_array_size = (len(resp_topic_details["partitions"]) + 1).to_bytes(1, byteorder='big')

        for partition in resp_topic_details["partitions"]:
            partition_index = partition["partition_index"].to_bytes(4, byteorder='big')
            leader_id = partition["leader_id"].to_bytes(4, byteorder='big')
            leader_epoch = partition["leader_epoch"].to_bytes(4, byteorder='big')
            replica_nodes = partition["num_replicas"].to_bytes(4, byteorder='big')
            isr_nodes = partition["num_isr"].to_bytes(4, byteorder='big')

            partition_data += (resp_topic_error_code + partition_index + leader_id + leader_epoch + replica_nodes + broker
                               + isr_nodes + broker + elr + last_elr + offline_replicas + tag_buffer)

        partition_array = partition_array_size + partition_data

    resp_topic_name = request_topic.encode("utf-8")
    resp_topic_name_length = int(len(resp_topic_name) + 1).to_bytes(1, byteorder='big')
    resp_topic_data = resp_topic_error_code + resp_topic_name_length + resp_topic_name + resp_topic_id \
                      + is_internal + partition_array + topic_authorized_operations + tag_buffer

    return resp_topic_data


def extract_record_batch(cluster_metadata: bytes, record_batch_index: int) -> bytes:
    record_batch_size = 12 + int.from_bytes(cluster_metadata[record_batch_index + 8:
                                                             record_batch_index + 12],
                                            byteorder='big')
    return cluster_metadata[record_batch_index: record_batch_index + record_batch_size]


def process_record_batch(record_batch: bytes) -> list:
    record_list: list = []
    num_records = int.from_bytes(record_batch[57:61], byteorder='big')
    record_array = record_batch[61:]
    for _ in range(num_records):
        record_size = int.from_bytes(record_array[0:1], byteorder='big') // 2
        if int.from_bytes(record_array[1:2], byteorder='big') == 0:
            record = record_array[7:(record_size + 1)]
            next_record_index = record_size + 1
        else:
            record = record_array[9: (record_size + 2)]
            next_record_index = record_size + 2
        record_list.append(record)
        record_array = record_array[next_record_index:]
    return record_list


def process_record(record: bytes) -> dict[str, Any]:
    processed_record: dict[str, Any] = dict()
    record_type = int.from_bytes(record[0:1], byteorder='big')

    if record_type == 2:
        processed_record["type"] = "topic"
        processed_record["partitions"] = []

        topic_name_length = int.from_bytes(record[2:3], byteorder='big') - 1
        topic_name = record[3: 3 + topic_name_length].decode("utf-8")
        processed_record["topic_name"] = topic_name

        topic_uuid_index = 3 + topic_name_length
        topic_uuid = uuid.UUID(bytes=record[topic_uuid_index: topic_uuid_index + 16])
        processed_record["topic_uuid"] = topic_uuid
    elif record_type == 3:
        processed_record["type"] = "partition"

        partition_index = int.from_bytes(record[2:6], byteorder='big')
        processed_record["partition_index"] = partition_index

        topic_uuid = uuid.UUID(bytes=record[6:22])
        processed_record["topic_uuid"] = topic_uuid

        num_replicas = int.from_bytes(record[22:23], byteorder='big')
        processed_record["num_replicas"] = num_replicas

        num_isr = int.from_bytes(record[27:28], byteorder='big')
        processed_record["num_isr"] = num_isr

        leader_id = int.from_bytes(record[34:38], byteorder='big')
        processed_record["leader_id"] = leader_id

        leader_epoch = int.from_bytes(record[38:42], byteorder='big')
        processed_record["leader_epoch"] = leader_epoch
    else:
        processed_record["type"] = "unknown"
    return processed_record


def parse_cluster_metadata(cluster_metadata: bytes) -> dict:
    topics: dict[str, dict[str, Any]] = dict()
    record_batch_index = 0
    cluster_metadata_len = len(cluster_metadata)

    while record_batch_index < cluster_metadata_len:
        record_batch = extract_record_batch(cluster_metadata, record_batch_index)
        #logger.info(f"record_batch: {record_batch_index}|{len(record_batch)}|{record_batch.hex()}")
        record_list = process_record_batch(record_batch)
        #logger.info(f"record_list size: {len(record_list)}")
        for record in record_list:
            #logger.info(f"record: {len(record)}|{record.hex()}")
            processed_record = process_record(record)
            if processed_record["type"] == "topic":
                #logger.info(f"topic: {processed_record}")
                topics[processed_record["topic_name"]] = processed_record
            elif processed_record["type"] == "partition":
                #logger.info(f"partition: {processed_record}")
                for topic in topics:
                    if processed_record["topic_uuid"] == topics[topic]["topic_uuid"]:
                        topics[topic]["partitions"].append(processed_record)
                        break
                else:
                    raise Exception(f"partition with no associated topic!|{processed_record}")
            else:
                logger.info(f'unknown record|{processed_record["type"]}|{record}')
        record_batch_index += len(record_batch)
    return topics


def handle_api_version_requests(client_request: bytes) -> bytes:
    request_api_version = int.from_bytes(client_request[6:8], byteorder='big')

    if request_api_version in (0, 1, 2, 3, 4):
        error_code = int(0).to_bytes(2, byteorder='big')
        api_key_array_length = int(5).to_bytes(1, byteorder='big')
        api_key_min = int(0).to_bytes(2, byteorder='big')
        tag_buffer = int(0).to_bytes(1, byteorder='big')
        throttle_time = int(0).to_bytes(4, byteorder='big')

        # Produce API
        produce_api_key = int(0).to_bytes(2, byteorder='big')
        produce_api_key_max = int(11).to_bytes(2, byteorder='big')

        # Fetch API
        fetch_api_key = int('1').to_bytes(2, byteorder='big')
        fetch_api_key_max = int(16).to_bytes(2, byteorder='big')

        # ApiVersions API
        versions_api_key = int('18').to_bytes(2, byteorder='big')
        versions_api_key_max = int(4).to_bytes(2, byteorder='big')

        # DescribeTopicPartitions API
        topics_api_key = int('75').to_bytes(2, byteorder='big')
        topics_api_key_max = int(0).to_bytes(2, byteorder='big')


        resp_body = error_code + api_key_array_length \
                    + versions_api_key + api_key_min + versions_api_key_max + tag_buffer \
                    + topics_api_key + api_key_min + topics_api_key_max + tag_buffer \
                    + fetch_api_key + api_key_min + fetch_api_key_max + tag_buffer \
                    + produce_api_key + api_key_min + produce_api_key_max + tag_buffer \
                    + throttle_time + tag_buffer
    else:
        error_code = int(35).to_bytes(2, byteorder='big')
        resp_body = error_code

    return resp_body


def handle_describe_topic_partition_requests(client_request: bytes) -> bytes:
    #logger.info(f"client request: {client_request.hex()}")
    if Path(CLUSTER_METADATA_FILE).is_file():
        logger.info("Loading Cluster metadata file")
        with open(CLUSTER_METADATA_FILE, 'rb') as f:
            cluster_metadata = f.read()
            logger.info(f"Cluster metadata: {len(cluster_metadata)}|{cluster_metadata.hex()}")
            topics = parse_cluster_metadata(cluster_metadata)
            logger.info(f"Cluster metadata file loaded successfully. Topics: {topics}")
    else:
        logger.info("Cluster metadata file not found.")


    client_id_length = int.from_bytes(client_request[12:14], byteorder='big')

    field_sizes = {
        "message": 4,
        "api_key": 2,
        "api_version": 2,
        "correlation_id": 4,
        "client_id": 2,
        "tag_buffer": 1,
        "topics_array": 1,
        "topic_name": 1
    }

    tag_buffer = int(0).to_bytes(1, byteorder='big')
    throttle_time = int(0).to_bytes(4, byteorder='big')
    next_cursor = int(255).to_bytes(1, byteorder='big')

    topic_array_index = field_sizes["message"] + field_sizes["api_key"] + field_sizes["api_version"] \
                              + field_sizes["correlation_id"] + field_sizes["client_id"] \
                              + client_id_length + field_sizes["tag_buffer"]

    topics_array_length = client_request[topic_array_index: topic_array_index + 1]

    request_topics = []
    topic_index = topic_array_index + 1

    for _ in range(int.from_bytes(topics_array_length, byteorder='big') - 1):
        topic_name_length = int.from_bytes(client_request[topic_index: topic_index + 1], byteorder='big')
        topic_name = client_request[topic_index + 1: topic_index + topic_name_length].decode("utf-8")
        request_topics.append(topic_name)
        topic_index += topic_name_length + 1

    resp_topics_array = topics_array_length

    for request_topic in sorted(request_topics):
        #logger.info(f"request_topic: {request_topic}")
        resp_topic_data = get_response_topic_data(request_topic, topics)
        #logger.info(f"resp_topic_data: {len(resp_topic_data)}|{resp_topic_data.hex()}")
        resp_topics_array += resp_topic_data


    resp_body = tag_buffer + throttle_time + resp_topics_array + next_cursor + tag_buffer

    #logger.info(f"resp_body: {len(resp_body)}|{resp_body.hex()}")
    return resp_body


def handle_fetch_requests(client_request: bytes) -> bytes:
    #logger.info(f"client request: {len(client_request)}|{client_request.hex()}")
    if Path(CLUSTER_METADATA_FILE).is_file():
        logger.info("Loading Cluster metadata file")
        with open(CLUSTER_METADATA_FILE, 'rb') as f:
            cluster_metadata = f.read()
            logger.info(f"Cluster metadata: {len(cluster_metadata)}|{cluster_metadata.hex()}")
            topics = parse_cluster_metadata(cluster_metadata)
            logger.info(f"Cluster metadata file loaded successfully. Topics: {topics}")
    else:
        logger.info("Cluster metadata file not found.")

    client_id_length = int.from_bytes(client_request[12:14], byteorder='big')

    field_sizes = {
        "message": 4,
        "api_key": 2,
        "api_version": 2,
        "correlation_id": 4,
        "client_id": 2,
        "tag_buffer": 1,
        "max_wait_ms": 4,
        "min_bytes": 4,
        "max_bytes": 4,
        "isolation_level": 1,
        "session_id": 4,
        "session_epoch": 4
    }

    session_id_index = field_sizes["message"] + field_sizes["api_key"] + field_sizes["api_version"] \
                        + field_sizes["correlation_id"] + field_sizes["client_id"] \
                        + client_id_length + field_sizes["tag_buffer"] \
                        + field_sizes["max_wait_ms"] + field_sizes["min_bytes"] + field_sizes["max_bytes"] \
                        + field_sizes["isolation_level"]

    topics_array_index =  session_id_index + field_sizes["session_id"] + field_sizes["session_epoch"]

    session_id = client_request[session_id_index: session_id_index + 4]
    topics_array_length = client_request[topics_array_index: topics_array_index + 1]
    topic_uuid = client_request[topics_array_index + 1: topics_array_index + 17]
    partitions_array_length = client_request[topics_array_index + 17: topics_array_index + 18]
    partition_index = client_request[topics_array_index + 18: topics_array_index + 22]

    tag_buffer = int(0).to_bytes(1, byteorder='big')
    throttle_time = int(0).to_bytes(4, byteorder='big')
    error_code = int(0).to_bytes(2, byteorder='big')

    for topic in topics:
        if topics[topic]["topic_uuid"] == uuid.UUID(bytes=topic_uuid):
            #logger.info(f"request topic: {topic}")
            partition_error_code = int(0).to_bytes(2, byteorder='big')
            record_batch_log_dir = LOG_FILES_DIR + "/" + topic \
                                   + "-" + str(int.from_bytes(partition_index, byteorder='big')) \
                                   + "/"
            record_batch_log = record_batch_log_dir + LOG_FILE_NAME
            partition_records_array = b""
            if Path(record_batch_log).is_file() and Path(record_batch_log).stat().st_size > 0:
                #logger.info(f"record batch log file found: {record_batch_log}")
                with open(record_batch_log, 'rb') as log_file:
                    partition_records_array = log_file.read()

            partition_records_array = encode_kafka_unsigned_varint(len(partition_records_array) + 1) \
                                      + partition_records_array
            #logger.info(f"partition_records_array: {len(partition_records_array)}|{partition_records_array.hex()}")
            break
    else:
        partition_error_code = int(100).to_bytes(2, byteorder='big')
        partition_records_array = int(0).to_bytes(1, byteorder='big')

    partition_high_watermark = int(0).to_bytes(8, byteorder='big')
    partitions_last_stable_offset = int(0).to_bytes(8, byteorder='big')
    partition_log_start_offset = int(0).to_bytes(8, byteorder='big')
    partition_aborted_transactions = int(1).to_bytes(1, byteorder='big')
    partition_preferred_read_replica = int(0).to_bytes(4, byteorder='big')
    partition_diverging_epoch_array = int(0).to_bytes(1, byteorder='big')
    partition_current_leader_array = int(0).to_bytes(1, byteorder='big')
    partition_snapshot_id_array = int(0).to_bytes(1, byteorder='big')

    partitions_array = partitions_array_length + partition_index + partition_error_code \
                       + partition_high_watermark + partitions_last_stable_offset \
                       + partition_log_start_offset + partition_aborted_transactions \
                       + partition_preferred_read_replica + partition_records_array \
                       + partition_diverging_epoch_array + partition_current_leader_array \
                       + partition_snapshot_id_array + tag_buffer
    #logger.info(f"partitions_array: {len(partitions_array)}|{partitions_array.hex()}")
    topics_array = topics_array_length + topic_uuid + partitions_array + tag_buffer
    #logger.info(f"topics_array: {len(topics_array)}|{topics_array.hex()}")
    node_endpoints_array = int(1).to_bytes(1, byteorder='big')

    resp_body = tag_buffer + throttle_time + error_code + session_id \
                + topics_array  + node_endpoints_array + tag_buffer

    #logger.info(f"fetch resp: {len(resp_body)}|{resp_body.hex()}")
    return resp_body


def handle_produce_requests(client_request: bytes) -> bytes:
    #logger.info(f"client_request: {client_request.hex()}")
    if Path(CLUSTER_METADATA_FILE).is_file():
        logger.info("Loading Cluster metadata file")
        with open(CLUSTER_METADATA_FILE, 'rb') as f:
            cluster_metadata = f.read()
            logger.info(f"Cluster metadata: {len(cluster_metadata)}|{cluster_metadata.hex()}")
            topics = parse_cluster_metadata(cluster_metadata)
            logger.info(f"Cluster metadata file loaded successfully. Topics: {topics}")
    else:
        logger.info("Cluster metadata file not found.")

    client_id_length = int.from_bytes(client_request[12:14], byteorder='big')

    field_sizes = {
        "message": 4,
        "api_key": 2,
        "api_version": 2,
        "correlation_id": 4,
        "client_id": 2,
        "tag_buffer": 1,
        "transactional_id": 1,
        "required_acks": 2,
        "timeout": 4
    }

    topics_array_index = field_sizes["message"] + field_sizes["api_key"] + field_sizes["api_version"] \
                        + field_sizes["correlation_id"] + field_sizes["client_id"] \
                        + client_id_length + field_sizes["tag_buffer"] \
                        + field_sizes["transactional_id"] + field_sizes["required_acks"] + field_sizes["timeout"]

    topics_array_length = client_request[topics_array_index: topics_array_index + 1]
    topics_resp_array = topics_array_length
    topics_array_size = int.from_bytes(topics_array_length, byteorder='big') - 1
    topic_index = topics_array_index + 1

    for _ in range(topics_array_size):
        #logger.info(f"topic_index: {topic_index}|{topics_array_size}")
        topics_response, topics_next_index = produce_topic_response(client_request, topic_index, topics)
        #logger.info(f"topics resp: {len(topics_response)}|{topics_response.hex()}")
        topics_resp_array += topics_response
        topic_index = topics_next_index


    tag_buffer = int(0).to_bytes(1, byteorder='big')
    throttle_time = int(0).to_bytes(4, byteorder='big')

    resp_body = tag_buffer + topics_resp_array + throttle_time + tag_buffer
    #logger.info(f"produce resp: {len(resp_body)}|{resp_body.hex()}")
    return resp_body


async def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
    client_address: str = writer.get_extra_info('peername')
    logger.info(f"Connection accepted from {client_address}")

    api_handlers = {
        API_VERSIONS_KEY: handle_api_version_requests,
        DESCRIBE_TOPIC_PARTITIONS_KEY: handle_describe_topic_partition_requests,
        FETCH_KEY: handle_fetch_requests,
        PRODUCE_KEY: handle_produce_requests
    }

    try:
        while True:
            client_request: bytes = await reader.read(1024)
            request_api_key = int.from_bytes(client_request[4:6], byteorder='big')
            correlation_id = client_request[8:12]
            resp_body = b''

            if request_api_key in api_handlers:
                resp_body = api_handlers[request_api_key](client_request)
            else:
                logger.error(f"Unsupported API: {request_api_key}|{client_request.hex()}")

            msg_size = len(correlation_id) + len(resp_body)
            resp_msg_size = int(msg_size).to_bytes(4, byteorder='big')
            resp = resp_msg_size + correlation_id + resp_body
            writer.write(resp)
            await writer.drain()
    except Exception as e:
        logger.exception(f"Error in client_handler: {client_address}|{e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    host_ip = "localhost"
    host_port = 9092
    server: Server = await asyncio.start_server(client_connected_cb=client_handler,
                                                host=host_ip,
                                                port=host_port,
                                                reuse_port=True)
    logger.info(server)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
