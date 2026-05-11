import asyncio
import logging
import uuid
from asyncio import StreamReader, StreamWriter, Server
from pathlib import Path
from typing import Any


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

PRODUCE_KEY = 0
FETCH_KEY = 1
API_VERSIONS_KEY = 18
DESCRIBE_TOPIC_PARTITIONS_KEY = 75
LOG_FILES_DIR = "/tmp/kraft-combined-logs"
LOG_FILE_NAME = "00000000000000000000.log"


async def produce_topic_response(client_request: bytes,
                           topic_index: int,
                           topics: dict,
                           ) -> tuple[bytes, int]:
    """
    Generate a response for a specific topic based on the given client request, topic index, and topic metadata.

    This function processes client request data to extract topic and partition
    information, generates responses for individual partitions using a corresponding
    helper function, and constructs a full response for the specified topic.

    :param client_request: The raw client request data represented as bytes.
    :param topic_index: The index in the client request where the topic information begins.
    :param topics: A dictionary containing metadata about available topics and partitions.

    :return: A tuple consisting of the topic response as bytes and the next index
             in the client request to process after handling the topic.
    """

    tag_buffer = int(0).to_bytes(1, byteorder='big')
    topic_name_size = int.from_bytes(client_request[topic_index: topic_index + 1], byteorder='big')
    topic_name = client_request[topic_index + 1: topic_index + topic_name_size].decode("utf-8")

    partition_array_index = topic_index + topic_name_size
    partitions_array_length = client_request[partition_array_index: partition_array_index + 1]
    partitions_resp_array = partitions_array_length
    partitions_array_size = int.from_bytes(partitions_array_length, byteorder='big') - 1
    partition_id_index = partition_array_index + 1

    for _ in range(partitions_array_size):
        partition_resp, next_partition_id_index = await produce_partition_response(client_request, partition_id_index, topic_name,
                                                                         topics)
        partitions_resp_array += partition_resp
        partition_id_index = next_partition_id_index

    topics_resp = topic_name_size.to_bytes(1, byteorder='big') + topic_name.encode('utf-8') \
                  + partitions_resp_array + tag_buffer

    topics_next_index = partition_id_index + 1

    return topics_resp, topics_next_index


async def produce_partition_response(client_request: bytes,
                               partition_id_index: int,
                               topic_name: str,
                               topics: dict
                               ) -> tuple[bytes, int]:
    """
    Processes a partition-level response for a Kafka Produce request.

    This function extracts information related to a specific topic partition from the client
    request, validates it against the list of server-side topic partitions, and constructs
    a response reflecting the result. When the partition and topic validation is successful,
    it logs the received record batch into an appropriate log file.

    :param client_request: Bytes containing the client request payload.
    :param partition_id_index: Index in the client request where the partition ID starts.
    :param topic_name: Name of the topic to which the partition belongs.
    :param topics: Dictionary containing topic metadata, including partitions.
    :return: A tuple containing the serialized partition response and the next byte index to
             process in the client request payload.
    """

    tag_buffer = int(0).to_bytes(1, byteorder='big')
    partition_id_size_length = 4
    partition_index = client_request[partition_id_index: partition_id_index + partition_id_size_length]

    record_batch_array_index = partition_id_index + partition_id_size_length
    record_batch_size_length = varint_encoding_size(client_request, record_batch_array_index + 1)
    record_batch_size = int.from_bytes(
        client_request[record_batch_array_index: record_batch_array_index + record_batch_size_length],
        byteorder='big')
    record_batch_index = record_batch_array_index + record_batch_size_length
    record_batch = client_request[record_batch_index: record_batch_index + record_batch_size]

    valid_topic_and_partition = False

    if topic_name in topics:
        partition_idx = int.from_bytes(partition_index, byteorder='big')
        for partition in topics[topic_name]["partitions"]:
            if partition_idx == partition["partition_index"]:
                valid_topic_and_partition = True
                break

    if valid_topic_and_partition:
        error_code = int(0).to_bytes(2, byteorder='big')
        base_offset = int(0).to_bytes(8, byteorder='big')
        log_start_offset = int(0).to_bytes(8, byteorder='big')

        record_batch_log_file = LOG_FILES_DIR \
                                + "/" + topic_name + "-" + str(int.from_bytes(partition_index, byteorder='big')) \
                                + "/" + LOG_FILE_NAME

        with open(record_batch_log_file, 'wb') as log_file:
            log_file.write(record_batch)

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


def encode_unsigned_varint(value: int) -> bytes:
    """
    Encode an integer as Kafka UVARINT (unsigned LEB128).

    This function takes a non-negative integer and encodes it as an unsigned
    variable-length integer using the LEB128 format. The encoding proceeds by
    writing 7-bit chunks of the integer, with the most significant bit of each
    byte used as a continuation flag. The most significant bit is set to 1 for
    all chunks except the final one, which indicates the end of the encoding.

    :param value: The non-negative integer to encode.
    :return: Encoded bytes representing the integer in Kafka UVARINT format.
    :raises ValueError: If the input value is negative.
    """

    if value < 0:
        raise ValueError("UVARINT cannot encode negative values")
    encoded = bytearray()
    while True:
        to_write = value & 127
        value >>= 7
        if value:
            encoded.append(to_write | 128)
        else:
            encoded.append(to_write)
            break
    return bytes(encoded)


def varint_encoding_size(request: bytes, index: int) -> int:
    """
    Calculates the size of a varint-encoded integer in bytes from a given array of bytes.

    The function iterates through each byte of the provided request starting at the
    given index until it encounters a byte indicating the end of the varint-encoded
    integer. The size of the varint-encoded integer is then returned in bytes.

    :param request: The byte array that holds the varint-encoded integer. The data
        must be properly formatted for varint encoding.
    :param index: The starting index within the byte array from where to begin
        decoding.
    :return: The total size of the varint-encoded integer in bytes.
    """

    num_bytes = 1
    while int.from_bytes(request[index: index + 1], byteorder='big') != 0:
        num_bytes += 1
        index += 1
    return num_bytes


def get_response_topic_data(request_topic: str, topics: dict[str, dict[str, Any]]) -> bytes:
    """
    Generate response topic data containing information about the requested topic.

    This function constructs a byte-encoded response object that encapsulates
    metadata and information about a requested topic. It accesses the topic
    details from the provided dictionary of topics and encodes the necessary
    data such as topic ID, error codes, partition information, and various
    properties. If the requested topic does not exist, it returns a default
    response indicating an error condition.

    :param request_topic: The name of the topic for which metadata is being
                          requested.
    :param topics: A dictionary mapping topic names to their respective metadata.
                   The metadata includes details such as topic UUID, error codes,
                   and information about partitions. Each topic metadata should
                   follow a specific schema with required keys.
    :return: Byte-encoded response containing topic metadata information or a
             default error response if the topic does not exist.
    """

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
    """
    Processes a binary record batch and extracts individual records.

    This function takes a binary record batch, parses the number of records, and then
    iterates through the batch to extract individual records. The records are determined
    based on their size and structure, and the extracted records are added to a list,
    which is returned.

    :param record_batch: The binary batch of records to process.
        Must contain metadata in the first 61 bytes, including
        the number of records (bytes 57 to 61), followed by the
        actual record data.
    :return: A list of individual records extracted from the
        provided binary record batch.
    """

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
    """
    Processes a binary record and returns a dictionary representation of its content. The
    function interprets the binary input based on its type and extracts relevant
    information for topic or partition records. Unknown record types are also
    handled and labeled appropriately.

    :param record: A binary sequence representing encoded topic or partition data.
    :return: A dictionary containing the parsed record's type and additional
        attributes depending on its content. If the record is unknown, the dictionary
        will only contain the "type" key with the value "unknown".
    """

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
    """
    Parses cluster metadata from a byte stream and extracts information about topics and their
    associated partitions. The extracted metadata is organized in a dictionary structure.

    :param cluster_metadata: A byte stream containing the raw metadata of the cluster.
    :return: A dictionary where the keys are topic names and the values are dictionaries
        containing topic metadata, including a list of associated partition records.
    """

    topics: dict[str, dict[str, Any]] = dict()
    record_batch_index = 0
    cluster_metadata_len = len(cluster_metadata)

    while record_batch_index < cluster_metadata_len:
        record_batch = extract_record_batch(cluster_metadata, record_batch_index)
        record_list = process_record_batch(record_batch)
        for record in record_list:
            processed_record = process_record(record)
            if processed_record["type"] == "topic":
                topics[processed_record["topic_name"]] = processed_record
            elif processed_record["type"] == "partition":
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


async def handle_api_version_requests(client_request: bytes, topics: dict[str, dict]) -> bytes:
    """
    Handles API version requests by generating the appropriate response based on the
    API version specified in the client's request.

    The function decodes the API version from the provided `client_request`, verifies whether
    it is supported, and constructs a response containing metadata for supported API keys
    and their associated minimum/maximum versions. If the API version is not supported,
    an error code is returned in the response.

    :param client_request: The byte string containing the client's request message.
    :param topics: A dictionary containing topic metadata.
    :return: A byte string containing the response for the API version request.
    """

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


async def handle_describe_topic_partition_requests(client_request: bytes, topics: dict[str, dict]) -> bytes:
    """
    Handles describe topic partition requests from a Kafka client. Parses the incoming request, extracts the requested
    topics, prepares the appropriate response data, and constructs a response byte stream.

    :param client_request: A byte stream representing the client request, containing information such as API key,
        API version, client ID, and the list of topics being queried.
    :param topics: A dictionary mapping topic names to their corresponding metadata and partition details. The keys
        are strings representing topic names, and the values are dictionaries containing partition-level details or
        topic-specific information.
    :return: A byte stream representing the server response to the client's describe topic partition request, prepared
        with appropriate encoding and response format.
    """

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
        resp_topic_data = get_response_topic_data(request_topic, topics)
        resp_topics_array += resp_topic_data


    resp_body = tag_buffer + throttle_time + resp_topics_array + next_cursor + tag_buffer

    return resp_body


async def handle_fetch_requests(client_request: bytes, topics: dict[str, dict]) -> bytes:
    """
    Handles Kafka fetch requests by processing a client request for specific topics and returning
    a formatted response with the requested data or error information.

    :param client_request: The byte-encoded client request containing request metadata, topic
        information, and other configurations as used in Kafka fetch protocol.
    :param topics: A dictionary where keys represent topic names and values are nested dictionaries
        including metadata (e.g., "topic_uuid") for each topic.
    :return: A byte-encoded Kafka fetch response containing the requested records, error codes,
        and other necessary metadata, conforming to the Kafka protocol.
    """

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
            partition_error_code = int(0).to_bytes(2, byteorder='big')
            record_batch_log_dir = LOG_FILES_DIR + "/" + topic \
                                   + "-" + str(int.from_bytes(partition_index, byteorder='big')) \
                                   + "/"
            record_batch_log = record_batch_log_dir + LOG_FILE_NAME
            partition_records_array = b""
            if Path(record_batch_log).is_file() and Path(record_batch_log).stat().st_size > 0:
                with open(record_batch_log, 'rb') as log_file:
                    partition_records_array = log_file.read()

            partition_records_array = encode_unsigned_varint(len(partition_records_array) + 1) \
                                      + partition_records_array
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
    topics_array = topics_array_length + topic_uuid + partitions_array + tag_buffer
    node_endpoints_array = int(1).to_bytes(1, byteorder='big')

    resp_body = tag_buffer + throttle_time + error_code + session_id \
                + topics_array  + node_endpoints_array + tag_buffer

    return resp_body


async def handle_produce_requests(client_request: bytes, topics: dict[str, dict]) -> bytes:
    """
    Processes a produce request from a Kafka client. This function parses the client request,
    extracts necessary information about topics, and generates a response with relevant
    topic data. It optionally handles cluster metadata if available in the cluster metadata
    file. Finally, the constructed response is returned.

    :param client_request: The byte-encoded request sent by the Kafka client.
    :param topics: A dictionary containing topic information.
    :return: A byte-encoded response containing processed topic information.
    """

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
        topics_response, topics_next_index = await produce_topic_response(client_request, topic_index, topics)
        topics_resp_array += topics_response
        topic_index = topics_next_index


    tag_buffer = int(0).to_bytes(1, byteorder='big')
    throttle_time = int(0).to_bytes(4, byteorder='big')

    resp_body = tag_buffer + topics_resp_array + throttle_time + tag_buffer
    return resp_body


async def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
    """
    Handles communication with a connected client in an asynchronous manner. The client_handler function
    reads client requests, identifies the appropriate API handler based on a predefined set of API keys,
    and sends responses back to the client. It ensures proper error handling, logging, and closure of
    the client connection upon termination.

    :param reader: The asyncio StreamReader instance used to receive data from the client.
    :param writer: The asyncio StreamWriter instance used to send data to the client.
    :return: None
    """

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
                topics = {}
                cluster_metadata_file = LOG_FILES_DIR + "/__cluster_metadata-0/" + LOG_FILE_NAME
                if Path(cluster_metadata_file).is_file():
                    logger.info("Loading Cluster metadata file")
                    with open(cluster_metadata_file, 'rb') as f:
                        cluster_metadata = f.read()
                        logger.info(f"Cluster metadata: {len(cluster_metadata)}|{cluster_metadata.hex()}")
                        topics = parse_cluster_metadata(cluster_metadata)
                        logger.info(f"Cluster metadata file loaded successfully. Topics: {topics}")
                else:
                    logger.info("Cluster metadata file not found.")

                resp_body = await api_handlers[request_api_key](client_request, topics)
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
