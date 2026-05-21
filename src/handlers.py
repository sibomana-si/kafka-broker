import logging
from typing import Any
from src.storage import Storage
from src.protocol.reader import BufferReader
from src.protocol.writer import BufferWriter
from src.protocol.parser import (
    parse_request_header,
    parse_describe_topic_partitions_request,
    parse_fetch_request,
    parse_produce_request
)

logger = logging.getLogger(__name__)

TAG_BUFFER = b'\x00'
THROTTLE_TIME = b'\x00\x00\x00\x00'
ERROR_CODE_NONE = b'\x00\x00'
ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION = b'\x00\x03'
ERROR_CODE_UNKNOWN_SERVER_ERROR = b'\x00\x23'
ERROR_CODE_UNKNOWN_TOPIC_ID = b'\x00\x64'


class RequestHandler:
    def __init__(self, storage: Storage):
        self.storage = storage

    async def handle_api_version_requests(self, client_request: bytes) -> bytes:
        """
        Handles API version requests and generates the appropriate response.

        :param client_request: A byte representation of the client's API version request.
        :return: A byte representation of the API version response, which includes error codes and
                supported API key information.
        """

        reader = BufferReader(client_request, 4) # Skip message length
        header = parse_request_header(reader)

        if header.api_version in (0, 1, 2, 3, 4):
            api_key_array_length = int(5).to_bytes(1, byteorder="big")
            api_key_min = int(0).to_bytes(2, byteorder="big")

            # Produce API
            produce_api_key = int(0).to_bytes(2, byteorder="big")
            produce_api_key_max = int(11).to_bytes(2, byteorder="big")

            # Fetch API
            fetch_api_key = int(1).to_bytes(2, byteorder="big")
            fetch_api_key_max = int(16).to_bytes(2, byteorder="big")

            # ApiVersions API
            versions_api_key = int(18).to_bytes(2, byteorder="big")
            versions_api_key_max = int(4).to_bytes(2, byteorder="big")

            # DescribeTopicPartitions API
            topics_api_key = int(75).to_bytes(2, byteorder="big")
            topics_api_key_max = int(0).to_bytes(2, byteorder="big")

            resp_body = (
                    ERROR_CODE_NONE
                    + api_key_array_length
                    + versions_api_key
                    + api_key_min
                    + versions_api_key_max
                    + TAG_BUFFER
                    + topics_api_key
                    + api_key_min
                    + topics_api_key_max
                    + TAG_BUFFER
                    + fetch_api_key
                    + api_key_min
                    + fetch_api_key_max
                    + TAG_BUFFER
                    + produce_api_key
                    + api_key_min
                    + produce_api_key_max
                    + TAG_BUFFER
                    + THROTTLE_TIME
                    + TAG_BUFFER
            )
        else:
            resp_body = ERROR_CODE_UNKNOWN_SERVER_ERROR

        return resp_body

    async def handle_describe_topic_partition_requests(self, client_request: bytes, topics: dict[str, dict]) -> bytes:
        """
        Handles the processing of describe topic partition requests.

        This function parses the client's request to extract topic details, processes the requested topics,
        and generates an appropriate response body based on the provided topics dictionary.

        :param client_request: The raw incoming request data from the client in bytes format. It contains
            information related to the topics to be described and other metadata.
        :param topics: A dictionary containing details about available topics and their metadata.
            The keys are topic names, and the values are dictionaries containing partition-related
            details for each topic.
        :return: A bytes object representing the response body, constructed according to the input
            request and the topic details.
        """

        reader = BufferReader(client_request, 4) # Skip message length
        header = parse_request_header(reader)
        request = parse_describe_topic_partitions_request(reader)

        writer = BufferWriter()
        writer.write_tag_buffer()
        writer.write_int32(0) # THROTTLE_TIME
        writer.write_compact_array_length(len(request.topics))

        request_topics = [t.name for t in request.topics]
        request_topics.sort()

        for topic_name in request_topics:
            self.write_response_topic_data(writer, topic_name, topics)

        writer.write_int8(255) # next_cursor
        writer.write_tag_buffer()

        return writer.get_bytes()

    def write_response_topic_data(self, writer: BufferWriter, request_topic: str, topics: dict[str, dict[str, Any]]) -> None:
        """
        Writes topic response data to the provided writer based on the requested topic and available topic metadata.

        The method examines the request_topic to determine if it is present within the topics metadata. If the topic is
        not found, it writes error details for the unknown topic. If the topic is found, it writes metadata about the
        requested topic, including details about its associated partitions.

        :param writer: A BufferWriter instance used for writing the response data in the appropriate format.
        :param request_topic: The name of the topic being requested.
        :param topics: A dictionary containing metadata of topics. Each key is a topic name mapping to a dictionary,
            which includes the UUID of the topic and partition details.

        :return: None
        """

        if request_topic not in topics:
            writer.write_bytes(ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION)
            writer.write_compact_string(request_topic)
            writer.write_uuid(int(0).to_bytes(16, byteorder="big"))
            writer.write_int8(0)  # is_internal
            writer.write_compact_array_length(0)  # partitions
            writer.write_int32(0)  # topic_authorized_operations
            writer.write_tag_buffer()
        else:
            resp_topic_details = topics[request_topic]
            writer.write_bytes(ERROR_CODE_NONE)
            writer.write_compact_string(request_topic)
            writer.write_uuid(resp_topic_details["topic_uuid"].bytes)
            writer.write_int8(0)

            partitions = resp_topic_details["partitions"].values()
            writer.write_compact_array_length(len(partitions))

            for partition in partitions:
                writer.write_bytes(ERROR_CODE_NONE)
                writer.write_int32(partition["partition_index"])
                writer.write_int32(partition["leader_id"])
                writer.write_int32(partition["leader_epoch"])
                writer.write_int32(partition["num_replicas"])
                writer.write_int8(1)  # broker
                writer.write_int32(partition["num_isr"])
                writer.write_int8(1)  # broker
                writer.write_int8(1)  # elr
                writer.write_int8(1)  # last_elr
                writer.write_int8(1)  # offline_replicas
                writer.write_tag_buffer()

            writer.write_int32(0)  # topic_authorized_operations
            writer.write_tag_buffer()

    async def handle_fetch_requests(self, client_request: bytes, topics_by_uuid: dict[bytes, dict[str, Any]]) -> bytes:
        """
        Handles incoming fetch requests and generates the corresponding response.

        This function processes a binary representation of a fetch request from
        a Kafka client, parsing various fields and constructing an appropriate
        response. It identifies the topic and partition information from the
        provided request and retrieves the relevant partition log. If the requested
        topic or partition does not exist, it encodes an appropriate error in the
        response.

        :param client_request: The binary data representing the client fetch request.
        :param topics_by_uuid: A dictionary for O(1) lookup of topics by their raw UUID bytes.
        :return: A binary-encoded response that contains the requested data or an
                 error message if the request could not be fulfilled.
        """

        reader = BufferReader(client_request, 4) # Skip message length
        header = parse_request_header(reader)
        request = parse_fetch_request(reader)

        writer = BufferWriter()
        writer.write_tag_buffer()
        writer.write_int32(0) # THROTTLE TIME
        writer.write_bytes(ERROR_CODE_NONE)
        writer.write_int32(request.session_id)

        writer.write_compact_array_length(len(request.topics))

        if len(request.topics) == 0:
            writer.write_tag_buffer()

        for topic in request.topics:
            writer.write_uuid(topic.topic_id)
            writer.write_compact_array_length(len(topic.partitions))

            if topic.topic_id in topics_by_uuid:
                topic_data = topics_by_uuid[topic.topic_id]
                topic_name = topic_data["topic_name"]

                for partition in topic.partitions:
                    partition_records_array = await self.storage.read_partition_log(
                        topic_name, partition.partition_index, partition.fetch_offset, partition.partition_max_bytes
                    )

                    writer.write_int32(partition.partition_index)
                    writer.write_bytes(ERROR_CODE_NONE)
                    writer.write_int64(0) # high_watermark
                    writer.write_int64(0) # last_stable_offset
                    writer.write_int64(0) # log_start_offset
                    writer.write_int8(1) # aborted_transactions length
                    writer.write_int32(0) # preferred_read_replica

                    # records array (compact length)
                    writer.write_unsigned_varint(len(partition_records_array) + 1)
                    writer.write_bytes(partition_records_array)

                    writer.write_int8(0) # diverging_epoch
                    writer.write_int8(0) # current_leader
                    writer.write_int8(0) # snapshot_id
                    writer.write_tag_buffer()
            else:
                for partition in topic.partitions:
                    writer.write_int32(partition.partition_index)
                    writer.write_bytes(ERROR_CODE_UNKNOWN_TOPIC_ID)
                    writer.write_int64(0)
                    writer.write_int64(0)
                    writer.write_int64(0)
                    writer.write_int8(1)
                    writer.write_int32(0)

                    writer.write_unsigned_varint(0) # empty records array

                    writer.write_int8(0)
                    writer.write_int8(0)
                    writer.write_int8(0)
                    writer.write_tag_buffer()

            writer.write_tag_buffer()

        writer.write_int8(1) # node_endpoints_array
        writer.write_tag_buffer() # tag buffer for fetch response

        return writer.get_bytes()

    async def handle_produce_requests(self, client_request: bytes, topics: dict[str, dict]) -> bytes:
        """
        The function parses the incoming request to extract topic-related information and prepares an
        appropriate response message.

        :param client_request: The incoming request payload in bytes format.
        :param topics: A dictionary where each key is a topic name and the associated
            value is a dictionary containing topic-specific details.
        :return: A bytes object representing the response payload for the produce
            request.
        """

        reader = BufferReader(client_request, 4)
        header = parse_request_header(reader)
        request = parse_produce_request(reader)

        writer = BufferWriter()
        writer.write_tag_buffer()
        writer.write_compact_array_length(len(request.topics))

        for topic in request.topics:
            writer.write_compact_string(topic.name)
            writer.write_compact_array_length(len(topic.partitions))

            for partition in topic.partitions:
                valid_topic_and_partition = False
                if topic.name in topics:
                    if partition.partition_index in topics[topic.name]["partitions"]:
                        valid_topic_and_partition = True

                writer.write_int32(partition.partition_index)

                if valid_topic_and_partition:
                    writer.write_bytes(ERROR_CODE_NONE)
                    writer.write_int64(0) # base_offset
                    writer.write_int64(-1) # log_append_time
                    writer.write_int64(0) # log_start_offset
                    await self.storage.write_partition_log(
                        topic.name, partition.partition_index, partition.record_batch
                    )
                else:
                    writer.write_bytes(ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION)
                    writer.write_int64(-1) # base_offset
                    writer.write_int64(-1) # log_append_time
                    writer.write_int64(-1) # log_start_offset

                writer.write_int8(1)  # record errors array
                writer.write_int8(0)  # error message
                writer.write_tag_buffer()  # partition tag buffer

            writer.write_tag_buffer()  # topic tag buffer

        writer.write_int32(0) # throttle time
        writer.write_tag_buffer()  # tag buffer for produce response

        return writer.get_bytes()

