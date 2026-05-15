from typing import Any
from utils import encode_unsigned_varint, varint_encoding_size
from storage import Storage


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

        request_api_version = int.from_bytes(client_request[6:8], byteorder="big")

        if request_api_version in (0, 1, 2, 3, 4):
            error_code = int(0).to_bytes(2, byteorder="big")
            api_key_array_length = int(5).to_bytes(1, byteorder="big")
            api_key_min = int(0).to_bytes(2, byteorder="big")
            tag_buffer = int(0).to_bytes(1, byteorder="big")
            throttle_time = int(0).to_bytes(4, byteorder="big")

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
                    error_code
                    + api_key_array_length
                    + versions_api_key
                    + api_key_min
                    + versions_api_key_max
                    + tag_buffer
                    + topics_api_key
                    + api_key_min
                    + topics_api_key_max
                    + tag_buffer
                    + fetch_api_key
                    + api_key_min
                    + fetch_api_key_max
                    + tag_buffer
                    + produce_api_key
                    + api_key_min
                    + produce_api_key_max
                    + tag_buffer
                    + throttle_time
                    + tag_buffer
            )
        else:
            error_code = int(35).to_bytes(2, byteorder="big")
            resp_body = error_code

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

        :raises KeyError: Raised if a requested topic is not present in the provided topics dictionary.
        """

        client_id_length = int.from_bytes(client_request[12:14], byteorder="big")

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

        tag_buffer = int(0).to_bytes(1, byteorder="big")
        throttle_time = int(0).to_bytes(4, byteorder="big")
        next_cursor = int(255).to_bytes(1, byteorder="big")

        topic_array_index = (
                field_sizes["message"]
                + field_sizes["api_key"]
                + field_sizes["api_version"]
                + field_sizes["correlation_id"]
                + field_sizes["client_id"]
                + client_id_length
                + field_sizes["tag_buffer"]
        )

        topics_array_length = client_request[topic_array_index : topic_array_index + 1]

        request_topics = []
        topic_index = topic_array_index + 1

        for _ in range(int.from_bytes(topics_array_length, byteorder="big") - 1):
            topic_name_length = int.from_bytes(client_request[topic_index : topic_index + 1], byteorder="big")
            topic_name = client_request[topic_index + 1 : topic_index + topic_name_length].decode("utf-8")
            request_topics.append(topic_name)
            topic_index += topic_name_length + 1

        resp_topics_list = [topics_array_length]

        for request_topic in sorted(request_topics):
            resp_topic_data = self.get_response_topic_data(request_topic, topics)
            resp_topics_list.append(resp_topic_data)

        resp_body = tag_buffer + throttle_time + b"".join(resp_topics_list) + next_cursor + tag_buffer

        return resp_body

    async def handle_fetch_requests(self, client_request: bytes,  topics_by_uuid: dict[bytes, dict[str, Any]]) -> bytes:
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

        client_id_length = int.from_bytes(client_request[12:14], byteorder="big")

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

        session_id_index = (
                field_sizes["message"]
                + field_sizes["api_key"]
                + field_sizes["api_version"]
                + field_sizes["correlation_id"]
                + field_sizes["client_id"]
                + client_id_length
                + field_sizes["tag_buffer"]
                + field_sizes["max_wait_ms"]
                + field_sizes["min_bytes"]
                + field_sizes["max_bytes"]
                + field_sizes["isolation_level"]
        )

        topics_array_index =  session_id_index + field_sizes["session_id"] + field_sizes["session_epoch"]

        session_id = client_request[session_id_index : session_id_index + 4]
        topics_array_length = client_request[topics_array_index : topics_array_index + 1]
        topic_uuid = client_request[topics_array_index + 1 : topics_array_index + 17]
        partitions_array_length = client_request[topics_array_index + 17 : topics_array_index + 18]
        partition_index = client_request[topics_array_index + 18 : topics_array_index + 22]

        tag_buffer = int(0).to_bytes(1, byteorder="big")
        throttle_time = int(0).to_bytes(4, byteorder="big")
        error_code = int(0).to_bytes(2, byteorder="big")

        if topic_uuid in topics_by_uuid:
            topic_data = topics_by_uuid[topic_uuid]
            topic_name = topic_data["topic_name"]

            partition_error_code = int(0).to_bytes(2, byteorder="big")
            partition_idx = int.from_bytes(partition_index, byteorder="big")
            partition_records_array = await self.storage.read_partition_log(topic_name, partition_idx)

            partition_records_array = encode_unsigned_varint(len(partition_records_array) + 1) + partition_records_array

        else:
            partition_error_code = int(100).to_bytes(2, byteorder="big")
            partition_records_array = int(0).to_bytes(1, byteorder="big")

        partition_high_watermark = int(0).to_bytes(8, byteorder="big")
        partitions_last_stable_offset = int(0).to_bytes(8, byteorder="big")
        partition_log_start_offset = int(0).to_bytes(8, byteorder="big")
        partition_aborted_transactions = int(1).to_bytes(1, byteorder="big")
        partition_preferred_read_replica = int(0).to_bytes(4, byteorder="big")
        partition_diverging_epoch_array = int(0).to_bytes(1, byteorder="big")
        partition_current_leader_array = int(0).to_bytes(1, byteorder="big")
        partition_snapshot_id_array = int(0).to_bytes(1, byteorder="big")

        partitions_array = (
                partitions_array_length
                + partition_index
                + partition_error_code
                + partition_high_watermark
                + partitions_last_stable_offset
                + partition_log_start_offset
                + partition_aborted_transactions
                + partition_preferred_read_replica
                + partition_records_array
                + partition_diverging_epoch_array
                + partition_current_leader_array
                + partition_snapshot_id_array
                + tag_buffer
        )
        topics_array = topics_array_length + topic_uuid + partitions_array + tag_buffer
        node_endpoints_array = int(1).to_bytes(1, byteorder="big")

        resp_body = (
                tag_buffer
                + throttle_time
                + error_code
                + session_id
                + topics_array
                + node_endpoints_array
                + tag_buffer
        )

        return resp_body

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

        client_id_length = int.from_bytes(client_request[12:14], byteorder="big")

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

        topics_array_index = (
                field_sizes["message"]
                + field_sizes["api_key"]
                + field_sizes["api_version"]
                + field_sizes["correlation_id"]
                + field_sizes["client_id"]
                + client_id_length
                + field_sizes["tag_buffer"]
                + field_sizes["transactional_id"]
                + field_sizes["required_acks"]
                + field_sizes["timeout"]
        )

        topics_array_length = client_request[topics_array_index : topics_array_index + 1]
        topics_resp_list = [topics_array_length]
        topics_array_size = int.from_bytes(topics_array_length, byteorder="big") - 1
        topic_index = topics_array_index + 1

        for _ in range(topics_array_size):
            topics_response, topics_next_index = await self.produce_topic_response(client_request, topic_index, topics)
            topics_resp_list.append(topics_response)
            topic_index = topics_next_index

        tag_buffer = int(0).to_bytes(1, byteorder="big")
        throttle_time = int(0).to_bytes(4, byteorder="big")

        resp_body = tag_buffer + b"".join(topics_resp_list) + throttle_time + tag_buffer
        return resp_body

    async def produce_partition_response(
            self,
            client_request: bytes,
            partition_id_index: int,
            topic_name: str,
            topics: dict
    ) -> tuple[bytes, int]:
        """
        Generate a partition response for a produce request and return the response alongside the next index.

        This method processes a produce request for a specific topic and partition, extracts the relevant
        record batch, validates the topic and partition, and generates the corresponding partition response.
        It also determines the next index in the client request for further processing.

        :param client_request: The raw request data received from the client as a byte sequence.
        :param partition_id_index: The index within the raw data where the partition ID starts.
        :param topic_name: The name of the topic to which the produce request is targeted.
        :param topics: A dictionary containing metadata about topics and their partitions. The metadata
                       includes partition indices and other necessary information for validation.
        :return: A tuple containing:
                 - The partition response as a byte sequence.
                 - The next index in the client request as an integer.
        """

        tag_buffer = int(0).to_bytes(1, byteorder="big")
        partition_id_size_length = 4
        partition_index = client_request[partition_id_index : partition_id_index + partition_id_size_length]

        record_batch_array_index = partition_id_index + partition_id_size_length
        record_batch_size_length = varint_encoding_size(client_request, record_batch_array_index + 1)
        record_batch_size = int.from_bytes(
            client_request[record_batch_array_index : record_batch_array_index + record_batch_size_length],
            byteorder="big"
        )
        record_batch_index = record_batch_array_index + record_batch_size_length
        record_batch = client_request[record_batch_index : record_batch_index + record_batch_size]

        valid_topic_and_partition = False

        if topic_name in topics:
            partition_idx = int.from_bytes(partition_index, byteorder="big")
            if partition_idx in topics[topic_name]["partitions"]:
                valid_topic_and_partition = True

        if valid_topic_and_partition:
            error_code = int(0).to_bytes(2, byteorder="big")
            base_offset = int(0).to_bytes(8, byteorder="big")
            log_start_offset = int(0).to_bytes(8, byteorder="big")
            partition_idx = int.from_bytes(partition_index, byteorder="big")
            await self.storage.write_partition_log(topic_name, partition_idx, record_batch)
        else:
            error_code = int(3).to_bytes(2, byteorder="big")
            base_offset = int(-1).to_bytes(8, byteorder="big", signed=True)
            log_start_offset = int(-1).to_bytes(8, byteorder="big", signed=True)

        log_append_time = int(-1).to_bytes(8, byteorder="big", signed=True)
        records_array = int(1).to_bytes(1, byteorder="big")
        error_message = int(0).to_bytes(1, byteorder="big")

        partition_resp = (
                partition_index
                + error_code
                + base_offset
                + log_append_time
                + log_start_offset
                + records_array
                + error_message
                + tag_buffer
        )

        next_index = (
                partition_id_index
                + partition_id_size_length
                + record_batch_size_length
                + record_batch_size
        )

        return partition_resp, next_index

    async def produce_topic_response(
            self,
            client_request: bytes,
            topic_index: int,
            topics: dict
    ) -> tuple[bytes, int]:
        """
        Generates a response for a specific Kafka topic by parsing the client request and processing each partition
        within the topic.

        The function creates a properly formatted response with topic details and corresponding partition responses.

        :param client_request: The binary data received from the Kafka client containing metadata for topic parsing.
        :param topic_index: The starting index in the client request where the topic metadata begins.
        :param topics: A dictionary of available topics mapped to their configurations, used for validation and
                       processing partitions.
        :return: A tuple containing the topic response as bytes and the updated index in the client request after
                 processing the topic.
        """

        tag_buffer = int(0).to_bytes(1, byteorder="big")
        topic_name_size = int.from_bytes(client_request[topic_index : topic_index + 1], byteorder="big")
        topic_name = client_request[topic_index + 1 : topic_index + topic_name_size].decode("utf-8")

        partition_array_index = topic_index + topic_name_size
        partitions_array_length = client_request[partition_array_index : partition_array_index + 1]
        partitions_resp_list = [partitions_array_length]
        partitions_array_size = int.from_bytes(partitions_array_length, byteorder="big") - 1
        partition_id_index = partition_array_index + 1

        for _ in range(partitions_array_size):
            partition_resp, next_partition_id_index = await self.produce_partition_response(
                client_request,
                partition_id_index,
                topic_name,
                topics
            )
            partitions_resp_list.append(partition_resp)
            partition_id_index = next_partition_id_index

        topics_resp = (
                topic_name_size.to_bytes(1, byteorder="big")
                + topic_name.encode("utf-8")
                + b"".join(partitions_resp_list)
                + tag_buffer
        )

        topics_next_index = partition_id_index + 1

        return topics_resp, topics_next_index

    def get_response_topic_data(self, request_topic: str, topics: dict[str, dict[str, Any]]) -> bytes:
        """
        Generates the response payload for a given request topic based on available topics metadata.

        Constructs a byte-encoded representation of the topic's information, including its partitions,
        leaders, replicas, and other associated data.

        :param request_topic: The name of the topic being requested
        :param topics: A mapping of topics to their detailed metadata, where each topic maps to a dictionary containing
            its UUID and partition-related data.
        :return: A byte-encoded representation of the response topic data, detailing the topic's metadata.
        """

        tag_buffer = int(0).to_bytes(1, byteorder="big")
        is_internal = int(0).to_bytes(1, byteorder="big")
        topic_authorized_operations = int(0).to_bytes(4, byteorder="big")
        partition_data_list: list = []

        if request_topic not in topics:
            resp_topic_id = int(0).to_bytes(16, byteorder="big")
            resp_topic_error_code = int(3).to_bytes(2, byteorder="big")
            partition_array_size = int(1).to_bytes(1, byteorder="big")
            partition_array = partition_array_size
        else:
            resp_topic_details = topics[request_topic]
            resp_topic_id = resp_topic_details["topic_uuid"].bytes
            resp_topic_error_code = int(0).to_bytes(2, byteorder="big")

            broker = int(1).to_bytes(1, byteorder="big")
            elr = int(1).to_bytes(1, byteorder="big")
            last_elr = int(1).to_bytes(1, byteorder="big")
            offline_replicas = int(1).to_bytes(1, byteorder="big")

            partition_array_size = (len(resp_topic_details["partitions"]) + 1).to_bytes(1, byteorder="big")

            for partition in resp_topic_details["partitions"].values():
                partition_index = partition["partition_index"].to_bytes(4, byteorder="big")
                leader_id = partition["leader_id"].to_bytes(4, byteorder="big")
                leader_epoch = partition["leader_epoch"].to_bytes(4, byteorder="big")
                replica_nodes = partition["num_replicas"].to_bytes(4, byteorder="big")
                isr_nodes = partition["num_isr"].to_bytes(4, byteorder="big")

                partition_data_list.append(
                        resp_topic_error_code
                        + partition_index
                        + leader_id
                        + leader_epoch
                        + replica_nodes
                        + broker
                        + isr_nodes
                        + broker
                        + elr
                        + last_elr
                        + offline_replicas
                        + tag_buffer
                )

            partition_array = partition_array_size + b"".join(partition_data_list)

        resp_topic_name = request_topic.encode("utf-8")
        resp_topic_name_length = int(len(resp_topic_name) + 1).to_bytes(1, byteorder="big")
        resp_topic_data = (
                resp_topic_error_code
                + resp_topic_name_length
                + resp_topic_name
                + resp_topic_id
                + is_internal
                + partition_array
                + topic_authorized_operations
                + tag_buffer
        )

        return resp_topic_data
