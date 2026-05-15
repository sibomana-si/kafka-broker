import asyncio
import logging
import uuid
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

class Storage:
    def __init__(self, log_dir: str, log_file_name: str):
        self.log_dir = log_dir
        self.log_file_name = log_file_name

    async def load_metadata(self) -> dict[str, dict[str, Any]]:
        """
        Reads the cluster metadata file and parses its contents into a structured dictionary.

        The metadata file is identified using a predefined directory and log file name.
        If the file does not exist or an error occurs during reading or parsing, it logs the error and returns
        an empty dictionary.

        :return: A dictionary containing the parsed cluster metadata organized by topic names as keys.
        If the file is not found or parsing fails, returns an empty dictionary.
        """

        cluster_metadata_file = f"{self.log_dir}/__cluster_metadata-0/{self.log_file_name}"
        topics = {}
        if await asyncio.to_thread(Path(cluster_metadata_file).is_file):
            try:
                cluster_metadata = await self._read_file(cluster_metadata_file)
                topics = await asyncio.to_thread(self._parse_cluster_metadata, cluster_metadata)
            except Exception as e:
                logger.error(f"Failed to load metadata: {e}")
        return topics

    async def read_partition_log(self, topic_name: str, partition_index: int) -> bytes:
        """
        Reads the content of a specific partition log file for a given topic.

        If the log file exists and is not empty, its contents will be returned as bytes.
        Otherwise, an empty byte string is returned. This function facilitates the processing and
        retrieval of partitioned log data for the provided topic and partition index.

        :param topic_name: Name of the topic associated with the log file.
        :param partition_index: Index of the partition corresponding to the log file.
        :return: Content of the partition log file as bytes if it exists and is non-empty;
                 otherwise, an empty byte string.
        """

        log_file = f"{self.log_dir}/{topic_name}-{partition_index}/{self.log_file_name}"
        path = Path(log_file)
        if await asyncio.to_thread(lambda: path.is_file() and path.stat().st_size > 0):
            return await self._read_file(log_file)
        return b""

    async def write_partition_log(self, topic_name: str, partition_index: int, data: bytes) -> None:
        """
        Writes data to a specific partition log file.

        This method creates or opens a log file for the specified topic and partition
        index, then writes the provided binary data into it. The log file is placed
        inside a directory determined by the topic name and partition index.

        :param topic_name: The name of the topic associated with the log.
        :param partition_index: The index of the partition for which the log is written.
        :param data: The binary data to write into the log file.
        :return: None
        """

        log_file = f"{self.log_dir}/{topic_name}-{partition_index}/{self.log_file_name}"
        await self._write_file(log_file, data)

    async def _read_file(self, file_path: str) -> bytes:
        def _read():
            with open(file_path, 'rb') as f:
                return f.read()
        return await asyncio.to_thread(_read)

    async def _write_file(self, file_path: str, data: bytes) -> None:
        def _write():
            with open(file_path, 'wb') as f:
                f.write(data)
        await asyncio.to_thread(_write)

    def _parse_cluster_metadata(self, cluster_metadata: bytes) -> dict:
        """
        Parses cluster metadata from a byte sequence and constructs a dictionary containing
        information about topics and their associated partitions.

        :param cluster_metadata: Byte sequence containing serialized cluster metadata.
        :return: A dictionary where keys are topic names and values are dictionaries containing
            information about each topic, including their associated partitions.
        """

        topics = {}
        record_batch_index = 0
        cluster_metadata_len = len(cluster_metadata)

        while record_batch_index < cluster_metadata_len:
            record_batch = self._extract_record_batch(cluster_metadata, record_batch_index)
            record_list = self._process_record_batch(record_batch)
            for record in record_list:
                processed_record = self._process_record(record)
                if processed_record["type"] == "topic":
                    topics[processed_record["topic_name"]] = processed_record
                elif processed_record["type"] == "partition":
                    for topic in topics:
                        if processed_record["topic_uuid"] == topics[topic]["topic_uuid"]:
                            topics[topic]["partitions"].append(processed_record)
                            break
                    else:
                        raise Exception(f"partition with no associated topic!|{processed_record}")
            record_batch_index += len(record_batch)
        return topics

    @staticmethod
    def _extract_record_batch(cluster_metadata: bytes, record_batch_index: int) -> bytes:
        """
        Extracts a record batch from the given cluster metadata starting at the specified index.

        This static method retrieves a block of data, designated as a record batch, from the
        `cluster_metadata` byte sequence. The size of the record batch is computed based on
        the metadata at the specified `record_batch_index`. The record batch size includes a
        header of 12 bytes plus the size of the data indicated in the header.

        :param cluster_metadata: A byte sequence containing cluster data.
        :param record_batch_index: The starting index in the `cluster_metadata` where the record batch is located.
        :return: A byte sequence containing the extracted record batch.
        """

        record_batch_size = 12 + int.from_bytes(
            cluster_metadata[record_batch_index + 8: record_batch_index + 12],
            byteorder='big'
        )
        return cluster_metadata[record_batch_index: record_batch_index + record_batch_size]

    @staticmethod
    def _process_record_batch(record_batch: bytes) -> list:
        """
        Processes a batch of records from a binary-encoded input and extracts individual records.

        This method parses a binary record batch, which consists of a header and an array of records.
        It extracts each record's content based on the size information encoded within the batch and creates
        a list containing all the records.

        :param record_batch: A binary sequence representing a batch of records.
            The first part of the binary contains metadata or configuration,
            while the remaining part contains the actual records.
        :return: A list of extracted records from the provided binary record batch.
        """

        record_list = []
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

    @staticmethod
    def _process_record(record: bytes) -> dict[str, Any]:
        """
        Processes a binary record to extract metadata and structural information, returning a dictionary
        representation of the record.

        This method interprets the first byte of the record to identify its type (e.g., "topic" or "partition")
        and extracts relevant fields according to the identified structure. If the type is unrecognized,
        it marks the record as "unknown".

        :param record: The binary data representing a single record to be processed. Must
            be in a format conforming to the specifications for topic or partition records.
        :return: A dictionary containing processed data from the record depending on its
            identified type. Includes properties such as "topic_name", "topic_uuid",
            "partition_index", "num_replicas", and others when applicable.
        """

        processed_record: dict[str, Any] = {}
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
