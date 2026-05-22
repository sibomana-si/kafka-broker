import asyncio
import uuid
import os
from pathlib import Path
from typing import Any
import structlog

logger = structlog.get_logger(__name__)


class Storage:
    def __init__(self, log_dir: str, log_file_name: str):
        self.log_dir = log_dir
        self.log_file_name = log_file_name
        self.buffers: dict[tuple[str, int], bytearray] = {}
        self.buffer_lock = asyncio.Lock()

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
                logger.error("failed_to_load_metadata", error=str(e), exc_info=True)
        return topics

    async def read_partition_log(self, topic_name: str, partition_index: int, fetch_offset: int, max_bytes: int) -> bytes:
        """
        Reads a chunk of the content of a specific partition log file for a given topic.

        It respects the maximum bytes requested. For simplicity, it currently seeks to
        fetch_offset byte index (instead of record offset) and reads max_bytes.
        It also combines the content from the disk and any in-memory buffer for that partition.

        :param topic_name: Name of the topic associated with the log file.
        :param partition_index: Index of the partition corresponding to the log file.
        :param fetch_offset: The offset to start reading from.
        :param max_bytes: The maximum number of bytes to return.
        :return: Content of the partition log file as bytes if it exists and is non-empty;
                 otherwise, an empty byte string.
        """

        log_file = f"{self.log_dir}/{topic_name}-{partition_index}/{self.log_file_name}"
        path = Path(log_file)
        disk_data = b""

        def _read_chunk():
            if path.is_file() and path.stat().st_size > 0:
                with open(log_file, "rb") as f:
                    f.seek(fetch_offset)
                    return f.read(max_bytes)
            return b""

        try:
            disk_data = await asyncio.to_thread(_read_chunk)
        except Exception as e:
            logger.error("failed_to_read_from_disk_partition_log", log_file=log_file, error=str(e), exc_info=True)

        buffer_key = (topic_name, partition_index)
        async with self.buffer_lock:
            buffer_data = bytes(self.buffers.get(buffer_key, bytearray()))

        # For the memory buffer, we also need to respect offset and max_bytes.
        # fetch_offset refers to disk bytes. Since the buffer represents newly appended bytes,
        # we adjust the offset relative to the current file size.

        file_size = await asyncio.to_thread(lambda: path.stat().st_size if path.is_file() else 0)

        if fetch_offset >= file_size:
            # If the fetch offset is beyond the disk file, we only read from the buffer.
            buffer_offset = fetch_offset - file_size
            buffer_data = buffer_data[buffer_offset:buffer_offset + max_bytes]
            return buffer_data
        else:
            # We got some data from disk, so limit how much we take from buffer
            remaining_bytes = max_bytes - len(disk_data)
            if remaining_bytes > 0:
                return disk_data + buffer_data[:remaining_bytes]
            else:
                return disk_data

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

        buffer_key = (topic_name, partition_index)
        async with self.buffer_lock:
            if buffer_key not in self.buffers:
                self.buffers[buffer_key] = bytearray()
            self.buffers[buffer_key].extend(data)

    async def flush_buffers(self) -> None:
        """
        Flushes all in-memory buffers to their respective log files on disk.

        This method iterates through all the buffered data, identifies the appropriate
        log file for each buffer, and writes the data to disk. This is useful for ensuring
        data persistence, especially during graceful shutdowns.
        :return: None
        """

        async with self.buffer_lock:
            if not self.buffers:
                return
            buffers_to_flush = self.buffers
            self.buffers = {}

        failed_buffers = {}

        for (topic_name, partition_index), data in buffers_to_flush.items():
            if not data:
                continue
            log_dir = f"{self.log_dir}/{topic_name}-{partition_index}"
            log_file = f"{log_dir}/{self.log_file_name}"
            try:
                await asyncio.to_thread(os.makedirs, log_dir, exist_ok=True)
                await self._append_file(log_file, bytes(data))
            except Exception as e:
                logger.error(
                    "failed_to_flush_buffer_to_disk",
                    topic_name=topic_name,
                    partition_index=partition_index,
                    error=str(e),
                    exc_info=True
                )
                # We couldn't flush this buffer, so we need to add it back to the failed buffers
                # to retry later and avoid data loss.
                failed_buffers[(topic_name, partition_index)] = data

        if failed_buffers:
            async with self.buffer_lock:
                for key, data in failed_buffers.items():
                    if key not in self.buffers:
                        self.buffers[key] = bytearray()
                    # Prepend the failed data so it gets flushed first next time
                    # We create a new bytearray to ensure correct ordering: [failed data] + [new data]
                    new_buffer = bytearray(data)
                    new_buffer.extend(self.buffers[key])
                    self.buffers[key] = new_buffer

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
                    processed_record["partitions"] = {}
                    topics[processed_record["topic_name"]] = processed_record
                elif processed_record["type"] == "partition":
                    for topic in topics:
                        if processed_record["topic_uuid"] == topics[topic]["topic_uuid"]:
                            topics[topic]["partitions"][processed_record["partition_index"]] = processed_record
                            break
                    else:
                        raise Exception(f"partition with no associated topic!|{processed_record}")
            record_batch_index += len(record_batch)
        return topics

    @staticmethod
    async def _read_file(file_path: str) -> bytes:
        """
        Reads the content of a file asynchronously and returns its binary content.

        This method reads the file in binary mode and executes the read operation
        in a separate thread to avoid blocking the main thread.

        :param file_path: The path of the file to be read.
        :return: The binary content of the file.
        """

        def _read():
            with open(file_path, "rb") as f:
                return f.read()
        return await asyncio.to_thread(_read)

    @staticmethod
    async def _write_file(file_path: str, data: bytes) -> None:
        """
        Writes binary data asynchronously to a specified file.

        This method uses a separate thread to handle file-writing operations,
        ensuring that the main event loop remains non-blocking.

        :param file_path: The path to the file where data will be written.
        :param data: The binary data to be written to the file.
        :return: None
        """

        def _write():
            with open(file_path, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
        await asyncio.to_thread(_write)

    @staticmethod
    async def _append_file(file_path: str, data: bytes) -> None:
        """
        Asynchronously appends binary data to a file.

        This function opens the specified file in binary append mode ('ab') and writes
        the provided binary data to the file. It performs the file I/O operation in a
        separate thread, ensuring non-blocking behavior in async applications.
        Also calls fsync to guarantee crash durability.

        :param file_path: The path of the file to which the data will be appended.
        :param data: The binary data to append to the file.
        :return: None
        """

        def _append():
            with open(file_path, "ab") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
        await asyncio.to_thread(_append)

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
            cluster_metadata[record_batch_index + 8 : record_batch_index + 12],
            byteorder="big"
        )
        return cluster_metadata[record_batch_index : record_batch_index + record_batch_size]

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
        num_records = int.from_bytes(record_batch[57:61], byteorder="big")
        record_array = record_batch[61:]
        for _ in range(num_records):
            record_size = int.from_bytes(record_array[0:1], byteorder="big") // 2
            if int.from_bytes(record_array[1:2], byteorder="big") == 0:
                record = record_array[7 : (record_size + 1)]
                next_record_index = record_size + 1
            else:
                record = record_array[9 : (record_size + 2)]
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
        record_type = int.from_bytes(record[0:1], byteorder="big")

        if record_type == 2:
            processed_record["type"] = "topic"

            topic_name_length = int.from_bytes(record[2:3], byteorder="big") - 1
            topic_name = record[3 : 3 + topic_name_length].decode("utf-8")
            processed_record["topic_name"] = topic_name

            topic_uuid_index = 3 + topic_name_length
            topic_uuid = uuid.UUID(bytes=record[topic_uuid_index : topic_uuid_index + 16])
            processed_record["topic_uuid"] = topic_uuid
        elif record_type == 3:
            processed_record["type"] = "partition"

            partition_index = int.from_bytes(record[2:6], byteorder="big")
            processed_record["partition_index"] = partition_index

            topic_uuid = uuid.UUID(bytes=record[6:22])
            processed_record["topic_uuid"] = topic_uuid

            num_replicas = int.from_bytes(record[22:23], byteorder="big")
            processed_record["num_replicas"] = num_replicas

            num_isr = int.from_bytes(record[27:28], byteorder="big")
            processed_record["num_isr"] = num_isr

            leader_id = int.from_bytes(record[34:38], byteorder="big")
            processed_record["leader_id"] = leader_id

            leader_epoch = int.from_bytes(record[38:42], byteorder="big")
            processed_record["leader_epoch"] = leader_epoch
        else:
            processed_record["type"] = "unknown"

        return processed_record
