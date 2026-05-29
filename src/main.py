import asyncio
import signal
import time
from asyncio import StreamReader, StreamWriter, Server
from typing import Any
import structlog
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from src.storage import Storage
from src.handlers import RequestHandler

# Configure structlog for JSON output
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory()
)

logger = structlog.get_logger(__name__)

PRODUCE_KEY = 0
FETCH_KEY = 1
API_VERSIONS_KEY = 18
DESCRIBE_TOPIC_PARTITIONS_KEY = 75
LOG_FILES_DIR = "/tmp/kraft-combined-logs"
LOG_FILE_NAME = "00000000000000000000.log"
CLIENT_READ_TIMEOUT = 5
CLIENT_WRITE_TIMEOUT = 5
MAX_WRITE_RETRIES = 3
MAX_CONCURRENT_CONNECTIONS = 100
GRACEFUL_SHUTDOWN_TIMEOUT = 10.0
BUFFER_FLUSH_INTERVAL = 10 # Flush buffer every 10 seconds
MAX_REQUEST_SIZE = 1024

# Reverse lookup for metrics
API_KEY_NAMES = {
    PRODUCE_KEY: "produce",
    FETCH_KEY: "fetch",
    API_VERSIONS_KEY: "api_versions",
    DESCRIBE_TOPIC_PARTITIONS_KEY: "describe_topic_partitions"
}

# Metrics Definitions
REQUEST_COUNT = Counter(
    'kafka_server_requests_total',
    'Total requests',
    ['api_key', 'status']
)
REQUEST_LATENCY = Histogram(
    'kafka_server_request_duration_seconds',
    'Request latency',
    ['api_key']
)
ACTIVE_CONNECTIONS = Gauge(
    'kafka_server_active_connections',
    'Number of active client connections'
)
FLUSH_DURATION = Histogram(
    'kafka_server_disk_flush_duration_seconds',
    'Disk flush latency'
)


async def client_handler(
        reader: StreamReader,
        writer: StreamWriter,
        handler: RequestHandler,
        topics: dict[str, dict[str, Any]],
        topics_by_uuid: dict[bytes, dict[str, Any]],
        shutdown_event: asyncio.Event
) -> None:
    """
    Handles communication with a single client, processing incoming requests and sending appropriate responses.

    This coroutine manages the flow of requests and responses for various supported API keys and logs errors
    for unsupported ones. Upon completion or error, the connection is closed.

    :param reader: The input stream to read data received from the client.
    :param writer: The output stream to send data back to the client.
    :param handler: An instance of the `RequestHandler` class responsible for handling specific API requests.
    :param topics: A dictionary containing the cluster metadata.
    :param topics_by_uuid: A dictionary containing the cluster metadata, keyed by UUID bytes.
    :param shutdown_event: An event triggered when the server is shutting down.
    :return: None
    """

    client_address: str = writer.get_extra_info("peername")
    log = logger.bind(client_address=client_address)
    log.info("connection_accepted")

    shutdown_task = asyncio.create_task(shutdown_event.wait())

    try:
        while not shutdown_event.is_set():
            try:
                # Read exactly 4 bytes for the message size
                size_task = asyncio.create_task(reader.readexactly(4))

                done, pending = await asyncio.wait(
                    [size_task, shutdown_task],
                    timeout=CLIENT_READ_TIMEOUT,
                    return_when=asyncio.FIRST_COMPLETED
                )

                if shutdown_task in done:
                    log.info("shutdown_event_received")
                    size_task.cancel()
                    break

                if not done:
                    # Timeout occurred
                    size_task.cancel()
                    log.warning("client_read_timeout")
                    break

                try:
                    size_bytes = size_task.result()
                except asyncio.IncompleteReadError:
                    break

                msg_size = int.from_bytes(size_bytes, byteorder="big")

                if msg_size <= 0 or msg_size > MAX_REQUEST_SIZE:
                    log.warning("invalid_message_size", msg_size=msg_size)
                    break

                # Read exactly msg_size bytes for the payload
                payload_task = asyncio.create_task(reader.readexactly(msg_size))

                done, pending = await asyncio.wait(
                    [payload_task, shutdown_task],
                    timeout=CLIENT_READ_TIMEOUT,
                    return_when=asyncio.FIRST_COMPLETED
                )

                if shutdown_task in done:
                    log.info("shutdown_event_received")
                    payload_task.cancel()
                    break

                if not done:
                    # Timeout occurred
                    payload_task.cancel()
                    log.warning("client_payload_read_timeout")
                    break

                try:
                    payload_bytes = payload_task.result()
                except asyncio.IncompleteReadError:
                    break

                client_request: bytes = size_bytes + payload_bytes

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("read_error", error=str(e), exc_info=True)
                break

            if not client_request:
                break
            request_api_key = int.from_bytes(client_request[4:6], byteorder="big")
            correlation_id = client_request[8:12]
            resp_body = b""

            api_name = API_KEY_NAMES.get(request_api_key, "unknown")
            start_time = time.perf_counter()
            status = "success"

            try:
                if request_api_key == API_VERSIONS_KEY:
                    resp_body = await handler.handle_api_version_requests(client_request)
                elif request_api_key == DESCRIBE_TOPIC_PARTITIONS_KEY:
                    resp_body = await handler.handle_describe_topic_partition_requests(client_request, topics)
                elif request_api_key == FETCH_KEY:
                    resp_body = await handler.handle_fetch_requests(client_request, topics_by_uuid)
                elif request_api_key == PRODUCE_KEY:
                    resp_body = await handler.handle_produce_requests(client_request, topics)
                else:
                    log.error("unsupported_api_key", api_key=request_api_key, request_hex=client_request.hex())
                    status = "error"
            except Exception as e:
                status = "error"
                log.error("request_handling_error", api_key=request_api_key, error=str(e), exc_info=True)
            finally:
                REQUEST_COUNT.labels(api_key=api_name, status=status).inc()
                REQUEST_LATENCY.labels(api_key=api_name).observe(time.perf_counter() - start_time)

            msg_size_out = len(correlation_id) + len(resp_body)
            resp_msg_size = int(msg_size_out).to_bytes(4, byteorder="big")
            resp = resp_msg_size + correlation_id + resp_body

            for attempt in range(MAX_WRITE_RETRIES):
                try:
                    writer.write(resp)
                    await asyncio.wait_for(writer.drain(), timeout=CLIENT_WRITE_TIMEOUT)
                    break
                except (asyncio.TimeoutError, ConnectionError) as e:
                    log.warning("write_attempt_failed", attempt=attempt + 1, error=str(e))
                    if attempt == MAX_WRITE_RETRIES - 1:
                        log.error("max_write_retries_reached")
                        return

    except Exception as e:
        log.exception("unhandled_error_in_client_handler", error=str(e))
    finally:
        shutdown_task.cancel()
        writer.close()
        try:
            await asyncio.wait_for(writer.wait_closed(), timeout=CLIENT_WRITE_TIMEOUT)
        except asyncio.TimeoutError:
            log.warning("timeout_waiting_for_connection_to_close")
        except Exception as e:
            log.warning("error_waiting_for_connection_to_close", error=str(e))


async def flush_buffers_periodically(storage: Storage, shutdown_event: asyncio.Event) -> None:
    """
    Periodically flushes the in-memory write buffers to disk.

    This background task runs in a loop, sleeping for a specified interval
    and then flushing the storage buffers. It continues until a shutdown
    event is triggered.

    :param storage: The storage instance with the buffers to flush.
    :param shutdown_event: The event that signals the server to shut down.
    :param storage:
    :param shutdown_event:
    :return:
    """

    while not shutdown_event.is_set():
        await asyncio.sleep(BUFFER_FLUSH_INTERVAL)
        logger.info("flushing_buffers_to_disk")
        start_time = time.perf_counter()
        try:
            await storage.flush_buffers()
        finally:
            FLUSH_DURATION.observe(time.perf_counter() - start_time)


async def main():
    host_ip = "localhost"
    host_port = 9092
    metrics_port = 8000

    # Start Prometheus metrics server
    start_http_server(metrics_port)
    logger.info("metrics_server_started", port=metrics_port)
    
    storage = Storage(LOG_FILES_DIR, LOG_FILE_NAME)
    handler = RequestHandler(storage)

    topics = await storage.load_metadata()

    # lookup index for Fetch requests
    topics_by_uuid = {topic_data["topic_uuid"].bytes: topic_data for topic_data in topics.values()}

    connection_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONNECTIONS)
    shutdown_event = asyncio.Event()
    active_connections = set()

    def signal_handler():
        logger.info("received_termination_signal")
        shutdown_event.set()

    # Register signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    async def handle_client(reader: StreamReader, writer: StreamWriter):
        await connection_semaphore.acquire()
        ACTIVE_CONNECTIONS.inc()

        # Track the active connection task
        current_task = asyncio.current_task()
        active_connections.add(current_task)

        try:
            await client_handler(reader, writer, handler, topics, topics_by_uuid, shutdown_event)
        finally:
            ACTIVE_CONNECTIONS.dec()
            active_connections.remove(current_task)
            connection_semaphore.release()

    server: Server = await asyncio.start_server(
        client_connected_cb=handle_client,
        host=host_ip,
        port=host_port,
        reuse_port=True
    )
    logger.info("server_started", host=host_ip, port=host_port)

    # Start the background task for flushing buffers
    flush_task = asyncio.create_task(flush_buffers_periodically(storage, shutdown_event))

    async def serve():
        async with server:
            try:
                await shutdown_event.wait()
            finally:
                logger.info("stopping_server")
                server.close()
                await server.wait_closed()

                if active_connections:
                    logger.info("waiting_for_active_connections_to_close", count=len(active_connections))
                    done, pending = await asyncio.wait(active_connections, timeout=GRACEFUL_SHUTDOWN_TIMEOUT)

                    if pending:
                        logger.warning("forcefully_cancelling_pending_connections", count=len(pending))
                        for task in pending:
                            task.cancel()

                # Stop the flush task and do a final flush
                logger.info("stopping_buffer_flush_task")
                flush_task.cancel()
                await asyncio.gather(flush_task, return_exceptions=True)
                logger.info("performing_final_buffer_flush")
                await storage.flush_buffers()

    await serve()
    logger.info("server_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
