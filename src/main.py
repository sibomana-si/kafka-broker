import asyncio
import logging
import signal
from asyncio import StreamReader, StreamWriter, Server
from typing import Any

from storage import Storage
from handlers import RequestHandler

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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


async def client_handler(
        reader: StreamReader,
        writer: StreamWriter,
        handler: RequestHandler,
        topics: dict[str, dict[str, Any]],
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
    :param shutdown_event: An event triggered when the server is shutting down.
    :return: None
    """

    client_address: str = writer.get_extra_info("peername")
    logger.info(f"Connection accepted from {client_address}")

    api_handlers = {
        API_VERSIONS_KEY: handler.handle_api_version_requests,
        DESCRIBE_TOPIC_PARTITIONS_KEY: handler.handle_describe_topic_partition_requests,
        FETCH_KEY: handler.handle_fetch_requests,
        PRODUCE_KEY: handler.handle_produce_requests
    }

    try:
        while not shutdown_event.is_set():
            try:
                read_task = asyncio.create_task(reader.read(1024))
                shutdown_task = asyncio.create_task(shutdown_event.wait())
                done, pending = await asyncio.wait(
                    [read_task, shutdown_task],
                    timeout=CLIENT_READ_TIMEOUT,
                    return_when=asyncio.FIRST_COMPLETED
                )

                if shutdown_task in done:
                    logger.info(f"Shutdown event received, closing connection to {client_address}")
                    read_task.cancel()
                    break

                if not done:
                    # Timeout occurred
                    read_task.cancel()
                    logger.warning(f"Client read timeout: {client_address}")
                    break

                client_request: bytes = read_task.result()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Read error: {e}")
                break

            if not client_request:
                break
            request_api_key = int.from_bytes(client_request[4:6], byteorder="big")
            correlation_id = client_request[8:12]
            resp_body = b""

            if request_api_key in api_handlers:
                resp_body = await api_handlers[request_api_key](client_request, topics)
            else:
                logger.error(f"Unsupported API: {request_api_key}|{client_request.hex()}")

            msg_size = len(correlation_id) + len(resp_body)
            resp_msg_size = int(msg_size).to_bytes(4, byteorder="big")
            resp = resp_msg_size + correlation_id + resp_body

            for attempt in range(MAX_WRITE_RETRIES):
                try:
                    writer.write(resp)
                    await asyncio.wait_for(writer.drain(), timeout=CLIENT_WRITE_TIMEOUT)
                    break
                except (asyncio.TimeoutError, ConnectionError) as e:
                    logger.warning(f"Write attempt {attempt + 1} failed for {client_address}: {e}")
                    if attempt == MAX_WRITE_RETRIES - 1:
                        logger.error(f"Max write retries reached for {client_address}. Closing connection.")
                        return

    except Exception as e:
        logger.exception(f"Error in client_handler: {client_address}|{e}")
    finally:
        writer.close()
        try:
            await asyncio.wait_for(writer.wait_closed(), timeout=CLIENT_WRITE_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout while waiting for connection to close: {client_address}")
        except Exception as e:
            logger.warning(f"Error while waiting for connection to close: {client_address}|{e}")


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
        logger.info("Flushing buffers to disk...")
        await storage.flush_buffers()


async def main():
    host_ip = "localhost"
    host_port = 9092
    
    storage = Storage(LOG_FILES_DIR, LOG_FILE_NAME)
    topics = await storage.load_metadata()
    handler = RequestHandler(storage)

    connection_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONNECTIONS)
    shutdown_event = asyncio.Event()
    active_connections = set()

    def signal_handler():
        logger.info("Received termination signal. Initiating graceful shutdown...")
        shutdown_event.set()

    # Register signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    async def handle_client(reader: StreamReader, writer: StreamWriter):
        await connection_semaphore.acquire()

        # Track the active connection task
        current_task = asyncio.current_task()
        active_connections.add(current_task)

        try:
            await client_handler(reader, writer, handler, topics, shutdown_event)
        finally:
            active_connections.remove(current_task)
            connection_semaphore.release()

    server: Server = await asyncio.start_server(
        client_connected_cb=handle_client,
        host=host_ip,
        port=host_port,
        reuse_port=True
    )
    logger.info(f"Server started on {host_ip}:{host_port}")

    # Start the background task for flushing buffers
    flush_task = asyncio.create_task(flush_buffers_periodically(storage, shutdown_event))

    async def serve():
        async with server:
            try:
                await shutdown_event.wait()
            finally:
                logger.info("Stopping server from accepting new connections.")
                server.close()
                await server.wait_closed()

                if active_connections:
                    logger.info(f"Waiting for {len(active_connections)} active connections to close...")
                    done, pending = await asyncio.wait(active_connections, timeout=GRACEFUL_SHUTDOWN_TIMEOUT)

                    if pending:
                        logger.warning(f"Forcefully cancelling {len(pending)} pending connections.")
                        for task in pending:
                            task.cancel()

                # Stop the flush task and do a final flush
                logger.info("Stopping buffer flush task...")
                flush_task.cancel()
                await asyncio.gather(flush_task, return_exceptions=True)
                logger.info("Performing final buffer flush...")
                await storage.flush_buffers()

    await serve()
    logger.info("Server shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
