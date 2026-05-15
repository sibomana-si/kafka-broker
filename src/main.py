import asyncio
import logging
from asyncio import StreamReader, StreamWriter, Server

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


async def client_handler(reader: StreamReader, writer: StreamWriter, handler: RequestHandler, storage: Storage) -> None:
    """
    Handles communication with a single client, processing incoming requests and sending appropriate responses.

    This coroutine manages the flow of requests and responses for various supported API keys and logs errors
    for unsupported ones. Upon completion or error, the connection is closed.

    :param reader: The input stream to read data received from the client.
    :param writer: The output stream to send data back to the client.
    :param handler: An instance of the `RequestHandler` class responsible for handling specific API requests.
    :param storage: An instance of the `Storage` class for managing metadata required to handle requests.
    :return: None
    """

    client_address: str = writer.get_extra_info('peername')
    logger.info(f"Connection accepted from {client_address}")

    api_handlers = {
        API_VERSIONS_KEY: handler.handle_api_version_requests,
        DESCRIBE_TOPIC_PARTITIONS_KEY: handler.handle_describe_topic_partition_requests,
        FETCH_KEY: handler.handle_fetch_requests,
        PRODUCE_KEY: handler.handle_produce_requests
    }

    try:
        while True:
            try:
                client_request: bytes = await asyncio.wait_for(reader.read(1024), timeout=CLIENT_READ_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(f"Client read timeout: {client_address}")
                break

            if not client_request:
                break
            request_api_key = int.from_bytes(client_request[4:6], byteorder='big')
            correlation_id = client_request[8:12]
            resp_body = b''

            if request_api_key in api_handlers:
                topics = storage.load_metadata()
                resp_body = await api_handlers[request_api_key](client_request, topics)
            else:
                logger.error(f"Unsupported API: {request_api_key}|{client_request.hex()}")

            msg_size = len(correlation_id) + len(resp_body)
            resp_msg_size = int(msg_size).to_bytes(4, byteorder='big')
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


async def main():
    host_ip = "localhost"
    host_port = 9092
    
    storage = Storage(LOG_FILES_DIR, LOG_FILE_NAME)
    handler = RequestHandler(storage)

    async def handle_client(reader: StreamReader, writer: StreamWriter):
        await client_handler(reader, writer, handler, storage)

    server: Server = await asyncio.start_server(client_connected_cb=handle_client,
                                                host=host_ip,
                                                port=host_port,
                                                reuse_port=True)
    logger.info(server)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
