import asyncio
import logging
from asyncio import StreamReader, StreamWriter, Server


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
    client_address: str = writer.get_extra_info('peername')
    logger.info(f"Connection accepted from {client_address}")
    try:
        while True:
            client_request: bytes = await reader.read(1024)
            correlation_id = client_request[8:12]
            request_api_version = int.from_bytes(client_request[6:8], byteorder='big')
            if request_api_version in (0, 1, 2, 3, 4):
                error_code = int(0).to_bytes(2, byteorder='big')
                api_key_array_length = int(3).to_bytes(1, byteorder='big')

                # Supported ApiVersion
                api_key = int('18').to_bytes(2, byteorder='big')
                api_key_min_version = int(0).to_bytes(2, byteorder='big')
                api_key_max_version = int(4).to_bytes(2, byteorder='big')
                tag_buffer = int(0).to_bytes(1, byteorder='big')

                # DescribeTopicsPartitions API
                topics_api_key = int('75').to_bytes(2, byteorder='big')
                topics_api_key_min_version = int(0).to_bytes(2, byteorder='big')
                topics_api_key_max_version = int(0).to_bytes(2, byteorder='big')
                throttle_time = int(0).to_bytes(4, byteorder='big')

                resp_body = error_code + api_key_array_length \
                            + api_key + api_key_min_version + api_key_max_version + tag_buffer \
                            + topics_api_key + topics_api_key_min_version + topics_api_key_max_version + tag_buffer \
                            + throttle_time + tag_buffer
            else:
                error_code = int(35).to_bytes(2, byteorder='big')
                resp_body = error_code
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
