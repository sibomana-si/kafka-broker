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
            request_api_version = int.from_bytes(client_request[6:8], byteorder='big')
            correlation_id = client_request[8:12]
            client_id_length = int.from_bytes(client_request[12:14], byteorder='big')

            if request_api_version in (0, 1, 2, 3, 4):
                error_code = int(0).to_bytes(2, byteorder='big')
                api_key_array_length = int(3).to_bytes(1, byteorder='big')
                tag_buffer = int(0).to_bytes(1, byteorder='big')
                throttle_time = int(0).to_bytes(4, byteorder='big')

                message_field_size = 4
                api_key_field_size = 2
                api_version_field_size = 2
                correlation_id_field_size = 4
                client_id_field_size = 2
                tag_buffer_field_size = 1
                topics_array_field_size = 1
                topic_name_field_size = 1

                topic_name_length_index = (message_field_size + api_key_field_size + api_version_field_size
                                           + correlation_id_field_size + client_id_field_size + client_id_length
                                           + tag_buffer_field_size + topics_array_field_size)

                topic_name_length = int.from_bytes(
                    client_request[topic_name_length_index:topic_name_length_index + topic_name_field_size],
                    byteorder='big'
                )

                topic_name_index = topic_name_length_index + topic_name_field_size
                topic_name = client_request[topic_name_index: (topic_name_index + topic_name_length - 1)]
                topics_array_length = client_request[topic_name_length_index - 1: topic_name_length_index]
                topics_array_length_int = int.from_bytes(topics_array_length, byteorder='big')

                if topics_array_length_int == 2:
                    # topic response components
                    topic_id = int(0).to_bytes(16, byteorder='big')
                    topic_error_code = int(3).to_bytes(2, byteorder='big')
                    is_internal = int(0).to_bytes(1, byteorder='big')
                    partition_array_size = int(1).to_bytes(1, byteorder='big')
                    topic_authorized_operations = int(0).to_bytes(4, byteorder='big')
                    next_cursor = int(255).to_bytes(1, byteorder='big')

                    resp_body = tag_buffer + throttle_time + topics_array_length + topic_error_code \
                                + int(topic_name_length).to_bytes(1, byteorder='big') + topic_name \
                                + topic_id + is_internal + partition_array_size + topic_authorized_operations \
                                + tag_buffer + next_cursor + tag_buffer
                else:
                    # Supported ApiVersion
                    api_key = int('18').to_bytes(2, byteorder='big')
                    api_key_min_version = int(0).to_bytes(2, byteorder='big')
                    api_key_max_version = int(4).to_bytes(2, byteorder='big')

                    # DescribeTopicsPartitions API
                    topics_api_key = int('75').to_bytes(2, byteorder='big')
                    topics_api_key_min_version = int(0).to_bytes(2, byteorder='big')
                    topics_api_key_max_version = int(0).to_bytes(2, byteorder='big')

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
