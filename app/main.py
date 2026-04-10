import socket


def main():
    with (socket.create_server(("localhost", 9092), reuse_port=True) as server_socket):
        client, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_request = client.recv(1024)
        correlation_id = client_request[8:12]
        request_api_version = int.from_bytes(client_request[6:8], byteorder='big')
        if request_api_version in (0,1,2,3,4):
            error_code = int(0).to_bytes(2, byteorder='big')
            api_key_array_length = int(2).to_bytes(1, byteorder='big')
            api_key = int('18').to_bytes(2, byteorder='big')
            api_key_min_version = int(0).to_bytes(2, byteorder='big')
            api_key_max_version = int(4).to_bytes(2, byteorder='big')
            api_key_tag_buffer = int(0).to_bytes(1, byteorder='big')
            throttle_time = int(0).to_bytes(4, byteorder='big')
            throttle_time_tag_buffer = int(0).to_bytes(1, byteorder='big')
            resp_body = error_code + api_key_array_length + api_key + api_key_min_version + api_key_max_version \
                        + api_key_tag_buffer + throttle_time + throttle_time_tag_buffer
        else:
            error_code = int(35).to_bytes(2, byteorder='big')
            resp_body = error_code
        msg_size = len(correlation_id) + len(resp_body)
        resp_msg_size = int(msg_size).to_bytes(4, byteorder='big')
        resp = resp_msg_size + correlation_id + resp_body
        client.sendall(resp)



if __name__ == "__main__":
    main()
