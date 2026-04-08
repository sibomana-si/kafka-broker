import socket


def main():
    with (socket.create_server(("localhost", 9092), reuse_port=True)
          as server_socket):
        client, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_request = client.recv(1024)
        resp_msg_size = int('0').to_bytes(4, byteorder='big')
        correlation_id = client_request[8:12]
        resp = resp_msg_size + correlation_id
        client.sendall(resp)




if __name__ == "__main__":
    main()
