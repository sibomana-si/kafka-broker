import socket


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client, addr = server.accept()
    resp_msg_size = int('0').to_bytes(4, byteorder='big')
    resp_msg_header = int('7').to_bytes(4, byteorder='big')
    resp = resp_msg_size + resp_msg_header
    client.send(resp)
    client.close()



if __name__ == "__main__":
    main()
