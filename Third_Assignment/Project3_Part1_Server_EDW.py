############################################################################################################################################

import socket
import sys
from binary_multi_stream_generator_DGIM import BinaryMultiStreamGenerator


class SocketServer():
    def __init__(self, host, port):
        # Create a socket object
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the host and port
        server_socket.bind((HOST, PORT))

        # Listen for incoming connections
        server_socket.listen(1)
        print('Server is listening on {}:{}'.format(HOST, PORT))

        # Accept a connection from a client
        self.client_socket, client_address = server_socket.accept()
        print('Accepted connection from {}:{}'.format(client_address[0], client_address[1]))


if __name__ == "__main__":

    # Define the host and port
    if len(sys.argv) == 3:
        HOST, PORT = sys.argv[1], int(sys.argv[2])
    else:
        HOST, PORT = 'localhost', 9999


    ss = SocketServer(HOST, PORT)
    num_streams = 20
    generator = BinaryMultiStreamGenerator(num_streams)
    generator.generate(ss.client_socket)