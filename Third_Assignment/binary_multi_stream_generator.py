import socket
import time
import random
import sys
import json


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


class BinaryMultiStreamGenerator():
  def __init__(self, num_streams):
    super().__init__()
    self.probs = [max(0.01, round(random.random() / 10, 2)) for _ in range(num_streams)]

  def generate(self, client_socket):
    while True:
        bits = [1 if random.random() < prob else 0 for prob in self.probs]
        #data = str(bits) 
        # send data as dict
        data = json.dumps(dict([(i, b) for i, b in enumerate(bits)]))
        client_socket.send((data + "\n").encode("utf-8"))
        time.sleep(5)


if __name__ == "__main__":
    random.seed(57)

    # Define the host and port
    if len(sys.argv) == 3:
        HOST, PORT = sys.argv[1], int(sys.argv[2])
    else:
        HOST, PORT = 'localhost', 9999


    ss = SocketServer(HOST, PORT)
    num_streams = 10
    generator = BinaryMultiStreamGenerator(num_streams)
    generator.generate(ss.client_socket)

  