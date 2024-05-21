import socket
import random
import time

class WordStreamGenerator:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.words = ['Spark', 'Hadoop', 'Hive', 'Stream']

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.listen(1)
            print(f"Server listening on {self.host}:{self.port}")

            client_socket, addr = server_socket.accept()
            print(f"Accepted connection from {addr}")

            with client_socket:
                while True:
                    word = random.choice(self.words)
                    client_socket.sendall((word + '\n').encode('utf-8'))
                    print(f"Sent: {word}")
                    time.sleep(5)  # Adjust the sleep time as needed

if __name__ == "__main__":
    host = 'localhost'
    port = 9999
    generator = WordStreamGenerator(host, port)
    generator.start()
