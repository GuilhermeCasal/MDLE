import time
import random


class BinaryMultiStreamGenerator():
  def __init__(self, num_streams):
    super().__init__()
    self.probs = [max(0.01, round(random.random() / 10, 2)) for _ in range(num_streams)]

  def generate(self, client_socket):
    while True:
        bits = [1 if random.random() < prob else 0 for prob in self.probs]
        client_socket.send((str(bits) + "\n").encode("utf-8"))
        time.sleep(10)