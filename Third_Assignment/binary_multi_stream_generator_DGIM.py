import time
import random
import json

random.seed(57)

class BinaryMultiStreamGenerator():
  def __init__(self, num_streams):
    super().__init__()
    # self.probs = [max(0.01, round(random.random() / 10, 2)) for _ in range(num_streams)]
    self.probs = [0.8 for _ in range(num_streams)]


  def generate(self, client_socket):
    while True:
        bits = [1 if random.random() < prob else 0 for prob in self.probs]
        #data = str(bits) 
        # send data as dict
        data = json.dumps(dict([(i, b) for i, b in enumerate(bits)]))
        client_socket.send((data + "\n").encode("utf-8"))
        time.sleep(0.1)