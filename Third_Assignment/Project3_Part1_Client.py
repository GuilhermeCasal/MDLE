# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode, split


# spark = SparkSession.builder \
#         .appName("Project3_MLSD_Part1") \
#         .getOrCreate()


# socketData = spark.readStream \
#                  .format("socket") \
#                   .option("host", "localhost") \
#                    .option("port", 9999) \
#                     .load()


# split_lines = socketData.withColumn("value", explode(split(socketData["value"], ",")))

# query = split_lines.writeStream \
#                     .outputMode("append") \
#                      .format("console") \
#                       .start()

# query.awaitTermination()

############################################################################################################################################

class DGIM:
    def __init__(self, N, k, bit_stream):
        self.N = N
        self.k = k
        self.bit_stream = bit_stream
        self.buckets = []
        self.history = {}

    def add_bit(self, bit, timestamp):
        # Remove outdated buckets
        old_len = len(self.buckets)
        self.buckets = [bucket for bucket in self.buckets if timestamp - bucket[1] < self.N]

        if (old_len != len(self.buckets)):
            print(f'Number of buckets discarded by timestamp incompatibility: {old_len-len(self.buckets)}')
            print()

        # Add new bucket if the bit is 1
        if bit == 1:
            self.buckets.append((1, timestamp))

        # Merge buckets if needed
        self._merge_buckets()

        if timestamp % self.N == 0:
            self.count_ones(timestamp)

    def _merge_buckets(self):
        i = len(self.buckets) - 1
        j = i + 1
        while i > 1:
            if self.buckets[i][0] == self.buckets[i - 1][0] == self.buckets[i - 2][0]:
                self.buckets[i - 1] = (self.buckets[i - 1][0] * 2, self.buckets[i - 1][1])
                del self.buckets[i - 2]
                i -= 1
            else:
                i -= 1

        if j != len(self.buckets):
            print(f'Number of merged buckets: {j-len(self.buckets)}')
            print()

    def _updateHistory(self, timestamp, estimate):
        begin = timestamp-self.k if timestamp >= self.k else 0
        if begin == 0 :
            true_count = sum(self.bit_stream[0:1])
        else:
            true_count = sum(self.bit_stream[begin:timestamp])
        self.history[timestamp] = [true_count, estimate]


    def count_ones(self, timestamp):
        count = 0
        print(self.buckets)
        for i, (num, time) in enumerate(sorted(self.buckets, key=lambda x: x[1], reverse=True)):
            if timestamp == 0:
                count += num
            if timestamp-time < self.k:
                if i < len(self.buckets)-1:
                    count += num
                else:
                    count += num // 2
        self._updateHistory(timestamp, count)

        return count

    def results(self):
        print('\n\n')
        for timestamp, (true, estimate) in self.history.items():
            print(f"Timestamp: {timestamp}; Estimated count of 1s in the last {self.k} bits: {estimate}; True count of 1s: {true}")



import random

def generate_bit_stream(num_bits, prob):
    return [1 if random.random() < prob else 0 for _ in range(num_bits)]

def test_dgim():
    N = 10  # Total window size
    k = 5   # Sub-window size for estimation
    prob = 0.5  # Probability of a bit being 1
    num_bits = 21  # Total number of bits to generate

    # Generate a synthetic bit stream
    bit_stream = generate_bit_stream(num_bits, prob)
    print("Generated bit stream:", bit_stream)

    dgim = DGIM(N, k, bit_stream)
    timestamp = 0

    # Add bits to DGIM and print estimates at intervals
    for bit in bit_stream:
        dgim.add_bit(bit, timestamp)
        timestamp += 1
    
    dgim.results()


if __name__ == "__main__":
    test_dgim()



import socket

# class SocketClient():
#     def __init__(self, host, port):
#         # Create a socket object
#         self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#         # Connect to the server
#         self.client_socket.connect((host, port))
#         print('Connected to server at {}:{}'.format(host, port))

#     def read_data(self):
#         try:
#             while True:
#                 # Receive data from the server
#                 data = self.client_socket.recv(1024)
#                 if not data:
#                     break
#                 # Decode and print the received data
#                 print(data.decode('utf-8').strip())
#         except Exception as e:
#             print("An error occurred:", e)
#         finally:
#             self.client_socket.close()

# if __name__ == "__main__":
#     # Define the server host and port
#     HOST, PORT = 'localhost', 9999
    
#     # Create a client instance and connect to the server
#     client = SocketClient(HOST, PORT)
    
#     # Read and process data from the server
#     client.read_data()

############################################################################################################################################
