import numpy as np

class ExponentiallyDecayingWindows:
    def __init__(self, c, threshold):

        self.c = c
        self.threshold = threshold
        self.counts = [0,0]
    def update_counts(self, bit):
        # Decay counts
        self.counts = [count*(1 - self.c) for count in self.counts]

        # Update counts based on the incoming bit
        self.counts[bit] += 1

        # Remove the counts for unpopular bits (if bellow a threhsold)
        self.counts = [count if count >= self.threshold else 0 for count in self.counts]
        

if __name__ == '__main__':
    c = 0.1  # Decay factor
    threshold = 0.5  # Threshold for not common count elemination

    # Initialize EDW
    edw = ExponentiallyDecayingWindows(c, threshold)

    # Simulate a binary stream
    binary_stream = [1, 0, 1, 1, 0, 1, 0, 1, 1, 0]
    timestamp = 0

    # Update counts and calculate weights for each bit in the stream
    for bit in binary_stream:
        timestamp += 1
        edw.update_counts(bit)
        weight_0, weight_1 = edw.counts
        print(binary_stream[:timestamp])
        print(f"Weight of 1: {round(weight_1,0)}, Weight of 0: {round(weight_0,0)}")
        print(f'Number of 1: {np.sum(binary_stream[:timestamp])}, Number of 0: {timestamp-np.sum(binary_stream[:timestamp])}')
