
import numpy as np

class DGIM:
    """
    DGIM (Datar-Gionis-Indyk-Motwani) algorithm for efficiently estimating the number of 1s in a sliding window of a bit stream.
    
    The DGIM algorithm is designed to estimate the number of 1s within a specified time window (`k`) of a binary stream, 
    while operating within a sliding window of fixed size (`N`). It achieves this by maintaining a set of buckets, 
    each representing a segment of the stream, and periodically merging buckets to ensure accuracy and efficiency.
    
    Attributes:
        N (int): The size of the sliding window. Controls the maximum number of bits considered for estimation.
        k (int): The specific time window from the current time to `k` to count the 1s.
        buckets (list): List of tuples representing the buckets. Each tuple contains (timestamp, size).
        
    Methods:
        __init__(self, N, k): Initializes the DGIM instance with the sliding window size, time window, and bit stream.
        new_bit(self, bit): Processes a new incoming bit and updates the buckets accordingly.
        check_and_merge(self): Checks the buckets and merges them if necessary to maintain the DGIM algorithm's constraints.
        _merge_buckets(self, index): Merges the two oldest buckets among the three that have the same size.
        count_ones(self): Estimates the number of 1s in the bit stream from the current time to `k`.
    """

    def __init__(self, N, k, buckets=None):
        """
        Initializes the DGIM algorithm instance.
        
        Arguments:
        N (int): The maximum size of the sliding window.
        k (int): The value up to which the number of 1s will be counted, from the current time to k (k <= N).
        buckets (array of tuples): Buckets in a given time stamp
        
        Attributes:
        self.N (int): Stores the maximum size of the sliding window.
        self.k (int): Stores the value up to which the number of 1s will be counted.
        self.buckets (list): Initializes an empty list to store the buckets.
        """
        self.N = N
        self.k = k
        self.buckets = buckets if buckets else []


    def new_bit(self, bit):
        """
        Processes a new incoming bit and updates the buckets accordingly.
        
        Arguments:
        bit (int): The new bit to be added (either 0 or 1).
        
        Steps:
        1. Increment the timestamp of each existing bucket.
        2. Remove buckets that have exceeded the sliding window size `N`.
        3. If the incoming bit is 1, create a new bucket with the current timestamp and a count of 1.
        4. Merge buckets if necessary to maintain the DGIM algorithm's constraints.
        """

        # Increment the buckets timestamp
        self.buckets = [(time+1,count) for time, count in self.buckets]

        # Remove outdated buckets (exceed sliding window size N)
        new_buckets = []
        for bucket in self.buckets:
            if bucket[0] <= self.N:
                new_buckets.append(bucket)
            else:
                break
        self.buckets = new_buckets

        # Add a new bucket if the incoming bit is 1
        if bit == 1:
            self.buckets.append((0, 1))
            self.check_and_merge()
        
        
    def check_and_merge(self):
        """
        Checks the buckets and merges them if necessary to maintain the DGIM algorithm's constraints.
        
        Steps:
        1. Continuously check and merge buckets until no more merges are needed.
        2. Use a dictionary to count occurrences of bucket sizes.
        3. If any bucket size occurs 3 times, merge the oldest two buckets.
        """

        while True:
            merge_ocurred = False
            count_map = {}

            # Iterate through the buckets and count occurrences of each bucket size
            for i, bucket in enumerate(self.buckets):
                if bucket[1] in count_map:
                    count_map[bucket[1]].append(i)
                else:
                    count_map[bucket[1]] = [i] 
                
                # If there are 3 buckets of the same size, merge the oldest two
                if len(count_map[bucket[1]]) == 3:
                    index = count_map[bucket[1]]
                    self._merge_buckets(index)
                    merge_ocurred = True
                    break
            
            # Exit the loop if no merges occurred in this iteration
            if not merge_ocurred:
                break


    def _merge_buckets(self, index):
        """
        Merges the two oldest buckets among the three that have the same size.
        
        Arguments:
        index (list): List of indices of the buckets to be merged.
        
        Steps:
        1. Identify the two oldest buckets from the indices.
        2. Merge the two oldest buckets by combining their timestamps and counts.
        3. Update the buckets list with the new merged bucket, removing the redundant one.
        """

        first, second = index[0], index[1]
        first_bucket = self.buckets[first]
        second_bucket = self.buckets[second]
        
        # Determine the minimum timestamp and sum of the two buckets
        ts = min(first_bucket[0], second_bucket[0])
        sum = first_bucket[1] + second_bucket[1]
        merge = (ts,sum)

        # Update the first bucket with the merged bucket and remove the second bucket
        self.buckets[first] = merge
        del self.buckets[second]


    def estimate_count(self):
        """
        Estimates the number of 1s in the bit stream from the current time to k.
        
        Returns:
        count (float): The estimated count of 1s within the specified range.
        
        Steps:
        1. Initialize the count to 0.
        2. Sort the buckets by their timestamps in ascending order.
        3. Iterate through the sorted buckets.
        4. If the current bucket's timestamp is less than or equal to k and the next bucket's timestamp is greater than k,
           or if the current bucket is the last one in the list, add half of the current bucket's size to the count and exit the loop.
        5. Otherwise, add the entire size of the current bucket to the count and advance to the next bucket.
        """

        count = 0

        # Sort buckets by their timestamps in ascending order and iterate through them
        for i, (time, size) in enumerate(sorted(self.buckets, key=lambda x: x[0])):

            # Check if the current bucket's time is within range and the next bucket's time is beyond k
            if (time <= self.k-1 and self.buckets[len(self.buckets)-1-i-1][0] > self.k-1) or i == len(self.buckets)-1:
                count += np.ceil(size/2)
                break
            else:
                count += size
        return count
    

if __name__ == '__main__':

    N = 5
    k = 2
    bit_stream = [0,1,0,1,1,1,1,1,1,0,1,1,1,0,1,1,0,1]

    print(bit_stream,'\n')

    dgim = DGIM(N=N, k=k)

    timestamp = 0

    for bit in bit_stream:
        dgim.new_bit(bit)

        if timestamp >= N-1:
            print(f'Buckets: {dgim.buckets}')
            print(f'Stream to count: {bit_stream[timestamp-k:timestamp]}')
            print(f'Estimate: {dgim.estimate_count()}; True: {np.sum(bit_stream[timestamp-k:timestamp])}\n')
        
        timestamp += 1