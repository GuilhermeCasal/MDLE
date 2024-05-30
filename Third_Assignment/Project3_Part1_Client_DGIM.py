############################################################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType
import pandas as pd
import numpy as np


def new_bit(buckets, bit, N):
        if buckets: 
            # Increment the buckets timestamp
            buckets = [(time+1,count) for time, count in buckets]

            # Remove outdated buckets (exceed sliding window size N)
            new_buckets = []
            for bucket in buckets:
                if bucket[0] <= N:
                    new_buckets.append(bucket)
                else:
                    break
            buckets = new_buckets

        # Add a new bucket if the incoming bit is 1
        if bit == 1:
            buckets.append((0, 1))
            buckets = check_and_merge(buckets)
        
        return buckets


def check_and_merge(buckets):
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
            for i, bucket in enumerate(buckets):
                if bucket[1] in count_map:
                    count_map[bucket[1]].append(i)
                else:
                    count_map[bucket[1]] = [i] 
                
                # If there are 3 buckets of the same size, merge the oldest two
                if len(count_map[bucket[1]]) == 3:
                    index = count_map[bucket[1]]
                    buckets = merge_buckets(buckets, index)
                    merge_ocurred = True
                    break
            
            # Exit the loop if no merges occurred in this iteration
            if not merge_ocurred:
                break
        
        return buckets


def merge_buckets(buckets, index):
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
        first_bucket = buckets[first]
        second_bucket = buckets[second]
        
        # Determine the minimum timestamp and sum of the two buckets
        ts = min(first_bucket[0], second_bucket[0])
        sum = first_bucket[1] + second_bucket[1]
        merge = (ts,sum)

        # Update the first bucket with the merged bucket and remove the second bucket
        buckets[first] = merge
        del buckets[second]

        return buckets


def estimate_count(buckets, k=5):
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
        for i, (time, size) in enumerate(sorted(buckets, key=lambda x: x[0])):

            # Check if the current bucket's time is within range and the next bucket's time is beyond k
            if (time <= k-1 and buckets[len(buckets)-1-i-1][0] > k-1) or i == len(buckets)-1:
                count += np.ceil(size/2)
                break
            else:
                count += size
        return count


def update_buckets(stream_id, df_iter, state):

    if not state.exists:
        buckets = []
        count_ones = []
        state.update((buckets, count_ones))
    else:
        # Retrieve the existing state
        buckets = state.get[0]
        count_ones = state.get[1]

    # Process each partition of the DataFrame
    for df in df_iter:
        # Extract stream_id and bit value from the row
        bit = df["value"].iloc[1]
        # Update the DGIM algorithm with the incoming bit
        buckets = new_bit(buckets, bit, 10)
        # Increase the timestamp of the real bits count
        count_ones = [time+1 for time in count_ones]
        # Update the real count by adding the current timestep (0)
        if bit == 1:
            count_ones.append(0)

    # Update the state
    state.update((buckets,count_ones))

    # Yield empty DataFrame (no output needed for each batch)
    yield pd.DataFrame({"stream_id": [str(stream_id)], "count_ones": [count_ones], "buckets": [buckets]})


if __name__ == "__main__":

    # Define the state schema
    bucket_schema = StructType([
        StructField("timestamp", LongType()),
        StructField("size", IntegerType())
    ])
    state_schema = StructType([
        StructField("buckets", ArrayType(bucket_schema)),
        StructField('count_ones', ArrayType(LongType())),
    ])

    # Define the output schema with fields for counts
    output_schema = StructType([
        StructField("stream_id", StringType()),             
        StructField('count_ones', ArrayType(LongType())),
        StructField("buckets", ArrayType(bucket_schema)),
    ])

    # Initialize Spark session
    spark = SparkSession.builder \
                         .appName("DGIMStreaming") \
                          .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Read streaming data from socket (or any other source)
    data = spark.readStream \
                 .format("socket") \
                  .option("host", "localhost") \
                   .option("port", 9999) \
                    .option("includeTimestamp", "false") \
                     .load()

    # Parse JSON data and explode to individual rows
    data = data.selectExpr("cast(value as string) as json_str") \
                .select(F.from_json("json_str", "MAP<STRING, INT>").alias("bitstream")) \
                 .selectExpr("explode(bitstream) as (stream_id, value)")
            
    # # Print the received data
    # query = data.writeStream \
    #              .foreach(print) \
    #               .start()
    # query.awaitTermination()

    # Update state for each group using applyInPandasWithState
    updated_data = data.groupby("stream_id").applyInPandasWithState(
        update_buckets,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="append",
        timeoutConf=GroupStateTimeout.NoTimeout
    )

    from pyspark.sql.functions import pandas_udf
    @pandas_udf(IntegerType())
    def estimate_udf(buckets):
        return estimate_count(buckets)

    # Start the query
    query = updated_data.writeStream \
                         .outputMode("append") \
                          .format("console") \
                           .start()

    # Await termination
    query.awaitTermination()