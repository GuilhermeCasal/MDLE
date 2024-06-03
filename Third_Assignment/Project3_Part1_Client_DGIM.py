############################################################################################################################################
#
# DGIM (Datar-Gionis-Indyk-Motwani) Algorithm - PySpark Streaming Implementation
#
############################################################################################################################################


# DGIM Algorithm Hyperparameters:
# - N: window size that is allowed to keep in memory
# - k: size ask for the estimation
# - num_streams: number of streams (defined in the server)
# - prob: one output probability (defined in the server)

# By defining this hyperparameters the outcomes of the algorithm are changed


# Import the necessary libraries to run the code
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType
import pandas as pd


# Function to update the buckets based on incoming bits
def new_bit(buckets, bit, N):
    """
    Update the buckets based on incoming bits using the DGIM algorithm.

    Arguments:
    buckets (list): List of tuples representing buckets, each containing a timestamp and count.
    bit (int): The incoming bit (0 or 1).
    N (int): The sliding window size or maximum number of buckets to maintain in the DGIM algorithm.

    Returns:
    list: Updated list of buckets after processing the incoming bit.

    Steps:
    1. Increment the timestamp of each bucket by 1.
    2. Remove outdated buckets that exceed the sliding window size N.
    3. Add a new bucket with timestamp 0 if the incoming bit is 1.
    4. Apply the DGIM algorithm constraints by checking and merging buckets if necessary.
    """

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


# Function to check and merge buckets to maintain DGIM algorithm's constraints
def check_and_merge(buckets):
    """
    Check and merge buckets to maintain the DGIM algorithm's constraints.

    Arguments:
    buckets (list): List of tuples representing buckets, each containing a timestamp and count.

    Returns:
    list: Updated list of buckets after merging if necessary.

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


# Function to merge the two oldest buckets among the three that have the same size
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

    # Obtain the buckets to merge
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


# Function to update state and yield output DataFrame
def update_buckets(stream_id, df_iter, state):
    """
    Update the state and yield output DataFrame for each stream.

    Arguments:
    stream_id (str): Identifier of the stream.
    df_iter (iterable): Iterator over partitions of the DataFrame for the stream.
    state (GroupState): State object for maintaining the state across batches.

    Returns:
    pd.DataFrame: DataFrame containing stream_id, count of ones, and bucket structure.

    Steps:
    1. Define the hyperparameter window size (N).
    2. Create the state if it does not exist, initializing buckets and count of ones.
    3. Process each partition of the DataFrame.
    4. Extract stream_id and bit value from the row and update the DGIM algorithm with the incoming bit.
    5. Increase the timestamp of the real bits count.
    6. Update the real count by adding the current timestep (0) if the bit is 1.
    7. Update the state with the updated buckets and count of ones.
    8. Yield DataFrame with stream_id, count of ones, and buckets structure.
    """
     
    # Define the hyperparameter window size
    N = 100

    # Create the state if it does not exist
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
        buckets = new_bit(buckets, bit, N)
        # Increase the timestamp of the real bits count
        count_ones = [time+1 for time in count_ones]
        # Update the real count by adding the current timestep (0)
        if bit == 1:
            count_ones.append(0)

    # Update the state
    state.update((buckets,count_ones))

    # Yield dataframe with the stream_id, the timestamp of the ones, and the buckets structure
    yield pd.DataFrame({"stream_id": [str(stream_id)], "count_ones": [count_ones], "buckets": [buckets]})



if __name__ == "__main__":

    # Define the bucket schema
    bucket_schema = StructType([
        StructField("timestamp", LongType()),
        StructField("size", IntegerType())
    ])

    # Define the state schema
    state_schema = StructType([
        StructField("buckets", ArrayType(bucket_schema)),
        StructField('count_ones', ArrayType(LongType())),
    ])

    # Define the output schema
    output_schema = StructType([
        StructField("stream_id", StringType()),             
        StructField('count_ones', ArrayType(LongType())),
        StructField("buckets", ArrayType(bucket_schema)),
    ])

    # Initialize Spark session
    spark = SparkSession.builder \
                         .appName("DGIMStreaming") \
                          .getOrCreate()
    
    # Set spark configurations
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.sparkContext.setLogLevel("ERROR")

    # Read streaming data from socket
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

    # Start the query
        # Compute the real count of ones in the stream after adjusting for the value of k
        # Filter and transform the buckets to adjust timestamps and sizes, keeping only those within k
        # Calculate the total size of buckets within k
        # Compute the size of the last bucket (if any) within the k
        # Estimate the count of ones using DGIM algorithm, considering the last bucket size and rounding to nearest integer (count-last_bucket+last_bucket/2)
        # Calculate the squared difference between the estimated count and the real count
        # Select necessary columns for output
        # Start the streaming write process, appending data to the console

    query = updated_data.withColumn('real_count', F.expr("size(filter(transform(count_ones, x -> x - 25), x -> x < 0))")) \
                         .withColumn('buckets', F.expr("filter(transform(buckets, x -> named_struct('timestamp', x.timestamp - 25, 'size', x.size)), y -> y.timestamp < 0)")) \
                          .withColumn('total_size', F.expr("aggregate(buckets, 0, (acc, x) -> acc + x.size)")) \
                           .withColumn('last_bucket', F.expr("if(size(buckets) > 0, element_at(buckets, 1).size, 0)")) \
                            .withColumn('estimate_count', F.col('total_size') - F.col('last_bucket') + F.expr('ROUND(last_bucket / 2)')) \
                             .withColumn('squared_diff', (F.col('estimate_count') - F.col('real_count')) ** 2) \
                              .select('stream_id', 'real_count', 'estimate_count', 'squared_diff') \
                               .writeStream \
                                .outputMode("append") \
                                 .format("console") \
                                  .option("truncate", "false") \
                                   .start()

    # Await termination
    query.awaitTermination()