############################################################################################################################################
#
# EDW (Exponentially Decaying Window) Algorithm - PySpark Streaming Implementation
#
############################################################################################################################################


# EDW Algorithm Hyperparameters:
# - c: constant for decaying importance of items
# - threshold: value bellow which to discard counts
# - num_streams: number of streams (defined in the server)
# - prob: one output probability (defined in the server)

# By defining this hyperparameters the outcomes of the algorithm are changed


# Import the necessary libraries to run the code
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pandas as pd

# Function to update the state for each stream_id
def update_buckets(stream_id, df_iter, state):
    """
    Update the state for each stream_id based on incoming bits and yield the estimated popularity.

    Arguments:
    stream_id (str): Identifier of the stream.
    df_iter (iterable): Iterator over partitions of the DataFrame for the stream.
    state (GroupState): State object for maintaining the state across batches.

    Returns:
    pd.DataFrame: DataFrame containing stream_id and estimated popularity.

    Steps:
    1. Define the decay factor (c) to adjust the influence of older observations.
    2. Initialize or retrieve the existing state, representing the estimated popularity.
    3. Process each partition of the DataFrame.
    4. Extract bit value from the row and update the estimated popularity using the decay factor.
    5. Remove non-popular items based on a threshold (0.5) to avoid noise.
    6. Update the state with the updated estimated popularity.
    7. Yield empty DataFrame (no output needed for each batch).
    """

    # Define decay factor
    c = 1e-6

    # Initialize or retrieve the existing state
    if not state.exists:
        estimate_pop = 0.0
        state.update((estimate_pop,))
    else:
        estimate_pop = state.get[0]

    # Process each partition of the DataFrame
    for df in df_iter:
        # Extract bit value from the row
        bit = df["value"].iloc[1]
        # Update the estimated popularity with decay factor
        estimate_pop *= (1-c) 
        if bit == 1 :
            estimate_pop += 1

    # Remove non-popular items (based on a threshold)
    estimate_pop = 0.0 if estimate_pop < 0.5 else estimate_pop

    # Update the state
    state.update((estimate_pop,))

    # Yield empty DataFrame (no output needed for each batch)
    yield pd.DataFrame({"stream_id": [str(stream_id)], "estimated_pop": [estimate_pop]})


if __name__ == "__main__":
    
    # Define the output schema with fields for counts
    output_schema = StructType([
        StructField("stream_id", StringType()),             
        StructField("estimated_pop", FloatType()),
    ])

    # Define the state schema
    state_schema = StructType([
        StructField("estimated_pop", FloatType()),
    ])

    # Initialize Spark session
    spark = SparkSession.builder \
                         .appName("EDWStreaming") \
                          .getOrCreate()
    
    # Set Spark configurations
    spark.conf.set("spark.sql.shuffle.partitions", "1")
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
            
    # Update state for each group using applyInPandasWithState
    updated_data = data.groupby("stream_id") \
                        .applyInPandasWithState(
                            update_buckets,
                            outputStructType=output_schema,
                            stateStructType=state_schema,
                            outputMode="append",
                            timeoutConf=GroupStateTimeout.NoTimeout
                        )

    # Start the query
        # Group the data by stream_id
        # Aggregate by taking the maximum estimated popularity as "estimated_pop"
        # Order the results by estimated popularity in descending order
        # Limit the output to the top 5 most popular items
        # Start the streaming write process, appending data to the console

    query = updated_data.groupBy("stream_id") \
                         .agg(F.max("estimated_pop").alias("estimated_pop")) \
                          .orderBy(F.col("estimated_pop").desc()) \
                           .limit(5) \
                            .writeStream \
                             .outputMode("complete") \
                              .format("console") \
                               .option("truncate", "false") \
                                .start()

    # Await termination        
    query.awaitTermination()
