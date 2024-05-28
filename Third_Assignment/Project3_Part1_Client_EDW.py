############################################################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import pandas as pd
import numpy as np

def update_buckets(stream_id, df_iter, state):

    c = 10e-6

    if not state.exists:
        estimate_pop = 0
        state.update((estimate_pop, ))
    else:
        # Retrieve the existing state
        estimate_pop = state.get[0]

    # Process each partition of the DataFrame
    for df in df_iter:
        # Extract stream_id and bit value from the row
        bit = df["value"].iloc[1]
        # Update the DGIM algorithm with the incoming bit
        estimate_pop *= (1-c) 
        # Update the real count
        if bit == 1:
            estimate_pop += 1

    # Update the state
    state.update((estimate_pop, ))

    # Yield empty DataFrame (no output needed for each batch)
    yield pd.DataFrame({"stream_id": [str(stream_id)], "estimated_count": [estimate_pop]})


if __name__ == "__main__":
    # # Define parameters
    # N = 1000  # Specify the size of the sliding window
    # k = 50  # Specify the time window for counting 1s

    # Define the output schema with fields for counts
    output_schema = StructType([
        StructField("stream_id", StringType()),             
        StructField("estimated_count", LongType()),            
        
    ])

    # Define the state schema
    state_schema = StructType([
        StructField("estimated_pop", FloatType()),
    ])

    # Initialize Spark session
    spark = SparkSession.builder \
                         .appName("EDWStreaming") \
                          .getOrCreate()

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


    # Order by count in descending order
    ordered_items = updated_data.orderBy(F.col("estimated_pop").desc())

    # Limit to the top 5 most popular items
    top5 = ordered_items.limit(5)

    # Write the stream to the console
    query = top5.writeStream \
                    .outputMode("complete") \
                    .format("console") \
                    .start()

    query.awaitTermination()
    # Await termination
        
    query.awaitTermination()
