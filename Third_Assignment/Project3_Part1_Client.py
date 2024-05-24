
############################################################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json
import pandas as pd
from DGIM import DGIM



def update_buckets(stream_id, df_iter, state):

    if not state.exists:
        # Initialize DGIM algorithm if state does not exist
        dgim = DGIM(N=1000, k=50)
        # Serialize the initial state
        serialized_buckets = json.dumps(dgim.buckets)
        state.update((serialized_buckets, None))
    else:
        # Retrieve and deserialize the existing state
        serialized_buckets = state.get
        buckets = json.loads(serialized_buckets)
        dgim = DGIM(N=1000, k=50, buckets=buckets)

    # Process each partition of the DataFrame
    for df in df_iter:
        # Process each row in the partition
        for _, row in df.iterrows():
            # Extract stream_id and bit value from the row
            bit = row["value"]
            # Update the DGIM algorithm with the incoming bit
            dgim.new_bit(bit)

    # Update the state
    serialized_buckets = json.dumps(dgim.buckets)
    state.update((serialized_buckets, None))

    # Yield empty DataFrame (no output needed for each batch)
    yield pd.DataFrame({"stream_id": [str(stream_id)], "bit_count": [0], "estimated_count": [dgim.estimate_count()]})


if __name__ == "__main__":
    # Define parameters
    N = 1000  # Specify the size of the sliding window
    k = 50  # Specify the time window for counting 1s

    # Define the output schema with fields for counts
    output_schema = StructType([
        StructField("stream_id", StringType()),             
        StructField("bit_count", LongType()),            
        StructField("estimated_count", LongType()),         
    ])

    # Define the state schema
    state_schema = StructType([
        StructField("last_timestamp", LongType()),
        StructField("size", IntegerType())
    ])

    # Initialize Spark session
    spark = SparkSession.builder \
                         .appName("DGIMStreaming") \
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

    # Start the query
    query = updated_data.writeStream \
                         .outputMode("append") \
                          .format("console") \
                           .start()

    # Await termination
    query.awaitTermination()
