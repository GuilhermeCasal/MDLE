from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming.state import GroupStateTimeout
import pandas as pd
import sys



# Slide timestamps of existing buckets by one
# Check if the last bucket has an end time older than N
# If bit is 0 continue
# If bit is one create bucket for that bit (timestamp of 0)
# Check for bucket merging


def update_buckets(key, df_iterator, state):
    if not state.exists:
        # state is a tuple
        # set it according to stateStructType
        state.update((...,))

    current_state = state.get

    # your state update logic
            
    for df in df_iterator:
        print(df)

    state.update(current_state)

    # emit a pandas dataframe, according to outputStructType
    yield pd.DataFrame(...)



if __name__ == "__main__":
    # Define stream and socket details
    if len(sys.argv) > 1:
        NUM_STREAMS = int(sys.argv[1])

    if len(sys.argv) == 4:
        HOST, PORT = sys.argv[2], int(sys.argv[3])
    else:
        HOST, PORT = 'localhost', 9999

    spark = SparkSession.builder \
              .appName("StructuredNetworkCount") \
                .config("spark.driver.host","127.0.0.1") \
                  .config("spark.driver.memory", "2g") \
                    .config("spark.executor.memory", "2g") \
                      .getOrCreate()

    data = spark \
            .readStream.format("socket") \
              .option("host", HOST).option("port", PORT) \
                .option('includeTimestamp', 'true') \
                  .load()


    # alternatively, define input schema as StructType
    # schema = StructType([StructField(f"{i}", IntegerType()) for i in range(NUM_STREAMS)])
    data = data.select(
        #"timestamp",
        # transform json string to dict {"0": 0, "1": 0, "2": 1, ...}
        #F.from_json(data.value, "MAP<STRING,INT>").alias("bitstream"),
        # alternatively, explode dict entries to rows 
        F.explode(F.from_json(data.value, "MAP<STRING,INT>")).alias("stream_id", "value"),
    )

    # Use this to debug your data ingestion
    query = data.writeStream.foreach(print).start()
    query.awaitTermination()


    # use groupby(...).applyInPandasWithState(...)
    # to update a shared state
    # data = data.groupby(...).applyInPandasWithState(update_buckets, outputStructType="...",
    #                                 stateStructType="...", outputMode="append", timeoutConf=GroupStateTimeout.NoTimeout)

    # query = data \
    #     .writeStream \
    #         .outputMode("append") \
    #             .format("console") \
    #                 .start()

    # query.awaitTermination()




import socket
import json

def read_data_from_server(server_ip, server_port):
    # Create a socket object
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Connect to the server
    client_socket.connect((server_ip, server_port))
    
    try:
        while True:
            # Receive data from the server
            data = client_socket.recv(1024).decode("utf-8")
            
            print(data)
            # Ensure we handle the case where we receive partial data or multiple lines
            # lines = data.split("\n")
            # for line in lines:
            #     if line.strip():  # Process non-empty lines
            #         try:
            #             # Convert the JSON string back to a dictionary
            #             bits_dict = json.loads(line.strip())
            #             print(bits_dict)
            #         except json.JSONDecodeError:
            #             print("Failed to decode JSON data")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client_socket.close()

# Example usage
server_ip = "localhost"  # Replace with the server's IP address
server_port = 9999      # Replace with the server's port
read_data_from_server(server_ip, server_port)
