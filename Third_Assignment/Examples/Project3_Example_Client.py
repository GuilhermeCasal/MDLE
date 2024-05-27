from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, window

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WordCountStreaming") \
    .getOrCreate()

# Read streaming data from socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into words
words = lines.select(
    explode(
        split(lines.value, "\n")
    ).alias("word")
)

# Filter out empty words
words = words.filter(col("word") != "")

# Count the occurrences of each word
word_counts = words.groupBy("word").count()

# Start running the query that prints the word counts to the console
query = word_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()