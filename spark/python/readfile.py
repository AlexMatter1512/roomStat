from pyspark import SparkContext

# Create a SparkContext
sc = SparkContext("local", "ReadFileExample")

# Read the file
lines = sc.textFile("file.txt")

# Process the lines as needed
# For example, you can print each line
lines.foreach(print)

# Stop the SparkContext
sc.stop()