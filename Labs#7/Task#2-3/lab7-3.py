from pyspark import SparkConf, SparkContext

# Create SparkConf object
conf = SparkConf().setAppName("lab7-3").setMaster("local")

# Create SparkContext object
sc = SparkContext(conf=conf)

# Read the file
lines = sc.textFile("./Book")

# Parse Lines of the book
def parseLine(line):
    words = line.lower().split()
    return [(word, 1) for word in words if word.isalnum()]

# Get the word frequency
wordFrequency = lines.flatMap(parseLine)
wordFrequency = wordFrequency.reduceByKey(lambda x, y: x + y)

# Sort the results by count in descending order
wordFrequency = wordFrequency.sortBy(lambda x: x[1], ascending=False)

# Collect results
results = wordFrequency.collect()

# Print results
for result in results:
    print(result)

# Stop SparkContext
sc.stop()
