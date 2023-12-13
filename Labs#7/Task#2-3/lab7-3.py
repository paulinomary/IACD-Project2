from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("lab7-3").setMaster("local")

sc = SparkContext(conf=conf)

# Read the file
lines = sc.textFile("./Book")

def parseLine(line):
    words = line.lower().split()
    return [(word, 1) for word in words if word.isalnum()]

wordFrequency = lines.flatMap(parseLine)
wordFrequency = wordFrequency.reduceByKey(lambda x, y: x + y)

wordFrequency = wordFrequency.sortBy(lambda x: x[1], ascending=False)

results = wordFrequency.collect()

for result in results:
    print(result)

sc.stop()
