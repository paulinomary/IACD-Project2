from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("lab7-2").setMaster("local")

sc = SparkContext(conf=conf)
lines = sc.textFile("./Book")

def parseLine(line):
    words = line.lower().split()
    return [(word, 1) for word in words if word.isalnum()]


wordFrequency = lines.flatMap(parseLine)
wordFrequency = wordFrequency.reduceByKey(lambda x, y: x + y)

results = wordFrequency.collect()

for result in results:
    print(result)

# Stop SparkContext
sc.stop()
