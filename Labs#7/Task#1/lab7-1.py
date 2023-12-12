#Task 1 - Minimum Temperature per Capital
# Spark RDDs

# Import SparkContext and SparkConf
from pyspark import SparkContext, SparkConf

# Create SparkConf object
conf = SparkConf().setAppName("lab7-1").setMaster("local")

# Create SparkContext object
sc = SparkContext(conf=conf)

# Read the file
lines = sc.textFile("./1800.csv")

# Parse Lines
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    date = fields[1]
    entryType = fields[2]
    temperature = float(fields[3])
    return (stationID, entryType, temperature)

# Filter out all but TMIN entries
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

# Convert to (stationID, temperature)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

# Find minimum temperature per station
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

# Collect results
results = minTemps.collect()

# Print results
for result in results:
    print(result)
