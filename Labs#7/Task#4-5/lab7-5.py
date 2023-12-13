#Task #5 - Obtain the sorted amount spent by customer
#Spark RDDs

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("TotalAmountSpent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amountSpent = float(fields[2])
    return (customerID, amountSpent)

lines = sc.textFile("./customer-orders.csv")
rdd = lines.map(parseLine)

totalAmountSpent = rdd.reduceByKey(lambda x, y: x + y)
totalAmountSpentSorted = totalAmountSpent.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

results = totalAmountSpentSorted.collect()

for result in results:
    print(result)
