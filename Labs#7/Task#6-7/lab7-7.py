#Task 7 - Find the least popular superhero
#Spark RDDs

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("LeastPopularSuperhero")
sc = SparkContext(conf = conf)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

def countCoOccurences(line):
    fields = line.split()
    return (int(fields[0]), len(fields) - 1)

names = sc.textFile("./Marvel+Names")
namesRdd = names.map(parseNames)

lines = sc.textFile("./Marvel+Graph")
pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))

leastPopular = flipped.min()
leastPopularName = namesRdd.lookup(leastPopular[1])[0]

print(str(leastPopularName) + " is the least popular superhero, with " + \
    str(leastPopular[0]) + " co-appearances.")