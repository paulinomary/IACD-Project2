#Task 6 - Find the most popular superhero
#Spark RDDs

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MostPopularSuperhero")
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

mostPopular = flipped.max()
mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")

