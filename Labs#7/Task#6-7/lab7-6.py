from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder.appName("HeroAppearanceCount").getOrCreate()

movies_rdd = spark.read.text("Marvel+Graph").rdd.flatMap(lambda x: x)
names_rdd = spark.read.text("Marvel+Names").rdd.flatMap(lambda x: x)

heroCount = movies_rdd \
    .flatMap(lambda line: line.split(' ')) \
    .filter(lambda hero_id: hero_id != '') \
    .map(lambda hero_id: (hero_id, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)

mapNames = names_rdd \
    .map(lambda line: line.split(' ', 1)) \
    .map(lambda parts: (parts[0], parts[1]))


id_most = heroCount.first()[0]
name_most = mapNames.filter(lambda x: x[0] == id_most).first()[1]

print(f"\n\nMost Popular: {name_most}\n\n")

spark.stop()