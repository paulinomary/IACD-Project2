# Task 7 - Get the least popular superhero
# Spark SQL

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, desc

spark = SparkSession.builder.appName("leastPopularSuperheroSQL").getOrCreate()

movies = spark.read.text("./Marvel+Graph")
movies = movies.select(explode(split(movies.value, " ")).alias("hero")).filter(col("hero") != "")

heroCoOccurrences = movies.groupBy("hero").count()

heroNames = spark.read.text("./Marvel+Names")
heroNames = heroNames.select(split(heroNames.value, " ", 2).alias("heroInfo"))
heroNames = heroNames.withColumn("heroID", heroNames.heroInfo.getItem(0).cast("int"))
heroNames = heroNames.withColumn("heroName", heroNames.heroInfo.getItem(1))

leastPopular = heroCoOccurrences.join(heroNames, heroCoOccurrences.hero == heroNames.heroID)

leastPopularSuperhero = leastPopular.select("heroName", "count").orderBy("count", ascending = True).limit(1)
leastPopularSuperhero.show()