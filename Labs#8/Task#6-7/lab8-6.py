# Task 6 - Get the most popular superhero
# Spark SQL

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, desc

spark = SparkSession.builder.appName("MostPopularSuperheroSQL").getOrCreate()

movies = spark.read.text("./Marvel+Graph")
movies = movies.select(explode(split(movies.value, " ")).alias("hero")).filter(col("hero") != "")

heroCoOccurrences = movies.groupBy("hero").count()

heroNames = spark.read.text("./Marvel+Names")
heroNames = heroNames.select(split(heroNames.value, " ", 2).alias("heroInfo"))
heroNames = heroNames.withColumn("heroID", heroNames.heroInfo.getItem(0).cast("int"))
heroNames = heroNames.withColumn("heroName", heroNames.heroInfo.getItem(1))


mostPopular = heroCoOccurrences.join(heroNames, heroCoOccurrences.hero == heroNames.heroID)

mostPopularSuperhero = mostPopular.select("heroName", "count").orderBy("count", ascending = False).limit(1)
mostPopularSuperhero.show()