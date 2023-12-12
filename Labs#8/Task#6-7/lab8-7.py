# Task 7 - Get the most popular superhero
# Spark SQL

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, desc

# Step 1: Create a Spark session
spark = SparkSession.builder.appName("leastPopularSuperheroSQL").getOrCreate()

# Assuming 'Marvel+Graph' file contains lines with space-separated values like "heroID hero1 hero2 ..."
movies = spark.read.text("./Marvel+Graph")
movies = movies.select(explode(split(movies.value, " ")).alias("hero")).filter(col("hero") != "")

# Step 2: Create a DataFrame with (heroID, numberOfCoOccurrences)
heroCoOccurrences = movies.groupBy("hero").count()

# Assuming 'Marvel+Names' file contains lines with space-separated values like "heroID heroName"
heroNames = spark.read.text("./Marvel+Names")
heroNames = heroNames.select(split(heroNames.value, " ", 2).alias("heroInfo"))
heroNames = heroNames.withColumn("heroID", heroNames.heroInfo.getItem(0).cast("int"))
heroNames = heroNames.withColumn("heroName", heroNames.heroInfo.getItem(1))

# Step 3: Join the two DataFrames
leastPopular = heroCoOccurrences.join(heroNames, heroCoOccurrences.hero == heroNames.heroID)

# Step 4: Select the relevant columns and find the most popular superhero
leastPopularSuperhero = leastPopular.select("heroName", "count").orderBy("count", ascending = True).limit(1)

# Step 5: Show the result
leastPopularSuperhero.show()