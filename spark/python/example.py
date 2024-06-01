from pyspark.sql import SparkSession

# Create a SparkSession
spark: SparkSession = SparkSession.builder.appName("demo")\
    .config("spark.ui.port", "4050")\
    .getOrCreate()

df = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", 13),
    ],
    ["first_name", "age"],
)

df.show()

from pyspark.sql.functions import col, when

df1 = df.withColumn(
    "life_stage",
    when(col("age") < 13, "child")
    .when(col("age").between(13, 19), "teenager")
    .otherwise("adult"),
)

df1.show()

print("api")
df1.groupBy("life_stage").agg({"age": "mean"}).show()
df1.write.saveAsTable("people")
# or with SQL
print("sql")
spark.sql("SELECT life_stage, AVG(age) FROM people GROUP BY life_stage").show()
while True:
    pass