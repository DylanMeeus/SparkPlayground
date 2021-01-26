val df = spark.
  read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./input.csv")

df.select($"id", $"country", $"city").withColumn("uppercased", upper($"city")).show()
