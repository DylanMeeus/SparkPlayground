val df = spark.read.
      option("inferSchema", "true").
      option("header", "true").
      csv("./input.csv")


df.rollup("department").
  agg(sum($"salary"), avg($"salary")).show()
