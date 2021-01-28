val df = spark.read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./input.csv")

val group = df.groupBy("cc").pivot("udate").
  agg(first($"cc")).drop("cc")
