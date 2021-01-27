val df = spark.read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./input.csv")



  val r = df.groupBy("key").pivot("date").
    agg(first($"val1").as("_v1"), first($"val2").as("_v2")).
    orderBy("key")
  r.show()
