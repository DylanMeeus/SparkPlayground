val input = spark.range(50).withColumn("key", $"id" % 5)

// write a query that does a limited "collect_set" (max 3 elements)

input.groupBy($"key").
  agg(collect_set($"id").as("collection")).
  withColumn("collection", slice($"collection", 1, 3)).show()
