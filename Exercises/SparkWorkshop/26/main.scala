val nums = spark.range(5).
  withColumn("group", $"id" % 2)


val res = nums.groupBy("group").
  agg(min($"id").as("minid"), max($"id").as("maxid"))

res.select($"group", $"maxid", $"minid").show()
