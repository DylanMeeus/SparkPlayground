val nums = spark.range(5).
  withColumn("group", $"id" % 2)

nums.groupBy("group").agg(collect_list($"id").as("ids")).show()
