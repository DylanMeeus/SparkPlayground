val nums = spark.range(5).withColumn("group", $"id" % 2)

nums.groupBy("group").agg(max("id")).show()
