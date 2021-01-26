val nums = Seq(Seq(1,2,3)).toDF("n")

val r = nums.select(explode($"n").as("num"))
r.crossJoin(nums.withColumnRenamed("n", "nums")).show()


