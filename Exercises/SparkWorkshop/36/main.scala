val words = Seq(Array("hello", "world")).toDF("words")


val res = words.select($"words").
  withColumn("solution", concat_ws(" ", $"words"))

res.show()

  
