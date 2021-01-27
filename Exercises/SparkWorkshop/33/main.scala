val df = spark.
  read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./input.csv")

// find the difference in pay between the highest paid employee in a department and the other
// employees

val maxPaid = df.groupBy("department").max("salary")
maxPaid.createOrReplaceTempView("maxes")

val result = df.join(maxPaid, "department").
  withColumn("diff", $"max(salary)" - $"salary").
  drop("max(salary)")

result.orderBy("department").show()

