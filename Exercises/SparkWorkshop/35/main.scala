import org.apache.spark.sql.expressions._

val df = spark.read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./input.csv")



val w = Window.partitionBy("department").orderBy("running_total")


val res = df.withColumn("diff", $"running_total" - lag($"running_total", 1, 0).over(w))
res.show()
