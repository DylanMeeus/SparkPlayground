import org.apache.spark.sql.expressions._

val df = spark.read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./input.csv")

  
val w = Window.orderBy("Salary")

val res = df.withColumn("%", percent_rank over w).
withColumn("%", when(round($"%",2) > 0.60, "high").
    when($"%" < 0.50, "low").
    otherwise("average"))

res.show()
