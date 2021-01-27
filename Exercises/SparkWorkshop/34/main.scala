import org.apache.spark.sql.expressions.Window
val df = spark.read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./input.csv")


val w = Window.partitionBy("department").orderBy("items_sold").
  rowsBetween(Window.unboundedPreceding, Window.currentRow)

val res = df.withColumn("cum_sum", sum($"items_sold").over(w))


