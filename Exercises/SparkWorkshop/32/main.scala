import org.apache.spark.sql.expressions.Window

val df = spark.read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./input.csv")


val w = Window.partitionBy("genre").orderBy(desc("quantity"))

val ranked = df.withColumn("qrank", rank over w)

ranked.select($"id", $"title", $"genre", $"quantity", $"qrank").
  where($"qrank" < 3). // higher rank -> higher quantity
  orderBy("genre", "qrank").show()

