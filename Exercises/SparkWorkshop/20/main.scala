import org.apache.spark.sql.types._

val schema = StructType(Array(
  StructField("timestamp", StringType, true),
  StructField("IP", StringType, true)
))

val df = spark.read.
  option("delimiter", "|").
  schema(schema).
  csv("./input.csv")

val  res = df.withColumn(
  "timestamp", to_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss,SSS"))

