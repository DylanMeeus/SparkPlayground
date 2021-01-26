import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

// schema, although it fails on applying due to malformatted input :-)
val schema = StructType(Array(
  StructField("name", StringType, true),
  StructField("country", StringType, true),
  StructField("population", IntegerType, true)))

val df = spark.
  read.
  option("inferSchema", "true").
  option("header", "true").
  csv("input.csv")

// this makes sure population can be parsed as a number
val formatted = df.select($"name", $"country", $"population").
  withColumn("population", regexp_replace($"population", "\\s+", "")).
  withColumn("population", $"population".cast("Int"))

val m = formatted.groupBy("country").max("population").
  withColumnRenamed("country", "gcountry").
  withColumnRenamed("max(population)", "mp")

val result = m.join(formatted, $"gcountry" === $"country" && $"mp" === $"population").
  drop("gcountry", "mp")
