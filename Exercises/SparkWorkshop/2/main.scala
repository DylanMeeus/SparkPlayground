import org.apache.spark.sql.expressions.Window

val input = Seq(
  (1, "MV1"),
  (1, "MV2"),
  (2, "VPV"),
  (2, "Others")).toDF("id", "value")

// show these in order of importance 
// (for each ID, select first entry)
input.groupBy($"id").agg(first("value").alias("first")).show()
