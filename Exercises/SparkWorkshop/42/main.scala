import org.apache.spark.sql.expressions._
val input = Seq(
  (1, "Mr"),
  (1, "Mme"),
  (1, "Mr"),
  (1, null),
  (1, null),
  (1, null),
  (2, "Mr"),
  (3, null)).toDF("guest", "prefix")


// without UDF 
 val r = input.na.drop().groupBy("guest", "prefix").count().orderBy(desc("count"))

 val res = r.groupBy("guest").agg(first($"prefix").as("prefix"))
 res.createOrReplaceTempView("guests")

 // make sure all IDs are present
 val excl = input.select($"guest".as("g")).
  join(res, $"g" === $"guest", joinType="left_anti").
  withColumn("n", lit(null)).
  withColumnRenamed("g", "guest")

var fr = res.union(excl)

    

// with a UDF :-)

def mostCommon(s: Seq[Any]): String = {
  return "leet"
}

spark.udf.register("mc", mostCommon(_:Seq[Any]):String)


// collect_list skips the nulls ;-)
val udfResult = input.groupBy("guest").agg(collect_list($"prefix").as("c")).
  withColumn("prefix", expr("mc(c)"))

