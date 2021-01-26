import org.apache.spark.sql.function._

val df = Seq("a","b","c").toDF("letters")

// create and register a UDF
def myUpper(s: String): String = {
  return s.toUpperCase()
}
spark.udf.register("my_upper", myUpper(_:String):String)

val res = df.select($"letters").
  withColumn("uppercase", expr("my_upper(letters)"))
