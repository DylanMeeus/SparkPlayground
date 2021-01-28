import org.apache.spark.sql.expressions._

val df = spark.read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./input.csv")

val res = df.groupBy("id").
  agg(collect_list($"time").as("collection"))


def myReducer(input: Seq[Any]): Int = {
  val in = input.asInstanceOf[Seq[Int]]
  var tmp = List[Int]()

  var running = 0
  var previous = -1
  var max = 0
  for (i <- in) {
    if (previous == -1 || previous == i - 1) {
      running += 1
    } else {
      if (running > max) {
        max = running
      }
      running = 1
    }
    previous = i
  }
  if(running > max) {
    max = running
  }

  return max
}


spark.udf.register("my_reducer", myReducer(_:Seq[Any]):Int)


val f = res.withColumn("consecutive", expr("my_reducer(collection)")).
  drop("collection").
  orderBy("id")

f.show()
