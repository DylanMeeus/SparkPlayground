// Turn the columns into a DF with columns as indexes and the character where they appear
val input = Seq(
  Seq("a","b","c"),
  Seq("X","Y","Z")).toDF()


val t = input.select($"value", explode($"value").as("c")).
  withColumn("value", concat_ws("", $"value"))

// split into "KV" pairs essentially
val r = t.map(r => {
  val word = r.getString(0)
  val arr = r.getString(1)
  (word.indexOf(arr), arr, word)
}).toDF("index","character", "word")

// group, pivot, select and format
r.groupBy("word").pivot("index").agg(first($"character")).orderBy(desc("word")).drop("word").show()
