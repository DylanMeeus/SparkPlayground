// read and format the DF
val df = spark.read.option("inferSchema", "true").
  csv("./input.csv")


val RDF = df.
  withColumnRenamed("_c0", "id").
  withColumnRenamed("_c1", "words").
  withColumnRenamed("_c2", "word")

// split on delimiter (,) and explode into column values
val s =  RDF.select($"id", explode(split($"words", ",")).as("words"), $"word")

// filter the keys ('needles') and words ('haystacks')
val keys = s.select($"word").distinct()
val words = s.select($"id", $"words")

// remove the needles that are never in the haystack
val t = words.join(keys, $"words" === $"word", joinType="left_semi")

// count the IDs where the remaining words appear
t.groupBy("words").agg(collect_list($"id")).show()

