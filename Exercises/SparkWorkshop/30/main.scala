
val df = spark.read.
  option("inferSchema", "true").
  option("header",  "true").
  csv("./input.csv")



  // group the first part
val g = df.groupBy("ParticipantID", "Assessment").
  agg(first($"GeoTag"))


  // group the second part
  var o = df.groupBy("ParticipantID").pivot("Qid").
    agg(first($"AnswerText"))

  val schemaNames = o.schema.names.map(n => {
    var result = n
    if (n != "ParticipantID") {
      result = "Qid_" + n
    }
    result
  })

  o = o.toDF(schemaNames:_*)

  g.join(o, "ParticipantID").orderBy("ParticipantID").show()
