val data = Seq(
  (0, "A", 223, "201603", "PORT"),
  (0, "A", 22, "201602", "PORT"),
  (0, "A", 422, "201601", "DOCK"),
  (1, "B", 3213, "201602", "DOCK"),
  (1, "B", 3213, "201601", "PORT"),
  (2, "C", 2321, "201601", "DOCK")
).toDF("id","type", "cost", "date", "ship")



// calculate cost average per type per date
data.groupBy("type").
    pivot("date").
    agg(avg($"cost")).
    orderBy("type").
    show()


  data.groupBy("type").
    pivot("date").
    agg(collect_set($"ship")).
    orderBy("type").
    show()

