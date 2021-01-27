val data = Seq(
  (None, 0),
  (None, 1),
  (Some(2), 0),
  (None, 1),
  (Some(4), 1)).toDF("id", "group")


data.groupBy("group").agg(
  first($"id", true)).show()
