val dates = Seq(
   "08/11/2015",
   "09/11/2015",
   "09/12/2015").toDF("date_string")


val df = dates.withColumn("to_date", to_date($"date_string", "dd/MM/yyyy")).
  withColumn("diff", expr("DATEDIFF(current_date(), to_date)"))

df.show()
