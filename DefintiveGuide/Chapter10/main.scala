spark.read.json("../data/flight-data/json/2015-summary.json").createOrReplaceTempView("some_view")

val result = spark.sql("""
  select DEST_COUNTRY_NAME, sum(count) from some_view group by DEST_COUNTRY_NAME"""
  ).where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10").count()

result

