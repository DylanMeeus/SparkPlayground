println("messing around with the time series data")

val df = spark.read.
  option("inferSchema", "true").
  option("header", "true").
  csv("./data/time_series/time_series_covid19_deaths_global.csv")

val fdf = df.drop($"Lat").drop($"Long")
fdf.cache()

val country = "Belgium"

val filtered = fdf.where($"Country/Region" === country)

var previous: Integer = 0
val identifiedCols = Seq("Province/State", "Country/Region")

var id = -1
val names = fdf.schema.names.map(n => {
  id += 1
  id.toString
  }
)

val rids = spark.range(3,names.length).map(i => i.toString)

val renamedDF = filtered.toDF(names:_*)
println(rids)

