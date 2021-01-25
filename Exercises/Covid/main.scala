import org.apache.spark.sql.types.{IntegerType}
println("checking for parquet data")
// first we check for parquest

// if parquet is not found, try to read the CSV directory
// and then write this to parquet

// read csv with header
val df = spark.read.
    option("inferScheme", "true").
    option("header", "true").
    option("schema", "csv").
    csv("./data/*.csv")

// cache this because we will need it a lot :-)
df.cache()

case class Relevant(Last_Update: String, Country: String, Province_State: String, 
  Confirmed: Int, Deaths: Int, Recovered: Int, Active: Int)


val relevantDF = df.select($"Last_Update", $"Country_Region", $"Province_State", $"Confirmed", $"Deaths", $"Recovered", $"Active").
    withColumnRenamed("Country_Region", "Country").
    withColumn("Confirmed", $"Confirmed".cast(IntegerType)).
    withColumn("Deaths", $"Deaths".cast(IntegerType)).
    withColumn("Recovered", $"Recovered".cast(IntegerType)).
    withColumn("Active", $"Active".cast(IntegerType)).as[Relevant]

relevantDF.cache()
relevantDF.write.mode("overwrite").save("./data/parquet")

// deaths per country

println("deaths per country")
relevantDF.
  filter($"Country" === "Belgium").
  groupBy($"Country", $"Province_State").agg(sum("Confirmed")).show()

