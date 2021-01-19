import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, to_date}

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("../data/retail-data/all/*.csv").coalesce(5)
df.cache() // for faster access
df.createOrReplaceTempView("dfTable")

// aggregation functions
df.count()
df.select(countDistinct("StockCode")).show()
df.select(approx_count_distinct("StockCode", 0.1)).show()


df.select(countDistinct("Quantity").alias("c"), sumDistinct("Quantity").alias("s")).selectExpr("c / s").show()
df.select(count("Quantity").alias("c"), sum("Quantity").alias("s")).selectExpr("c / s").show()

// grouping
// vanilla groupBy
df.groupBy("InvoiceNo", "CustomerId").count().show()

// groupBy with expressions
df.groupBy("InvoiceNo").agg(expr("count(Quantity)")).show()

// groupBy with maps
df.groupBy("InvoiceNo").agg("Quantity" -> "count", "Quantity" -> "stddev_pop").show()

// Window functions
val dfWithDate = df.withColumn("date", to_date($"InvoiceDate", "MM/dd/yyyy HH:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")


val windowSpec = Window.partitionBy("CustomerId", "date").
                orderBy($"Quantity".desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)

val maxPurchaseQuantity = max($"Quantity").over(windowSpec)
val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)

dfWithDate.where("CustomerId is not null").orderBy("CustomerId").select($"CustomerId", $"date", $"Quantity",
      purchaseRank.alias("quantityRank"), purchaseDenseRank.alias("quantityDense"), maxPurchaseQuantity.alias("max")).show()


// Grouping Sets
// This is mostly just in SQL ;-)

// Rollups
val dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity")).selectExpr("Date", "Country", "`sum(Quantity)` as total_quan").orderBy("date")
rolledUpDF.show()

val cube = dfNoNull.cube("Date", "Country").agg(sum("Quantity")).selectExpr("Date", "Country", "`sum(Quantity)` as total_quan").orderBy("date")
cube.show()
