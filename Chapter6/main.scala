import org.apache.spark.sql.functions.{expr, col, pow, translate}
val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("../data/retail-data/by-day/2010-12-01.csv")

// some filtering
df.printSchema()
df.where($"InvoiceNo".equalTo(536365)).select("InvoiceNo", "Description").show(5, false)

// rounding and functions on fields
val x = pow($"Quantity" * $"UnitPrice", 2) + 5
df.select(expr("CustomerId"), round(x.alias("realQuantity"), 1)).show(2)

// pearson correlation
df.stat.corr("Quantity", "UnitPrice")

// view summary statistics
df.describe().show()

// cross-tabulation
df.stat.crosstab("StockCode", "Quantity").show()

// regex!
val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|")

df.select(regexp_replace($"Description", regexString, "_").alias("color_clean"), $"Description").show(2)

// translate
df.select(translate($"Description", "LEET", "1337"), $"Description").show(2)

// checking existance of a string (contains)
val containsBlack = $"Description".contains("BLACK")
df.withColumn("hasSimpleColor", containsBlack).where("hasSimpleColor").select("Description").show(3, false)

// create columns dynamically
val selectedColumns = simpleColors.map(c => {
    $"Description".contains(c.toUpperCase).alias(s"is_$c")
}):+expr("*")

df.select(selectedColumns:_*).where($"is_white").select("Description").show(3)
