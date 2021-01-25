import org.apache.spark.sql.functions.udf
// write a query that splits the value in the first column by the delimiter in the second column
val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")


// add some escape character because 'SPLIT' is regex based
val test = dept.select($"VALUES", $"Delimiter").
  withColumn("Delimiter", concat(lit("\\"), $"Delimiter"))

// push this to sql so we can just use queries
test.createOrReplaceTempView("dept")
spark.sql("select VALUES, Delimiter, SPLIT(VALUES, Delimiter) from dept").show(false)

