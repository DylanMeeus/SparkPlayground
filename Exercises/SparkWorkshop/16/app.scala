import org.apache.spark.sql.SparkSession



object Sparky {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TonySpark").
      getOrCreate()

    val df = spark.read.option("inferSchema", "true").
      option("header", "true").csv("./input.csv")

      
    df.show()

  }
}

