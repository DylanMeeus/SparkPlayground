
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)
val flightsDF = spark.read.parquet("../data/flight-data/parquet/2010-summary.parquet")

val flights = flightsDF.as[Flight]
flights.filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .take(5)


  

