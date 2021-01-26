val data = Seq(
  (100,1,23,10),
  (100,2,45,11),
  (100,3,67,12),
  (100,4,78,13),
  (101,1,23,10),
  (101,2,45,13),
  (101,3,67,14),
  (101,4,78,15),
  (102,1,23,10),
  (102,2,45,11),
  (102,3,67,16),
  (102,4,78,18)).toDF("id", "day", "price", "units")

// we want to basically join the price and units frames (pivoted) next to eachother
// we could do this with a join, since a pivot only allows to be pivotted on a single column


// calculate the two dataframes
val pricePivot = data.groupBy("id").pivot("day").agg(first($"price"))
val unitPivot = data.groupBy("id").pivot("day").agg(first($"units"))

// rename them so the headers make sense
val renamedPrice = pricePivot.schema.names.map(n => {
  var result = n
  if (result != "id") {
    result = "price_" + n
  }
  result
})

val renamedUnit = unitPivot.schema.names.map(n => {
  var result = n
  if (result != "id") {
    result = "unit_" + n
  }
  result
})

// apply new headers + merge
val priceResult = pricePivot.toDF(renamedPrice:_*)
val unitResult = unitPivot.toDF(renamedUnit:_*)
val joined = priceResult.join(unitResult, "id")
joined.show()
