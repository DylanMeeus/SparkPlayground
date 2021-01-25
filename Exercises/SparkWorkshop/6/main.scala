val input = Seq(
  ("100","John", Some(35),None),
  ("100","John", None,Some("Georgia")),
  ("101","Mike", Some(25),None),
  ("101","Mike", None,Some("New York")),
  ("103","Mary", Some(22),None),
  ("103","Mary", None,Some("Texas")),
  ("104","Smith", Some(25),None),
  ("105","Jake", None,Some("Florida"))).toDF("id", "name", "age", "city")


// merge rows so that the 'null' values are filled in.

// create separate dataframes of the pieces we want
val cities = input.select($"id", $"city").na.drop()
val ages = input.select($"id", $"age").na.drop()
val names = input.select($"id", $"name")

// perform some joins
val joined = cities.join(ages, Seq("id"), joinType="outer").
  join(names, Seq("id"), joinType="inner").
  select($"id", $"name", $"age", $"city").distinct().
  orderBy("id").
  show()
