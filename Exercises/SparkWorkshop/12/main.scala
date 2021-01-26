// reverse engineer the input.txt (which contains `.show()` output)
// read it to a DF -> and print the DF

val df = spark.read.option("inferSchema", "true").
  text("input.txt")

// remove comments (+--- like lines
val noComment = df.select($"value").filter(!$"value".startsWith("+--"))


// perform a manual split on the '|' character
// and store the result in a tuple of (id, words, moreWords)
val total = noComment.map(f => {
  val splitParts = f.getString(0).split("\\|")
  val id = splitParts(1)
  val words = splitParts(2)
  val moreWords = splitParts(3)
  (id, words, moreWords)
})

// now reconstruct this into a dataframe
// First, unpack the tuple to discover the headers. 
// Then convert the body to a DF with those headers.
val (a,b,c) = total.first()
val body = total.filter(r => r != header).toDF(a,b,c)
body.show()
