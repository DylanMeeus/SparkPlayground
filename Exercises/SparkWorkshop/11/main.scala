// create as many rows as there are numbers in the sequence
val nums = Seq(Seq(1,2,3)).toDF("n")

// crossjoin - bad for performance but hey, it works!
var result = nums.select(explode($"n")).crossJoin(nums).show()
