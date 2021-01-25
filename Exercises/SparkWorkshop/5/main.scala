case class MovieRatings(movieName: String, rating: Double)
case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])

val movies_critics = Seq(
  MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
  MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))))

val ratings = movies_critics.toDF


val exploded = ratings.select($"name", explode($"movieRatings"))


exploded.groupBy("name").
      pivot("col.movieName").
      sum("col.rating").
      orderBy(desc("name")).
      show()
