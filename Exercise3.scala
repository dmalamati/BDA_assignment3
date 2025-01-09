import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Exercise3 {
  def main(args: Array[String]): Unit = {
    // Create  SparkSession
    val spark = SparkSession
      .builder
      .appName("Assignment3")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val movies_df = spark.read.option("header", "true").csv("src/inputs/movies.csv")

    // Create new df with year column
    val year_regex = "\\((\\d{4})\\)" // Regex that finds the year in the title knowing its in brackets
    // Create new df with year column
    val new_movies_df = movies_df.withColumn("year", regexp_extract($"title", year_regex, 1))

//    println("\nMovies DataFrame with year column:")
//    new_movies_df.show(truncate = false)


    // 1. Find number of movies per genre
    val genres_df = new_movies_df
      .flatMap(row => { // For each row in the new_movies_df
        val genres = row.getAs[String]("genres").split("\\|") // Get the movie's genres
        genres.map(genre => (genre, 1)) // For each genre keep the tuple (genre, 1)
      })
      .toDF("genre", "count") // Turn the tuples into a dataframe with columns: genre, count (else it will be an RDD)
      .groupBy("genre") // Group by the genre column
      .sum("count") // Count how many movies belong to each genre
      .orderBy("genre") // Sort alphabetically by genre

    println("\nMovies per genre:")
    genres_df.show(truncate = false)
    // Save result in text file
    genres_df.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("outputs/output3_1")


    // 2. Find number of movies per year
    val year_df = new_movies_df
      .groupBy("year") // Group by the year column
      .count() // Count how many movies correspond to each year
      .orderBy($"count".desc) // Sort by the count

    println("\nTop 10 number of movies per year:")
    year_df.show(10, truncate = false) // Prints only top 10
    // Save result in text file
    year_df.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("outputs/output3_2")


    // 3. Find words that appear at least 10 times in titles
    val word_df = new_movies_df
      .flatMap(row => { // For each row in the new_movies_df
        val title = row.getAs[String]("title") // Get the movie's title string
        title.split("\\s+") // Split the title into words
          .map(word => word.toLowerCase.replaceAll("[^a-z]", "")) // Turn all word to lower case and remove punctuation and numbers
          .filter(word => word.length >= 4) // Filter out words with less than 4 letters
          .map(word => (word, 1)) // For each word keep the tuple (word, 1)
      })
      .toDF("word", "count") // Turn the tuples into a dataframe with columns: word, count (else it will be an RDD)
      .groupBy("word") // Group by the word column
      .sum("count") // Count how many times its word exist
      .filter($"sum(count)" >= 10) // Filter out the word that appear less than 10 times
      .orderBy($"sum(count)".desc) // Sort by the number of appearances

    println("\nWords that appear at least 10 times in titles:")
    word_df.show(truncate = false)
    // Save result in text file
    word_df.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("outputs/output3_3")

    spark.stop()
  }
}
