import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.functions._

object Exercise2 {
  def main(args: Array[String]): Unit = {
    val logFile = args(0)
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()

    // First query
    // Step 1: Read the data
    val validSentiments = Seq("positive", "neutral", "negative")
    val logData = spark.read
      .option("header", "true") // Use the first row as column names
      .csv(logFile)
      .filter(col("airline_sentiment").isin(validSentiments: _*))
      .cache()

    // Step 2: Split `text` into words
    val takeWords = logData
      .select(col("airline_sentiment"), explode(split(col("text"), "\\s+")).as("word"))
      .filter(col("word").rlike("^[A-Za-z]+$")) // Filter out non-alphabetical "words"
      .withColumn("word", lower(col("word")))    // Convert the word to lowercase

    // Step 3: Group by sentiment and word, then count
    val wordCounts = takeWords
      .groupBy("airline_sentiment", "word")
      .count()

    // Step 4: Rank words and filter the top 5 for each sentiment
    val windowSpec = Window.partitionBy("airline_sentiment").orderBy(desc("count"))

    val topWordsPerSentiment = wordCounts
      .withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") <= 5)
      .drop("rank")

    topWordsPerSentiment.show(false)


    // Second query
    // Step 1: filter column negativereason_confidence above 0.5
    val filteredData = logData
      .filter(col("negativereason_confidence")  > 0.5)

    // Step 2: group by airline and negativereason, then count
    val negativeReasonCount =  filteredData
      .groupBy("airline", "negativereason")
      .count()

    // Step 3: Rank negativereason and then filter the top 1 per airline
    val windowSpec1 = Window.partitionBy("airline").orderBy(desc("count"))

    // Step 4:
    val topNegativeReason = negativeReasonCount
      .withColumn("count", rank().over(windowSpec1))
      .filter(col("count") <= 1)
      .drop("count")

    topNegativeReason.show(false)

    spark.stop()
  }
}
