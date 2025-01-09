import org.apache.spark.{SparkConf, SparkContext}

object Exercise1 {
  def main(args: Array[String]): Unit = {
    // Create SparkContext
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Assignment1")
    val sc = new SparkContext(sparkConf)

    val inputFile = "src/inputs/SherlockHolmes.txt"
    val outputDir = "outputs/output1"
    val txtFile = sc.textFile(inputFile)

    val results = txtFile.flatMap(line => line.split(" ")) // Split text to lines
      .map(word => word.replaceAll("[\\p{Punct}]", "")) // Remove punctuation marks
      .map(word => word.toLowerCase) // Turn all letters to lower case
      .filter(word => word.matches("[a-z].*")) // Filter out the words that don't start with letters
      .map(word => (word.charAt(0), word.length)) // Create a tuple for each word: (word's first letter, word's length)
      .groupByKey() // Group the tuples based on the key (aka the word's first letter)
      .mapValues(lengths => lengths.sum.toDouble / lengths.size) // For each group calculate the length average
      .sortBy(-_._2) // Sort the tuples by the length average in descending order

    // Print all results
    results.collect().foreach(println)

    // Save the output as text
    results.map(_.toString().replace("(","").replace(")", "")).saveAsTextFile("outputs/output1")

    sc.stop()
  }
}

