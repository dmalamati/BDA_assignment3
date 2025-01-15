import org.apache.spark.{SparkConf, SparkContext}

object Exercise4 {
  def main(args: Array[String]): Unit = {
    // define the spark context
    val conf = new SparkConf()
      .setAppName("LocalFileRDDExample")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // localRDD is an RDD from a file in the local file system
    val localRDD = sc.textFile(args(0))
    val filteredFromComments = localRDD.filter(line => !line.startsWith("#"))

    // First query, calculate out and in degree for every node
    val outDegree = filteredFromComments
      .map(line => line.split("\\s+")(0))
    val topOutDegreeNode = outDegree
      .countByValue()
      .toSeq.sortBy(-_._2)

    val InDegree = filteredFromComments
      .map(line => line.split("\\s+")(1))
    val topInDegreeNode = InDegree
      .countByValue()
      .toSeq.sortBy(-_._2)

    // Print the results
    print("Top out degree nodes\n")
    topOutDegreeNode.take(10).foreach { case (value, count) =>
      println(s"Node: $value, Out degree: $count")
    }
    print("\nTop in degree nodes\n")
    topInDegreeNode.take(10).foreach { case (value, count) =>
      println(s"Node: $value, In degree: $count")
    }


    // Second Query
    // Filter edges where the reverse pair exists
    val edges = filteredFromComments.map { line =>
    val nodes = line.split("\\s+")
    // Normalize the edge by ensuring the smaller node comes first
    val node1 = nodes(0).toInt
    val node2 = nodes(1).toInt
    if (node1 < node2) (node1, node2) else (node2, node1)
}
    val uniqueEdges = edges.distinct()

    // Extract (node, degree) pairs
    val degrees = uniqueEdges
      .flatMap { case (node1, node2) =>
      Seq(node1, node2)
    }
      .countByValue()

    // Find mean
    val sumDegrees = degrees.values.sum
    val numberOfNodes = degrees.size
    val meanDegree = sumDegrees/numberOfNodes

    // Filter nodes above mean
    val nodesAboveMean = degrees
      .filter{case (node, value) => value > meanDegree}

//    print("\nNodes with degrees above mean degree \n")
//    nodesAboveMean.take(10).foreach { case (value, count) =>
//          println(s"Node: $value, In degree: $count")
//        }

    val nodesAboveMeanRDD = sc.parallelize(nodesAboveMean.toSeq)
    // Save the RDD to a text file
    nodesAboveMeanRDD.saveAsTextFile("nodesAboveMean.txt")

  }
}
