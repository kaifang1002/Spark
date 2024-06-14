package rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxxxxx"),
      ("cba", "xxxxxxxxxx"),
      ("wnba", "xxxxxxxxxx"),
      ("nba", "xxxxxxxxxx")
    ), 3)

    val partRDD = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("output")

    sc.stop()
  }

  class MyPartitioner extends Partitioner {
    // Number of partitions
    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }
}
