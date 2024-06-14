package rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Dep {
    def main(args: Array[String]): Unit = {
      val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
      val sc = new SparkContext(sparConf)

      val lines = sc.textFile("datas/2.txt")
      println(lines.toDebugString)
      println("**********")

      val words = lines.flatMap(_.split(" "))
      println(lines.toDebugString)
      println("**********")

      val wordToOne = words.map(word => (word, 1))
      println(lines.toDebugString)
      println("**********")

      val wordToSum = wordToOne.reduceByKey(_ + _)
      println(lines.toDebugString)
      println("**********")

      val array = wordToSum.collect()
      array.foreach(println)

      sc.stop()
    }
}
