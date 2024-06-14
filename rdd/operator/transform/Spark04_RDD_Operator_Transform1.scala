package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd = sc.makeRDD(List("hello spark","hello scala"))

    val flatRDD = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )

    flatRDD.collect().foreach(println)

    sc.stop()
  }

}
