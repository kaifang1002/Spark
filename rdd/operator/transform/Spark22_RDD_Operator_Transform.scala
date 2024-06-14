package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-key-value
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2)))

    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6), ("c", 7)))

    val cgRDD = rdd1.cogroup(rdd2)

    cgRDD.collect().foreach(println)

    sc.stop()
  }

}
