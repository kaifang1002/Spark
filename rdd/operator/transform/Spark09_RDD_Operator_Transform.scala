package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartition
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

    val rdd1 = rdd.distinct()
    rdd1.collect().foreach(println)

    sc.stop()
  }

}
