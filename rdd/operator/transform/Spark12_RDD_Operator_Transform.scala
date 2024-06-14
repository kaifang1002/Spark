package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartition
    val rdd = sc.makeRDD(List(6, 2, 4, 3, 5, 1), 2)

    val newRDD = rdd.sortBy(num => num)

    newRDD.saveAsTextFile("output")

    sc.stop()
  }

}
