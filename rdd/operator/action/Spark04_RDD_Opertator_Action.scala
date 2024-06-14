package rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Opertator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO -行动算子
    //countByValue
    //val rdd = sc.makeRDD(List(1,1,3,4),2)
    //val result = rdd.countByValue()
    //println(result)

    //countByKey
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3)))
    val result = rdd.countByKey()
    println(result)

    sc.stop()
  }

}
