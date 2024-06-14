package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-双value
    //两个数据源要求分区数量保持一致
    //两个数据源要求分区中数据数量保持一致
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4),2)
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6),4)

    val rdd7 = rdd1.zip(rdd2)
    println(rdd7.collect().mkString(","))

    sc.stop()
  }

}
