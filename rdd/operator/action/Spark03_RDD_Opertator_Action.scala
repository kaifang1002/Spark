package rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Opertator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    //TODO -行动算子
    //aggregateByKey:初始值只会参与分区内计算
    //aggregate：初始值会参与分区内计算，并且参与分区间计算
    //val result = rdd.aggregate(10)(_ + _, _ + _)

    //fold:分区内和分区间计算规则相同时简化
    val result = rdd.fold(10)(_ + _)

    println(result)

    sc.stop()
  }

}
