package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartition
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //coalesce方法默认不会打乱分区重新组合 可能会导致数据不均衡->数据倾斜

    //val newRDD = rdd.coalesce(2)

    //想要数据均衡，可以进行shuffle处理，默认是不shuffle的
    val newRDD = rdd.coalesce(2, true)

    newRDD.saveAsTextFile("output")

    sc.stop()
  }

}
