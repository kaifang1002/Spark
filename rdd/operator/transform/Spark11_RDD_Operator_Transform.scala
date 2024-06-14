package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartition
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //扩大分区 需要shuffle 打乱重新组合
    //val newRDD = rdd.coalesce(3,true)
    //扩大分区:repartition,底层代码调用的就是coalesce，并且已经采用了shuffle
    val newRDD = rdd.repartition(3)

    newRDD.saveAsTextFile("output")

    sc.stop()
  }

}
