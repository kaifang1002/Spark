package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-key-value groupByKey会导致数据打乱分组，存在shuffle操作
    //从shuffle角度：spark中，groupByKey shuffle操作必须落盘处理，不能在内存中等待，会导致内存溢出，所以性能较低
    //                      而redueceByKey支持分区内预聚合功能，可以有效减少shuffle落盘时的数据量，提升性能
    //从功能角度：redueceByKey包括分组和聚合，而groupByKey只有分组

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    val groupRDD = rdd.groupByKey()
    groupRDD.collect().foreach(println)

    val groupRDD1 = rdd.groupBy(_._1)
    groupRDD1.collect().foreach(println)

    sc.stop()
  }

}
