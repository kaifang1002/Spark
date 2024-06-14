package rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Opertator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //TODO -行动算子
    //reduce：两两聚合
    //val i = rdd.reduce(_ + _)
    //println(i)

    //collect:会将不同分区的数据按照分区顺序采集到driver端内存中，形成数组
    //val ints = rdd.collect()
    //println(ints.mkString(","))

    //count:数据源中数据的个数
    val cnt = rdd.count()
    println(cnt)

    //first 获取第一个数据
    val first = rdd.first()
    println(first)

    //take:获取n个数据
    val ints = rdd.take(3)
    println(ints.mkString(","))

    //takeOrdered:排序后取n个数据
    val rdd1 = sc.makeRDD(List(4,2,3,1))
    val ints1 = rdd1.takeOrdered(3)
    println(ints1.mkString(","))

    sc.stop()
  }

}
