package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartition
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //第一个参数 true（放回），false（丢弃）
    //第二个参数 每条数据被抽取的概率
    //         基准值的概念
    //第三个参数 抽取数据时随机算法的种子
    //         如果不传递第三个参数，那么使用的是当前系统时间
//    println(rdd.sample(
//      false,
//      0.4,
//      1
//    ).collect().mkString(","))

    //第一个参数 true（放回），false（丢弃）
    //第二个参数 每条数据被抽取的概率
    //         如果抽取不放回的场合，表示数据源中每条数据被抽取的概率，基准值的概念
    //         如果抽取放回的场合，表示数据源中的每条数据被抽取的可能次数
    //         基准值的概念
    //第三个参数 抽取数据时随机算法的种子
    //         如果不传递第三个参数，那么使用的是当前系统时间
    println(rdd.sample(
      true,
      2
    ).collect().mkString(","))

    sc.stop()
  }

}
