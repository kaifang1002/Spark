package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-key-value
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

    //join:两个不同数据源的数据，相同的key的value会连接在一起，形成元组
    //     如果两个数据源中的key没有匹配上，那么数据不会出现在结果中
    //     如果两个数据源中的key有多个相同的，那么会依次匹配，可能会出现笛卡尔积，数据量会几何形增长，性能会较低
    val joinRDD = rdd1.join(rdd2)

    joinRDD.collect().foreach(println)

    sc.stop()
  }

}
