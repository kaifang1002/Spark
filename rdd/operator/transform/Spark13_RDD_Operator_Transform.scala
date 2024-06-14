package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-双value 交集 并集 差集 要求数据类型保持一致
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))

    //交集[3,4]
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    //并集[1,2,3,4,3,4,5,6]
    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    //差集[1,2] [5,6]
    val rdd5 = rdd1.subtract(rdd2)
    val rdd6 = rdd2.subtract(rdd1)
    println(rdd5.collect().mkString(","))
    println(rdd6.collect().mkString(","))

    //拉链[(1,3),(2,4),(3,5),(4,6)]
    val rdd7 = rdd1.zip(rdd2)
    println(rdd7.collect().mkString(","))

    sc.stop()
  }

}
