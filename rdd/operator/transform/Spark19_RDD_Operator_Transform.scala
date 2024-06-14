package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-key-value
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    rdd.reduceByKey(_ + _)
    rdd.aggregateByKey(0)(_ + _, _ + _)
    rdd.foldByKey(0)(_ + _) // aggregateByKey的两个条件相同时简化写法
    rdd.combineByKey(v => v, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)

    sc.stop()
  }

}
