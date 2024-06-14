package rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Opertator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO -行动算子
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3)))
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    //saveAsSequenceFile:必须为kv类型的数据
    rdd.saveAsSequenceFile("output2")

    sc.stop()
  }

}
