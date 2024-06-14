package rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Opertator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //TODO -行动算子
    //触发作业（job）执行的方法
    //底层代码调用的是
    rdd.collect()

    sc.stop()
  }

}
