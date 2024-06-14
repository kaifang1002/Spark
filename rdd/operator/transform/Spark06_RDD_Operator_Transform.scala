package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    def groupFunction(num: Int) = {
      num % 2
    }

    val groupRDD = rdd.groupBy(groupFunction)

    groupRDD.collect().foreach(println)

    sc.stop()
  }

}
