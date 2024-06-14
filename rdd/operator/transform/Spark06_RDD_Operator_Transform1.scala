package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd = sc.makeRDD(List("hello", "spark", "scala", "hadoop"), 2)

    val groupRDD = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)

    sc.stop()
  }

}
