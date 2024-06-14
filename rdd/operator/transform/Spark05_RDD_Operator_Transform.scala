package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    // list=>int
    // int=>array
    val glomRDD = rdd.glom()

    glomRDD.collect().foreach(data=>println(data.mkString(",")))

    sc.stop()
  }

}
