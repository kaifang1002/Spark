package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-map
    val rdd = sc.textFile("datas/apache.log")

    val mapRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()
  }

}
