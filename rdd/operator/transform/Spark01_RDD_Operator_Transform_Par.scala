package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-map
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRDD = rdd.map(
      num => {
        println(">>>>>>>>>>" + num)
      }
    )

    val mapRDD1 = rdd.map(
      num => {
        println("##########" + num)
      }
    )

    mapRDD.collect()
    mapRDD1.collect()

    sc.stop()
  }

}
