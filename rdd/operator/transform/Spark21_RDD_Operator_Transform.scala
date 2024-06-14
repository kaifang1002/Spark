package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-key-value
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5)))

    val leftJoinRDD = rdd1.leftOuterJoin(rdd2)

    leftJoinRDD.collect().foreach(println)



    val rdd3 = sc.makeRDD(List(("a", 1), ("b", 2)))

    val rdd4 = sc.makeRDD(List(("a", 4), ("b", 5),("c",6)))

    val rightJoinRDD = rdd3.rightOuterJoin(rdd4)

    rightJoinRDD.collect().foreach(println)

    sc.stop()
  }

}
