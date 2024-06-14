package rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val list=List("hello scala","hello spark")

    val rdd =sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map((_, 1))

    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("**********")

    val list1 = List("hello scala", "hello spark")

    val rdd1 = sc.makeRDD(list1)

    val flatRDD1 = rdd1.flatMap(_.split(" "))

    val mapRDD1 = flatRDD1.map((_, 1))

    val groupRDD = mapRDD1.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }

}
