package rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val list=List("hello scala","hello spark")

    val rdd =sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word=>{
      println("@@@@@@@@@@")
      (word,1)
    }
    )

    //上面的结果存储到缓存中供下面使用
    //mapRDD.cache()
    //持久化必须在行动算子执行时使用
    //在数据执行较长，或者在数据比较重要的场合采用
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("**********")

    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }

}
