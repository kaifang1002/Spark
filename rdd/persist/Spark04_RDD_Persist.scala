package rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)
    sc.setCheckpointDir("cp")

    val list=List("hello scala","hello spark")

    val rdd =sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word=>{
      println("@@@@@@@@@@")
      (word,1)
    }
    )

    //checkpoint需要落盘，需要指定检查点保存路径
    //检查点路径保存的文件，当作业执行完毕后，不会被删除
    //一般保存路径都是在分布式存储系统中（如HDFS）
    mapRDD.checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("**********")

    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }

}
