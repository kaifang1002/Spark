package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("RDD")
    //手动配置核数
    //sparkConf.set("spark.default.parallelism","5")
    val sc=new SparkContext(sparkConf)

    //TODO 创建RDD
    //RDD的并行度&分区
    //makeRDD可以传递第二个参数，表示分区数量；也可以不传递，默认值当前环境下的最大可用核数
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4), 2
    )

    //待处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }

}
