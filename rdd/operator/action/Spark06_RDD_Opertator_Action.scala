package rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Opertator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO -行动算子
    val rdd = sc.makeRDD(List(1,2,3,4))
    //foreach 其实是driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("**********")
    //foreach 其实是excutor端内存数据打印
    rdd.foreach(println)

    //算子：operator（操作）
    //      rdd的方法和scala集合对象的方法不一样
    //      集合对象的方法都是在同一个节点的内存中完成的
    //      rdd的方法可以将计算逻辑发送到excutor端（分布式节点）执行
    //      为了区分不同的处理效果，所以将rdd的方法称之为算子
    //      rdd的方法外部的操作都是在driver端执行的，而方法内部的逻辑代码是在excutor端执行的

    sc.stop()
  }

}
