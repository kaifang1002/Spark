package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartition
    val rdd = sc.makeRDD(List(("1",1),("11",2),("2",3)), 2)

    //val newRDD = rdd.sortBy(num => num._1)

    //默认根据制定规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    //默认情况下不会改变分区，但是中间存在shuffle操作
    val newRDD = rdd.sortBy(num => num._1.toInt,false)

    newRDD.collect().foreach(println)

    sc.stop()
  }

}
