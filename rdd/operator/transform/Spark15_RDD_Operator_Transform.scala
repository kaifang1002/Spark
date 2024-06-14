package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-key-value
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    //相同的KEY的数据进行value数据的聚合操作，两两聚合
    //[1,2,3]=>[3,3]=>[6]
    //reduceByKey中如中key的数据只有一个，是不会参与运算的
    val reduceRDD = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x=${x},y=${y}")
      x + y
    })

    reduceRDD.collect().foreach(println)

    sc.stop()
  }

}
