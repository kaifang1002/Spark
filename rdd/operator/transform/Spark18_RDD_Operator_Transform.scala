package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-key-value
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    //combineByKey：方法需要三个参数
    //第一个参数表示将相同key的第一个数据进行结构的转换，实现操作
    //第二个参数表示分区内计算规则
    //第三个参数表示分区间计算规则

    val newRDD = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      }
      ,
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resultRDD = newRDD.mapValues {
      case (num, cnt) =>
        num / cnt
    }

    resultRDD.collect().foreach(println)

    sc.stop()
  }

}
