package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-key-value
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    //aggregateByKey最终的返回数据结果应该和初始值的类型保持一致
    //rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)

    //获取相同key的数据的平均值=>(a,3),(b,4)
    val newRDD = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      }
      ,
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resultRDD = newRDD.mapValues({
      case (num, cnt) => {
        num / cnt
      }
    }
    )

    resultRDD.collect().foreach(println)

    sc.stop()
  }

}
