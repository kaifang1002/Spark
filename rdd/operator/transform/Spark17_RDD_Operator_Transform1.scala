package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-key-value
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4),("b", 5),("a", 6)),2)

    val RDD = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    RDD.collect().foreach(println)

    sc.stop()
  }

}
