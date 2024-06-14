package req

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    //TODO:top10热门品类
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    //Q:actionRDD重复使用

    //1.读取原始日志数据
    val actionRDD = sc.textFile("/datas/user_visit_action.txt")
    actionRDD.cache()

    //2.统计品类的点击数量（品类id 点击数量）
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)
    //3.统计品类的下单数量（品类id 下单数量）
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)
    //4.统计品类的支付数量（品类id 支付数量）
    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)
    //5.将品类进行排序，并且取前10名
    //  点击数量排序，下单数量排序，支付数量排序
    //  元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    //  （品类id，（点击数量，下单数量，支付数量））
    val rdd1 = clickCountRDD.map({
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    })

    val rdd2 = orderCountRDD.map({
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    })

    val rdd3 = payCountRDD.map({
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    })

    val sourceRDD = rdd1.union(rdd2).union(rdd3)

    val analysisRDD = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t2._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD = analysisRDD.sortBy(_._2,false).take(10)
    //6.将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()
  }

}
