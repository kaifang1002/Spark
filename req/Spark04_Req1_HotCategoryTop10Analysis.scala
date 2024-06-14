package req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    //TODO:top10热门品类
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    //Q:存在大量的shuffle操作（reduceByKey）
    //reduceByKey 聚合算子，spark会提供优化

    //1.读取原始日志数据
    val actionRDD = sc.textFile("/datas/user_visit_action.txt")

    val acc = new HotCategoryAccumulator
    sc.register(acc, "hotCategory")

    //2.将数据转换结构
    actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          //点击场合
          acc.add((datas(6), "click"))
        } else if (datas(8) != "null") {
          val ids = datas(8).split("_")
          ids.foreach(
            id => {
              acc.add((id, "order"))
            }
          )
        } else if (datas(10) != "null") {
          val ids = datas(10).split("_")
          ids.foreach(
            id => {
              acc.add((id, "pay"))
            }
          )
        } else {
          Nil
        }
      }
    )

    //3.将相同的品类id的数据进行分组聚合
    //  （品类id，（点击数量，下单数量，支付数量））
    val accValue = acc.value
    val categories = accValue.map(_._2)

    val sort = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    )
    //4.将结果采集到控制台打印出来
    sort.take(10).foreach(println)

    sc.stop()
  }

  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  //自定义累加器
  //1.继承
  //IN:（品类id，行为类型）
  //OUT:mutable.Map[String,HotCategory]
  //2.重写方法（6）
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value

      map2.foreach({
        case (cid, hc) => {
          val category = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      })
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }

}