package req

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageflowAnalysis {
  def main(args: Array[String]): Unit = {
    //TODO:top10热门品类
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    val actionRDD = sc.textFile("/datas/user_visit_action.txt")

    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0)
          ,
          datas(1).toLong
          ,
          datas(2)
          ,
          datas(3).toLong
          ,
          datas(4)
          ,
          datas(5)
          ,
          datas(6).toLong
          ,
          datas(7).toLong
          ,
          datas(8)
          ,
          datas(9)
          ,
          datas(10)
          ,
          datas(11)
          ,
          datas(12).toLong
          ,
        )
      }
    )
    actionDataRDD.cache()

    //TODO 对指定的页面连续跳转进行统计
    //1-2，2-3，3-4，4-5，5-6，6-7
    val ids = List[Long](1,2,3,4,5,6,7)
    val okflowIds = ids.zip(ids.tail)

    //TODO 计算分母
    val pageidToCountMap: Map[Long, Long] = actionDataRDD.filter(
      action=>{
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap
    //TODO 计算分子
    //根据session进行分组
    val sessionRDD = actionDataRDD.groupBy(_.session_id)
    //分组后根据访问时间进行排序（升序）
    val mvRDD = sessionRDD.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        //[1,2,3,4]=>[1-2,2-3,3-4]
        //sliding:滑窗
        val flowIds = sortList.map(_.page_id)
        val pageflowIds = flowIds.zip(flowIds.tail)

        //将不合规的页面跳转进行过滤
        okflowIds.filter(
          t=>{
            okflowIds.contains(t)
          }
        ).map(
          t => {
            (t, 1)
          }
        )
      }
    )
    val flatRDD = mvRDD.map(_._2).flatMap(list => list)

    val dataRDD = flatRDD.reduceByKey(_ + _)
    //TODO 计算单跳转换率（分子/分母）
    dataRDD.foreach({
      case ((pageid1, pageid2), sum) => {
        val lon = pageidToCountMap.getOrElse(pageid1, 0L)
        println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为:" + (sum.toDouble / lon))
      }
    })

    sc.stop()
  }

  //用户访问动作列表
  case class UserVisitAction(
                              date: String,
                              user_id: Long,
                              session_id: String,
                              page_id: Long,
                              action_time: String,
                              search_keyword: String,
                              click_category_id: Long,
                              click_product_id: Long,
                              order_category_ids: String,
                              order_product_ids: String,
                              pay_category_ids: String,
                              pay_product_ids: String,
                              city_id: Long
                            )
}
