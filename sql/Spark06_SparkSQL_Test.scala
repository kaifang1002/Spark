package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Spark06_SparkSQL_Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    //TODO 创建sparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    spark.sql("user leader")

    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |      date: String,
        |      user_id: Long,
        |      session_id: String,
        |      page_id: Long,
        |      action_time: String,
        |      search_keyword: String,
        |      click_category_id: Long,
        |      click_product_id: Long,
        |      order_category_ids: String,
        |      order_product_ids: String,
        |      pay_category_ids: String,
        |      pay_product_ids: String,
        |      city_id: Long)
        |      row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/user_visit_action.txt' into table leader.user_visit_action
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name string,
        |  `extend_info` string)
        |   row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/product_info.txt' into table leader.product_info
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |  row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/city_info.txt' into table leader.city_info
        |""".stripMargin)

    //查询基本数据
    spark.sql(
      """
        | select
        |	  u.*,
        |	  p.product_name,
        |	  c.area,
        |	  c.city_name
        | from user_visit_action u
        | join product_info p on u.click_product_id = p.product_id
        | join city_info c on u.city_id = c.city_id
        | where u.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

    //根据区域，商品进行数据聚合
    spark.udf.register("cityRemark",functions.udaf(new CityRemarkUDAF()))
    spark.sql(
      """
        | select
        |		area,
        |		product_name,
        |		count(*) as clickCnt
        |   cityRemark(city_name) as city_remark
        | from t1
        | group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    //区域内对点击数量进行排序
    spark.sql(
      """
        | select
        |		*,
        |		rank() over(partition by area order by clickCnt) as rank
        |	from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    //取前三名
    spark.sql(
        """
          |select
          |	 *
          |from t3
          |where rank <= 3
          |""".stripMargin).show(false)

    spark.close()
  }
  //自定义聚合函数：实现城市备注功能
  //IN:城市名称
  //BUF:[总点击数量,map[(city,cnt),(city,cnt)]]
  //OUT:备注信息
  case class Buffer(var total:Long,var cityMap:mutable.Map[String,Long])
  class CityRemarkUDAF extends Aggregator[String,Buffer,String]{
    //缓冲区初始化
    override def zero: Buffer = {
      Buffer(0,mutable.Map[String,Long]())
    }

    //更新缓冲区
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total+=1
      val newCount = buff.cityMap.getOrElse(city,0L)+1
      buff.cityMap.update(city,newCount)
      buff
    }

    //合并缓冲区
    override def merge(buff1: Buffer, buff2: Buffer): Buffer = {
      buff1.total+=buff2.total

      val map1=buff1.cityMap
      val map2=buff2.cityMap

      buff1.cityMap = map1.foldLeft(map2){
        case(map,(city,cnt))=>{
          val newCount=map.getOrElse(city,0L)+cnt
          map.update(city,newCount)
          map
        }
      }
      buff1
    }

    //输出结果
    override def finish(buff: Buffer): String = {
      val remarkList = ListBuffer[String]()

      val totalcnt=buff.total
      val cityMap=buff.cityMap

      //降序排列
      val cityCntList = cityMap.toList.sortWith(
        (left,right)=>{
          left._2>right._2
        }
      ).take(2)

      val hasMore = cityMap.size>2
      var rsum=0L
      cityCntList.foreach({
        case(city,cnt)=>{
          val r = cnt*100/totalcnt
          remarkList.append(s"${city} ${r}%")
          rsum+=r
        }
      })

      if (hasMore){
        remarkList.append(s"其他 ${100-rsum}%")
      }

      remarkList.mkString(", ")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}