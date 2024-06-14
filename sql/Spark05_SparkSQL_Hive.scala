package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Spark05_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    //TODO 创建sparkSQL的运行环境

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //使用SparkSQL连接外置的Hive
    //1.拷贝hive-site.xml文件到classpath下
    //2.启用hive的支持
    //3.增加对应的依赖关系（包含mysql驱动）
    spark.sql("show tables").show

    spark.close()
  }
}