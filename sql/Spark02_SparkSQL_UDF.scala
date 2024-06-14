package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //TODO 创建sparkSQL的运行环境

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //自定义函数
    spark.udf.register("prefixName",(name:String)=>{
      "NAME:"+name
    })

    spark.sql("select age,prefixName(username) from user").show

    spark.close()
  }
}
