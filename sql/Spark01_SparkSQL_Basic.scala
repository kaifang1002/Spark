package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //TODO 创建sparkSQL的运行环境

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //TODO 执行逻辑操作

    // DaTaFrame
    //val df = spark.read.json("datas/user.json")
    //df.show()

    // DaTaFrame=>SQL
    //df.createOrReplaceTempView("user")
    //spark.sql("select avg(age) from user").show

    // DaTaFrame=>DSL
    //在使用DaTaFrame时，如果涉及到转换操作，需要引入转换规则
    //df.select("age","username").show
    //df.select($"age"+1).show
    //df.select('age+1).show

    // DaTaSet
    //val seq =Seq(1,2,3,4)
    //val ds:Dataset[Int]=seq.toDS()
    //ds.show

    // RDD<=>DaTaFrame
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 20)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD = df.rdd

    // DaTaFrame<=>DaTaSet
    val ds = df.as[User]
    val df1 = ds.toDF()

    // RDD<=>DaTaSet
    val ds1 = rdd.map({
      case (id, name, age) => {
        User(id, name, age)
      }
    }).toDS()
    val userRDD = ds1.rdd

    //TODO 关闭环境

    spark.close()
  }
  case class User(id:Int,name:String,age:Int)
}
