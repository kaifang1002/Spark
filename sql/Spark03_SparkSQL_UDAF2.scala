package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object Spark03_SparkSQL_UDAF2 {
  def main(args: Array[String]): Unit = {
    //TODO 创建sparkSQL的运行环境

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df = spark.read.json("datas/user.json")

    //早期版本中 spark不能在sql中使用强类型UDAF操作
    //SQL & DSL
    //早期的UDAF强类型聚合函数使用DSL
    val ds = df.as[User]

    //将UDAF函数转换为查询的列对象
    val udafCol = new MyAvgUDAF().toColumn

    ds.select(udafCol).show

    spark.close()
  }

  //自定义聚合函数类 计算年龄的平均值avg
  //1.继承Aggregator
  // IN:输入的数据类型 User
  // BUF:缓冲区的数据类型 BUFF
  // OUT:输出的数据类型 Long
  //2.重写方法
  case class User(username:String,age:Long)
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
    //缓冲区初始值
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //根据输入的数据来更新缓冲区的数据
    override def reduce(buff: Buff, in: User): Buff = {
      buff.total = buff.total + in.age
      buff.count = buff.count + 1
      buff
    }

    //合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total=buff1.total+buff2.total
      buff1.count=buff1.count+buff2.count
      buff1
    }

    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total/buff.count
    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}