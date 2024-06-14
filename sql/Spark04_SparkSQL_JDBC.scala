package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    //TODO 创建sparkSQL的运行环境

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取数据
    val df = spark.read
          .format("jdbc")
          .option("url","jdbc:mysql://hadoop102:3306/spark-sql")
          .option("driver","com.mysql.jdbc.Driver")
          .option("user","root")
          .option("password","Fk20021002.")
          .option("dbtable","user")
          .load()
    //df.show

    //保存数据
    df.write
        .format("jdbc")
        .option("url", "jdbc:mysql://hadoop102:3306/spark-sql")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "Fk20021002.")
        .option("dbtable", "user1")
        .mode(SaveMode.Append)
        .save()

    spark.close()
  }
}