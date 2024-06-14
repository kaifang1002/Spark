package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)

    val wordToOne = lines.map((_, 1))

    //窗口范围是采集周期的整数倍
    //窗口可以滑动，但是默认情况下，一个采集周期进行移动
    //可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的间隔（步长）
    val windowDS = wordToOne.window(Seconds(6),Seconds(6))

    val wordToCount = windowDS.reduceByKey(_ + _)

    wordToCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}