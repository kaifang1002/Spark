package rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Opertator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO -行动算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val user=new User()

    //rdd算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    rdd.foreach(
      num=>{
        println("age="+(user.age+num))
      }
    )

    sc.stop()
  }

  //class User extends Serializable {
  //样例类在编译时，会自动混入序列化特质（实现可序列化接口）
  case class User(){
    var age: Int = 30
  }

}
