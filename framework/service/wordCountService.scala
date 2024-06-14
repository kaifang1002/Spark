package framework.service

import framework.common.TService
import framework.dao.wordCountDao

class wordCountService extends TService {
  private val WordCountDao = new wordCountDao()

  def dataAnalysis(): Array[(String, Int)] ={
    val lines = WordCountDao.readFile("datas/2.txt")
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))
    val wordToSum = wordToOne.reduceByKey(_ + _)
    val array = wordToSum.collect()
    array
  }
}
