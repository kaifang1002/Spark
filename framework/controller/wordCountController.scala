package framework.controller

import framework.common.TController
import framework.service.wordCountService

class wordCountController extends TController{
  private val WordCountService=new wordCountService()

  def dispatch()={
    val array = WordCountService.dataAnalysis()
    array.foreach(println)
  }
}
