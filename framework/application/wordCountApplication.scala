package framework.application

import framework.common.TApplication
import framework.controller.wordCountController

object wordCountApplication extends App with TApplication{
  start(){
    val controller = new wordCountController()
    controller.dispatch()
  }
}
