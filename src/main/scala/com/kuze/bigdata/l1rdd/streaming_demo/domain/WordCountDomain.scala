package com.kuze.bigdata.l1rdd.streaming_demo.domain

import scala.beans.BeanProperty

class WordCountDomain {

  @BeanProperty var time: Long = 0
  @BeanProperty var wordName: String = ""
  @BeanProperty var wordCount: Int = 0

}
