package com.kuze.bigdata.l0utils

import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {

  def main(args: Array[String]): Unit = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val date = new Date
    date.setTime(1550304780000L)
    print(s"${simpleDateFormat.format(date)}")
  }

}
