package com.kuze.bigdata.l100shizhan.PageSingleHopConversionRate

import java.text.DecimalFormat

/**
 * DecimalFomatTest
 *
 * @author baiye
 * @date 2020/9/1 下午4:25
 */
object DecimalFormatTest {

  def main(args: Array[String]): Unit = {
    val pi = 3.1415927
    //取一位整数
    println(new DecimalFormat("0").format(pi))  //3
    //取一位整数和两位小数
    println(new DecimalFormat("0.00").format(pi))  //3.14
    //取两位整数和三位小数，整数不足部分以0填补
    println(new DecimalFormat("00.000").format(pi))  //03.142
    //取所有整数部分
    println(new DecimalFormat("#").format(pi))  //3
    //以百分比方式计数，并取两位小数
    println(new DecimalFormat("#.##%").format(pi))  //314.16%
    //不取小数
    println(new DecimalFormat("#%").format(pi))   //314%
    val speed = 299792458
    //显示为科学计数法，并取五位小数
    println(new DecimalFormat("#.#####E0").format(speed))  //2.99792E8
    //显示为两位整数的科学计数法，并取四位小数
    println(new DecimalFormat("#.####E0").format(speed))   //2.9979E8
    //每三位以逗号进行分隔
    println(new DecimalFormat(",###").format(speed))   //299,792,458
    //将格式嵌入文本
    println(new DecimalFormat("速度#").format(speed))  //速度#
  }

}
