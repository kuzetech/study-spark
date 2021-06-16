package com.kuze.bigdata.l100shizhan.topN

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * CategoryTopApp
 *
 * @author baiye
 * @date 2020/9/1 下午3:58
 */
object CategoryTopApp {

  def calcCategoryTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) = {

    // 使用累加器完成3个指标的累加:   点击 下单量 支付量
    val acc = new CategoryAcc
    sc.register(acc)

    userVisitActionRDD.foreach(action => acc.add(action))

    //把一个品类的三个指标封装到一个map中（品类ID，品类ID，行为类型，总次数）
    val cidActionCountGrouped: Map[String, Map[(String, String), Long]] = acc.value.groupBy(_._1._1)

    //把结果封装到样例类中
    val categoryCountInfoArray: Array[CategoryCountInfo] = cidActionCountGrouped.map {
      case (categoryId, map) =>
        CategoryCountInfo(categoryId,
          map.getOrElse((categoryId, "click"), 0L),
          map.getOrElse((categoryId, "order"), 0L),
          map.getOrElse((categoryId, "pay"), 0L)
        )
    }.toArray

    // 3. 对数据进行排序取top10
    val result: Array[CategoryCountInfo] = categoryCountInfoArray
      //按照clickCount、orderCount、payCount进行排序，前一个相等就比较后一个
      .sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount))
      .take(10)

    result.foreach(println)

  }

}
