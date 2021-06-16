package com.kuze.bigdata.l1rdd.core_cache

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 除了堆缓存，还有一种堆外缓存（缓存在外部系统）
  * ﻿Tachyon
  * http://tachyon-project.org/Running-Spark-on-Tachyon.html
  * reduce.cache() 直接存到内存
  * reduce.persist(StorageLevel.MEMORY_AND_DISK_SER_2) 可以指定存储方式
  * SER 代表存储序列化后的对象
  * _2  代表备份到两个不同节点
  */
object OperationCache_Persist {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val chars: RDD[String] = sc.parallelize(List("a","b","c","c"))

    val filter: RDD[String] = chars.filter(item => {
      println(item)
      item.length > 0
    })

    //既然filter会被多次利用肯定是要缓存的
    filter.cache()

    //RDD的操作分为两类：action和tranformation
    //cache和unpersisit两个操作比较特殊，他们既不是action也不是transformation。
    //cache会将标记需要缓存的rdd，真正缓存是在第一次被相关action调用后才缓存；
    //unpersisit是立刻抹掉该标记，并且立刻释放内存。

    val charTuple1: RDD[(String, Int)] = filter.map((_,1))

    val charTuple2: RDD[(String, Int)] = filter.map((_,2))

    //filter.unpersist()
    // 当位置在这里的时候，在charTuple1的take执行之前，filter还没存在内存中
    // 但是filter被标记了，又被剔除标记，等于没有标记。
    // 所以当charTuple2执行take时，虽然已经执行了charTuple1.take这个action，但是并不会缓存。
    // 这时就需要重新计算filter，导致rdd1.cache并没有达到应该有的作用

    println(charTuple1.take(4).toBuffer)
    println(charTuple2.take(4).toBuffer)

    //正确的位置是放在这
    filter.unpersist()

  }
}
