package com.kuze.bigdata.l1rdd.core_double_rdd

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  *  筛选两边都有的key，再进行join
  */
object OperationLazy_Join {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    //从hive读取注册表,对应sql为select * from dwd_reg where DATE_SUB(CURDATE(), INTERVAL 3 DAY) <= log_date
    val reg: RDD[(Int, String, Long, String)] = sc.parallelize(List((1,"ID",1L,"202-09-11"),(2,"ID",1L,"202-09-11")))

    reg.cache()

    val todayData: RDD[((Int, String), Int)] = reg.filter(o => {
      o._4.equals("¥当天时间")
    }).map(o => {
      ((o._1, o._2), 1)
    })

    //计算当天用户数
    //todayData.reduceByKey()

    //从hive读取用户充值表select * from dwd_tran where DATE_SUB(CURDATE(), INTERVAL 3 DAY) <= log_date
    val tran: RDD[(Int, String, Float, Long, String)] = sc.parallelize(List((1,"ID",0 ,1L,"202-09-11")))

    val mapReg: RDD[((Int, String, String), Float)] = reg.map(o => {
      ((o._1, o._2, o._4), 0)
    })

    val mapTran: RDD[((Int, String, String), Float)] = tran.map(o => {
      ((o._1, o._2, o._5), o._3)
    })

    val joinData: RDD[((Int, String, String), (Float, Float))] = mapReg.join(mapTran)

    joinData.cache()

    val day3map: RDD[((Int, String), Float)] = joinData.map(o => {
      ((o._1._1, o._1._2), o._2._2)
    })

    val day3reduce: RDD[((Int, String), Float)] = day3map.reduceByKey(_ + _)

    val todayData2: RDD[((Int, String, String), (Float, Float))] = joinData.filter(o => {
      o._1._3.equals("今天")
    })

    //todayData2.reduceByKey()

    //从hive读取注册表
    val resource1: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    val resource2: RDD[(Int, Int)] = sc.parallelize(List((3,7),(4,5)))

    val join: RDD[(Int, (Int, Int))] = resource1.join(resource2)

    val result: Array[(Int, (Int, Int))] = join.collect()

    println(result.toBuffer)
    //ArrayBuffer((3,(4,7)), (3,(6,7)))

  }

}
