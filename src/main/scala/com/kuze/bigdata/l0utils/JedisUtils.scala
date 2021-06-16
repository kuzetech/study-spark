package com.kuze.bigdata.l0utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisUtils {

  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(config, "192.168.0.243", 6379, 10000, "usky666!#%")

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]) {


    val conn = JedisUtils.getConnection()
    //    conn.set("income", "1000")
    //
    //    val r1 = conn.get("xiaoniu")
    //
    //    println(r1)
    //
    //    conn.incrBy("xiaoniu", -50)
    //
    //    val r2 = conn.get("xiaoniu")
    //
    //    println(r2)
    //
    //    conn.close()

    val r = conn.keys("statFlowChanArea-b2.com-127.0.0.2-*")
    import scala.collection.JavaConversions._
    for (p <- r) {
      println(p + " : " + conn.get(p))
    }
  }
}
