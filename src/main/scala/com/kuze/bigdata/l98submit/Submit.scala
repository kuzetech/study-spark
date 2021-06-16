package com.kuze.bigdata.l98submit

object Submit {

  /**
    * 代码配置 》 submit配置 》 文件配置 》系统变量
    *
    *
    * bin/spark-submit
    * --master 标记指定连接的集群URL spark://host:port local[N] local[*] yarn
    * --deploy-mode 驱动器运行位置 client(本地) cluster(随机工作节点)
    * --class 应用主类
    * --name 应用名字
    * --jars 依赖的第三方jar包
    *
    * --files 分发到各节点的数据文件 /path/to/log4j.properties
    *
    * --properties-file 文件设置运行环境，键值对属性文件
    *
    * --conf properties=value 设置运行环境
    * --conf spark.yarn.maxAppAttempts=4 重新运行应用程序的最大尝试次数
    * --conf spark.yarn.am.attemptFailuresValidityInterval=1h 对应上面，一个小时重置一次最大尝试次数，不然重启一次少一次
    * --conf spark.yarn.max.executor.failures={8 * num_executors} 应用程序发生故障之前executor失败的最大数量
    * --conf spark.yarn.executor.failuresValidityInterval=1h 对应上面，一个小时重置一次最大尝试次数，不然重启一次少一次
    * --conf spark.task.maxFailures=8 放弃作业之前提高任务失败的最大数量
    * --conf spark.speculation=true 启用推测性执行时，批处理时间更加稳定
    * --conf spark.hadoop.fs.hdfs.impl.disable.cache=true 禁用HDFS缓存。如果没有，Spark将无法从HDFS上的文件读取更新的令牌
    * ﻿--conf spark.deploy.spreadOut=false 执行期进程合并到尽量少的核心
    * --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties
    * --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties
    * （--files /path/to/log4j.properties 配合上面log4j配置）
    *
    * --queue realtime_queue 单独的YARN队列
    *
    * --driver-memory 驱动器内存 512m
    * --executor-memory 单执行器内存 512m 1g
    * --num-executors 执行器数量 40
    * --executor-cores 单执行器核心数 30
    * --total-executor-cores 所有执行器核心总数 300
    *
    *
    */

}
