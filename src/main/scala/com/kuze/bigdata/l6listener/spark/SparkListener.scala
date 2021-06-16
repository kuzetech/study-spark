package com.kuze.bigdata.l6listener.spark

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockManagerAdded, SparkListenerBlockManagerRemoved, SparkListenerBlockUpdated, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerExecutorAdded, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart, SparkListenerUnpersistRDD}

object SparkListener extends SparkListener {
  /*当整个应用开始执行时*/
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit={
    /*获取appID-->spark on yarn模式下的appID一致*/
    applicationStart.appId
    /*appName*/
    applicationStart.appName
    /*driver端的日志，如果配置了日志的截断机制，获取的将不是完整日志*/
    applicationStart.driverLogs
    /*提交的用户*/
    applicationStart.sparkUser
    /*开始的事件*/
    applicationStart.time
  }
  /*当整个Application结束时调用的回调函数*/
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    /*结束的时间*/
    applicationEnd.time
  }
  /*当job开始执行时触发的回调函数*/
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    /*jobId和SParkUI上的一致*/
    jobStart.jobId
    /*配置信息*/
    jobStart.properties
    /*当前job根据宽窄依赖生成的所有strageID*/
    jobStart.stageIds
    /*job开始的时间*/
    jobStart.time
    jobStart.properties
    /*当前job每一个stage的信息抽象*/
    jobStart.stageInfos.foreach(stageInfo => {
      stageInfo.stageId
      /*stage提交的时间，不是task开始执行的时间，这个时间是stage开始抽象成taskDesc的开始时间*/
      stageInfo.submissionTime
      /*这个stage完成的时间*/
      stageInfo.completionTime
      /*当前stage发成了错误会重试，重试会在stageID后加上“_重试次数”*/
      stageInfo.attemptId
      /*当前staget的详细信息*/
      stageInfo.details
      /*当前stage累加器的中间结果*/
      stageInfo.accumulables
      /*如果当前stage失败，返回失败原因，如果做日志预警，可以在此处判断非空并嵌入代码收集错误日志*/
      stageInfo.failureReason
      stageInfo.name
      /*当前stage抽象出的taskSet的长度*/
      stageInfo.numTasks
      /*父依赖stage的id*/
      stageInfo.parentIds
      stageInfo.rddInfos
      /*task指标收集*/
      stageInfo.taskMetrics
      stageInfo.stageFailed("")
    })
  }
  /*当job结束时触发的回调函数*/
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobEnd.jobId
    jobEnd.time
    //如果job执行成功，会返回JobSucceeded，可以判断该值，进行邮件的发送。
    jobEnd.jobResult
  }
  /*当提交stage时触发的回调函数*/
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stageSubmitted.properties
    stageSubmitted.stageInfo.stageFailed("")
    stageSubmitted.stageInfo.attemptId
    stageSubmitted.stageInfo.taskMetrics.executorDeserializeTime
    stageSubmitted.stageInfo.taskMetrics.executorDeserializeCpuTime
    stageSubmitted.stageInfo.taskMetrics.executorCpuTime
    stageSubmitted.stageInfo.taskMetrics.diskBytesSpilled
    stageSubmitted.stageInfo.taskMetrics.inputMetrics.recordsRead
    stageSubmitted.stageInfo.taskMetrics.inputMetrics.bytesRead
    stageSubmitted.stageInfo.taskMetrics.outputMetrics.recordsWritten
    stageSubmitted.stageInfo.taskMetrics.outputMetrics.bytesWritten
    stageSubmitted.stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead
    stageSubmitted.stageInfo.taskMetrics.shuffleReadMetrics.recordsRead
    stageSubmitted.stageInfo.taskMetrics.shuffleReadMetrics.fetchWaitTime
    stageSubmitted.stageInfo.taskMetrics.shuffleReadMetrics.localBlocksFetched
    stageSubmitted.stageInfo.taskMetrics.shuffleReadMetrics.localBytesRead
    stageSubmitted.stageInfo.taskMetrics.shuffleReadMetrics.remoteBlocksFetched
    stageSubmitted.stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten
    stageSubmitted.stageInfo.taskMetrics.shuffleWriteMetrics.recordsWritten
    stageSubmitted.stageInfo.taskMetrics.shuffleWriteMetrics.writeTime
    stageSubmitted.stageInfo.taskMetrics.executorRunTime
    stageSubmitted.stageInfo.taskMetrics.jvmGCTime
    stageSubmitted.stageInfo.taskMetrics.memoryBytesSpilled
    stageSubmitted.stageInfo.taskMetrics.peakExecutionMemory
    stageSubmitted.stageInfo.taskMetrics.resultSerializationTime
    stageSubmitted.stageInfo.taskMetrics.resultSize
    stageSubmitted.stageInfo.taskMetrics.updatedBlockStatuses
    stageSubmitted.stageInfo.rddInfos
    stageSubmitted.stageInfo.parentIds
    stageSubmitted.stageInfo.details
    stageSubmitted.stageInfo.numTasks
    stageSubmitted.stageInfo.name
    stageSubmitted.stageInfo.accumulables
    stageSubmitted.stageInfo.completionTime
    stageSubmitted.stageInfo.submissionTime
    stageSubmitted.stageInfo.stageId
    stageSubmitted.stageInfo.failureReason

  }
  /*当stage完成时触发的回调函数*/
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageCompleted.stageInfo.attemptId
    stageCompleted.stageInfo.failureReason
    stageCompleted.stageInfo.stageId
    stageCompleted.stageInfo.submissionTime
    stageCompleted.stageInfo.completionTime
    stageCompleted.stageInfo.accumulables
    stageCompleted.stageInfo.details
    stageCompleted.stageInfo.name
    stageCompleted.stageInfo.numTasks
    stageCompleted.stageInfo.parentIds
    stageCompleted.stageInfo.rddInfos
    stageCompleted.stageInfo.taskMetrics

  }
  /*当task开始时触发的回调函数*/
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    taskStart.stageAttemptId
    taskStart.stageId
    taskStart.taskInfo.executorId
    taskStart.taskInfo.taskId
    taskStart.taskInfo.finishTime
    taskStart.taskInfo.launchTime
    taskStart.taskInfo.accumulables
    taskStart.taskInfo.attemptNumber
    taskStart.taskInfo.failed
    taskStart.taskInfo.gettingResultTime
    taskStart.taskInfo.gettingResult
    taskStart.taskInfo.executorId
    taskStart.taskInfo.host
    taskStart.taskInfo.index
    taskStart.taskInfo.killed
    taskStart.taskInfo.speculative
    taskStart.taskInfo.taskLocality
    taskStart.taskInfo.finished
    taskStart.taskInfo.id
    taskStart.taskInfo.running
    taskStart.taskInfo.successful
    taskStart.taskInfo.status
  }
  /*获取task执行的结果*/
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    taskGettingResult.taskInfo
  }
  /*当task执行完成时执行的回调函数*/
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    taskEnd.taskMetrics.resultSize
    taskEnd.taskMetrics.updatedBlockStatuses
    taskEnd.taskMetrics.resultSerializationTime
    taskEnd.taskMetrics.peakExecutionMemory
    taskEnd.taskMetrics.memoryBytesSpilled
    taskEnd.taskMetrics.jvmGCTime
    taskEnd.taskMetrics.executorRunTime
    taskEnd.taskMetrics.shuffleWriteMetrics
    taskEnd.taskMetrics.shuffleReadMetrics
    taskEnd.taskMetrics.outputMetrics
    taskEnd.taskMetrics.inputMetrics
    taskEnd.taskMetrics.diskBytesSpilled
    taskEnd.taskMetrics.executorCpuTime
    taskEnd.taskMetrics.executorDeserializeCpuTime
    taskEnd.taskMetrics.executorDeserializeTime
    taskEnd.taskInfo.executorId
    taskEnd.taskInfo.host
    taskEnd.taskInfo.index
    taskEnd.taskInfo.killed
    taskEnd.taskInfo.speculative
    taskEnd.taskInfo.taskLocality
    taskEnd.taskInfo.duration
    taskEnd.taskInfo.finished
    println("是否成功： "+taskEnd.taskInfo.finished)
    taskEnd.taskInfo.taskId
    taskEnd.taskInfo.id
    taskEnd.taskInfo.running
    taskEnd.stageId
    taskEnd.reason
    taskEnd.stageAttemptId
    taskEnd.taskType
  }
  /*新增bockManger时触发的回调函数*/
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    blockManagerAdded.blockManagerId.executorId
    blockManagerAdded.blockManagerId.host
    blockManagerAdded.blockManagerId.port
    blockManagerAdded.blockManagerId.topologyInfo
    blockManagerAdded.blockManagerId.hostPort
    blockManagerAdded.blockManagerId.isDriver
    blockManagerAdded.maxMem
    blockManagerAdded.time
  }
  /*当blockManage中管理的内存或者磁盘发生变化时触发的回调函数*/
  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    blockUpdated.blockUpdatedInfo.blockId
    blockUpdated.blockUpdatedInfo.blockManagerId
    blockUpdated.blockUpdatedInfo.diskSize
    blockUpdated.blockUpdatedInfo.memSize
    blockUpdated.blockUpdatedInfo.storageLevel
  }
  /*当blockManager回收时触发的回调函数*/
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    blockManagerRemoved.blockManagerId
    blockManagerRemoved.time
  }
  /*当 上下文环境发生变化是触发的回调函数*/
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    environmentUpdate.environmentDetails
  }
  /*当RDD发生unpersist时发生的回调函数*/
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
    unpersistRDD.rddId
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
  }
  /*当新增一个executor时触发的回调函数*/
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    executorAdded.executorId
    executorAdded.executorInfo.executorHost
    executorAdded.executorInfo.logUrlMap
    executorAdded.executorInfo.totalCores
    executorAdded.time
  }
  /*当executor发生变化时触发的回调函数*/
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    executorMetricsUpdate.accumUpdates
    executorMetricsUpdate.execId
  }
  /*当移除一个executor时触发的回调函数*/
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    executorRemoved.executorId
    executorRemoved.reason
    executorRemoved.time
  }
}
