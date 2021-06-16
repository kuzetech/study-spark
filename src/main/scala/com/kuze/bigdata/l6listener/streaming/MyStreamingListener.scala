package com.kuze.bigdata.l6listener.streaming

import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.scheduler._


object MyStreamingListener extends StreamingListener {
  /*流式计算开始时，触发的回调函数*/
  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    streamingStarted.time
  }

  /*当前batch提交时触发的回调函数*/
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    batchSubmitted.batchInfo.streamIdToInputInfo.foreach(tuple => {
      val streamInputInfo = tuple._2
      streamInputInfo.metadata
      streamInputInfo.numRecords
      streamInputInfo.inputStreamId
      streamInputInfo.metadataDescription
    })

    batchSubmitted.batchInfo.numRecords
    batchSubmitted.batchInfo.outputOperationInfos
    batchSubmitted.batchInfo.submissionTime
    batchSubmitted.batchInfo.batchTime
    batchSubmitted.batchInfo.processingEndTime
    batchSubmitted.batchInfo.processingStartTime
    batchSubmitted.batchInfo.processingDelay
    batchSubmitted.batchInfo.schedulingDelay
    batchSubmitted.batchInfo.totalDelay
  }

  /*当前batch开始执行时触发的回调函数*/
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    batchStarted.batchInfo.totalDelay
    batchStarted.batchInfo.schedulingDelay
    batchStarted.batchInfo.processingDelay
    batchStarted.batchInfo.processingStartTime
    batchStarted.batchInfo.processingEndTime
    batchStarted.batchInfo.batchTime
    batchStarted.batchInfo.submissionTime
    batchStarted.batchInfo.outputOperationInfos
    batchStarted.batchInfo.numRecords
    batchStarted.batchInfo.streamIdToInputInfo
  }

  /*当前batch完成时触发的回调函数，该接口比较重要*/
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    batchCompleted.batchInfo.streamIdToInputInfo
    batchCompleted.batchInfo.numRecords
    batchCompleted.batchInfo.outputOperationInfos
    batchCompleted.batchInfo.submissionTime
    batchCompleted.batchInfo.batchTime
    batchCompleted.batchInfo.processingEndTime
    batchCompleted.batchInfo.processingStartTime
    batchCompleted.batchInfo.processingDelay
    batchCompleted.batchInfo.schedulingDelay
    batchCompleted.batchInfo.totalDelay
    //可以设置当批次处理时间超过某个值时，发出邮件警报
    println("批次处理的时间: "+batchCompleted.batchInfo.totalDelay.get)
    /*获取offset，并持久化到第三方容器*/
    batchCompleted.batchInfo.streamIdToInputInfo.foreach(tuple => {
      val offsets = tuple._2.metadata.get("offsets").get
      classOf[List[OffsetRange]].cast(offsets).foreach(offsetRange => {
        val partition = offsetRange.partition
        val minOffset = offsetRange.fromOffset
        val maxOffset = offsetRange.untilOffset
        val topicName = offsetRange.topic
        //TODO 将kafka容错信息，写到mysql/redis等框架达到数据容错
      })
    })
  }

  /*当接收器启动时触发的回调函数（非directStreaming）*/
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    receiverStarted.receiverInfo.executorId
    receiverStarted.receiverInfo.active
    receiverStarted.receiverInfo.lastError
    receiverStarted.receiverInfo.lastErrorMessage
    receiverStarted.receiverInfo.location
    receiverStarted.receiverInfo.name
    receiverStarted.receiverInfo.streamId
    receiverStarted.receiverInfo.lastErrorTime
  }

  /*当接收器结束时触发的回调函数（非directStreaming）*/
  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    receiverStopped.receiverInfo.lastErrorTime
    receiverStopped.receiverInfo.lastError
    receiverStopped.receiverInfo.streamId
    receiverStopped.receiverInfo.name
    receiverStopped.receiverInfo.location
    receiverStopped.receiverInfo.lastErrorMessage
    receiverStopped.receiverInfo.active
    receiverStopped.receiverInfo.executorId
  }

  /*当接收器发生错误时触发的回调函数（非directStreaming）*/
  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    receiverError.receiverInfo.executorId
    receiverError.receiverInfo.active
    receiverError.receiverInfo.lastErrorMessage
    receiverError.receiverInfo.lastError
    receiverError.receiverInfo.location
    receiverError.receiverInfo.name
    receiverError.receiverInfo.streamId
    receiverError.receiverInfo.lastErrorTime
  }

  /*当output开始时触发的回调函数*/
  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    outputOperationStarted.outputOperationInfo.description
    outputOperationStarted.outputOperationInfo.batchTime
    outputOperationStarted.outputOperationInfo.endTime
    outputOperationStarted.outputOperationInfo.failureReason
    outputOperationStarted.outputOperationInfo.id
    outputOperationStarted.outputOperationInfo.name
    outputOperationStarted.outputOperationInfo.startTime
    outputOperationStarted.outputOperationInfo.duration
    println("时间间隔1"+outputOperationStarted.outputOperationInfo.duration)
  }

  /*当output结束时触发的回调函数*/
  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    outputOperationCompleted.outputOperationInfo.duration
    outputOperationCompleted.outputOperationInfo.startTime
    outputOperationCompleted.outputOperationInfo.name
    outputOperationCompleted.outputOperationInfo.id
    outputOperationCompleted.outputOperationInfo.failureReason
    outputOperationCompleted.outputOperationInfo.endTime
    outputOperationCompleted.outputOperationInfo.batchTime
    outputOperationCompleted.outputOperationInfo.description
  }
}