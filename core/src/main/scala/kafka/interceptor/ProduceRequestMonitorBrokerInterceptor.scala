package kafka.interceptor

import kafka.monitor.writer.{MonitorLogWriter, ScrapableConsoleMonitorLogWriteStrategy}
import kafka.monitor.{MonitorLog, MonitorQueue}
import kafka.network.RequestChannel
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.record.Record

import java.nio.charset.StandardCharsets
import java.nio.ByteOrder

class ProduceRequestMonitorBrokerInterceptor(val logContext: LogContext) extends IBrokerInterceptor {

  private var monitorQueue: MonitorQueue = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _

  override def init(): Unit = {
    monitorQueue = new MonitorQueue()
    monitorLogWriter = new MonitorLogWriter(
      monitorQueue, new ScrapableConsoleMonitorLogWriteStrategy(), 1000)
    monitorLogThread = new Thread(monitorLogWriter)
    monitorLogThread.start()
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()
    if (request.header.apiKey == ApiKeys.PRODUCE) {
      val produceRequest = request.body[ProduceRequest]
      produceRequest.data().topicData().forEach(topic => topic.partitionData.forEach { partition =>
        val memoryRecords: MemoryRecords = partition.records.asInstanceOf[MemoryRecords]
        memoryRecords.batches.forEach(batch => {
          batch.forEach(record => {
            val valueBuffer = record.value()
            val messageId = if (valueBuffer != null) {
              val bytes = new Array[Byte](Math.min(100, valueBuffer.remaining()))
              valueBuffer.get(bytes)
              new String(bytes, StandardCharsets.UTF_8)
            } else {
              ""
            }

            // Log에 priority 추가
            val header: Header = record.headers().lastHeader("priority")
            val priority: Int = if (header != null && header.value().length >= 4) {
              ByteBuffer.wrap(header.value()).getInt()
            } else {
              0
            }

            monitorQueue.enqueue(new MonitorLog(
              "PRODUCE",
              messageId,
              "REQUESTED",
              priority,
              currentTime,
              currentTimeNano
            ))
          })
        })
      })
      monitorLogWriter.notifyIfNeeded()
    }
  }

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = {}

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()
    if (response.request.header.apiKey == ApiKeys.PRODUCE) {
      val produceRequest = response.request.body[ProduceRequest]
      produceRequest.data().topicData().forEach(topic => topic.partitionData.forEach { partition =>
        val memoryRecords: MemoryRecords = partition.records.asInstanceOf[MemoryRecords]
        memoryRecords.batches.forEach(batch => {
          batch.forEach(record => {
            val valueBuffer = record.value()
            val messageId = if (valueBuffer != null) {
              val bytes = new Array[Byte](Math.min(100, valueBuffer.remaining()))
              valueBuffer.get(bytes)
              new String(bytes, StandardCharsets.UTF_8)
            } else {
              ""
            }

            // Log에 priority 추가
            val header: Header = record.headers().lastHeader("priority")
            val priority: Int = if (header != null && header.value().length >= 4) {
              ByteBuffer.wrap(header.value()).getInt()
            } else {
              0
            }

            monitorQueue.enqueue(new MonitorLog(
              "PRODUCE",
              messageId,
              "COMMITED",
              priority,
              currentTime,
              currentTimeNano
            ))
          })
        })
      })
      monitorLogWriter.notifyIfNeeded()
    }
  }

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {}

  override def shutdown(): Unit = {
    if (monitorLogWriter == null || monitorLogThread == null) {
      return
    }

    monitorLogWriter.gracefulShutdown()
    monitorLogWriter.syncedNotify()

    try {
      monitorLogThread.join()
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("ProduceRequestMonitorBrokerInterceptor shutdown interrupted", e)
    }
  }
}
