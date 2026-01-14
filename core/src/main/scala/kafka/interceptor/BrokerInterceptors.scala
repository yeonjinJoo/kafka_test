package kafka.interceptor

import kafka.network.RequestChannel;


class BrokerInterceptors(val interceptors: Vector[IBrokerInterceptor]) {

  def init(): Unit = {
    interceptors.foreach(_.init())
  }

  def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    interceptors.foreach(_.beforeSendRequestToQueue(request, connectionId))
  }

  def beforeHandleRequest(request: RequestChannel.Request): Unit = {
    interceptors.foreach(_.beforeHandleRequest(request))
  }

  def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    interceptors.foreach(_.beforeSendResponseToQueue(response))
  }

  def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {
    interceptors.foreach(_.afterProcessResponse(response, connectionId))
  }

  def addUselssRequest(request: RequestChannel.Request): Unit = {
    interceptors.foreach(_.addUselssRequest(request))
  }

  def shutdown(): Unit = {
    interceptors.foreach(_.shutdown())
  }
}
