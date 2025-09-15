package kafka.priorityscheduling

import kafka.network.RequestChannel

class StarvationCheck {
  // 변수 정의
  private val pass = new Array[Long](3) // queue 별로 pass 값 0으로 정의 - lock 필요
  private val stride = new Array[Float](3) // 비율에 맞게 변경 필요. queue 별로 각자 다른 stride 값 정의. lock 필요 x. 읽어오기만 o.
  private val starvationCount = new Array[Long](3) //  queue 별로 starvation count 값 정의
  private val starvationThreshold: Long = 0 // 설정 필요 - 그냥 값임. 읽어오기만 o.
  private val lock = new ReentrantLock()
  // 변수를 array로 정의하는 게 나을 것 같기도 하다. 다 어느 정도 한번에 확인하고, change 하니까. array가 맞겠네. 어차피 array 길이 3이니까.

  // queue 별로 비어있는지 확인하고, 비어있지않다면 모든 큐 starvation count에 대해 증가 함수
  def increaseStarvationCount(rc: RequestChannel): Unit = {
    lock.lock()
    try {
      if (rc.getRequestQueueP1Size() != 0) starvationCount(0) += 1
      if (rc.getRequestQueueP2Size() != 0) starvationCount(1) += 1
      if (rc.getRequestQueueP3Size() != 0) starvationCount(2) += 1
    } finally {
      lock.unlock()
    }
  }

  // 모든 queue에 대해서 starvation 발생한 queue가 있는지 확인하고, threshold를 넘었다면 해당 queue의 pass를 min pass로 조정
  def starvationBoosting(): Unit = {
    lock.lock()
    try {
      val minPassValue = getMinPassValue()
      for (i <- 0 until 3) {
        if (starvationCount(i) >= starvationThreshold)
          pass(i) = minPassValue
      }
    } finally {
      lock.unlock()
    }

  }

  // queue가 비어있지 않으면서, pass가 가장 작은 queue 어느건지 확인하고 그 starvation count 값 0으로 바꾸고 pass += stride 하는 함수 필요. return 값은 몇번째 큐인지 int - 그 큐를 실행하기 위함
  // 그 어떤 Queue에도 요청이 없는 경우 처리 필요. Kafka는 해당 Queue에서 계속 대기..?하는데... 이 경우는 Queue가 3개라
  def getMinPassQueueNum(rc: RequestChannel): Int = {
    lock.lock()
    try {
      var minPassValue = Long.MaxValue
      var queueNum = 0 // 모든 Queue에 요청이 없는 경우, 0 반환 - 값 받아서 시스템에서 처리 필요

      if (rc.getRequestQueueP1Size() != 0 && pass(0) < minPassValue) {
        minPassValue = pass(0);
        queueNum = 1
      }
      if (rc.getRequestQueueP2Size() != 0 && pass(1) < minPassValue) {
        minPassValue = pass(1);
        queueNum = 2
      }
      if (rc.getRequestQueueP3Size() != 0 && pass(2) < minPassValue) {
        minPassValue = pass(2);
        queueNum = 3
      }

      // 요청이 하나도 없는 경우가 아닐 때
      if (queueNum != 0) {
        starvationCount(queueNum - 1) = 0
        pass(queueNum - 1) += stride(queueNum - 1)
      }

      queueNum

    } finally {
      lock.unlock()
    }
  }

  // queue가 비었는지 여부와는 상관없이, 가장 작은 min pass 값 구해오기 - starvation인 큐 살려주기 위함
  def getMinPassValue(): Long = {
    // 이 함수 접근할 때는, 이미 다른 함수에서 lock 잡고 접근하기 때문에 lock 필요 x
    var minPassValue = Long.MaxValue
    for (i <- 0 until 3) {
      if (pass(i) < minPassValue) minPassValue = pass(i)
    }
    if (minPassValue == Long.MaxValue) 0L else minPassValue
  }

  // min pass가 0이 되도록 shift 해주는 함수
  def passNormalization(): Unit = {
    lock.lock()
    try {
      val minPassValue = getMinPassValue()
      for (i <- 0 until 3) {
        pass(i) -= minPassValue
      }
    } finally {
      lock.unlock()
    }
  }

  // def 정의
}