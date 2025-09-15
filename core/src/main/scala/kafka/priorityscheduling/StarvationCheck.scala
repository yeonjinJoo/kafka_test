package kafka.priorityscheduling

import kafka.network.RequestChannel
import java.util.concurrent.locks.ReentrantLock

class StarvationCheck {
  private val pass = new Array[Long](3) // queue 별로 pass 값 0으로 정의 - lock 필요
  private val stride = new Array[Long](3) // 비율에 맞게 변경 필요. queue 별로 각자 다른 stride 값 정의. lock 필요 x. 읽어오기만 o.
  private val starvationCount = new Array[Long](3) //  queue 별로 starvation count 값 정의
  private val starvationThreshold: Long = 0 // 설정 필요 - 그냥 값임. 읽어오기만 o.
  private val lock = new ReentrantLock()

  /**
   * 각 큐가 비어 있지 않은 경우, 해당 큐의 starvation 카운트를 1씩 증가시킨다.
   *
   * @param rc RequestChannel – 각 큐의 요청 개수를 조회
   * @return 없음
   */
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

  /**
   * 모든 큐에 대해 starvation이 발생했는지 확인한 뒤,
   * starvationThreshold를 초과한 큐의 pass 값을 현재 전체 큐 중 최소 pass 값으로 조정한다.
   *
   * @param 없음
   * @return 없음
   */
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

  /**
   * 현재 3개의 큐 중 pass 값이 가장 작은 큐를 선택한다.
   * - 비어있는 큐는 제외
   * - 선택된 큐의 starvationCount를 0으로 초기화하고
   * stride 값만큼 pass를 증가시킨다.
   *
   * @param rc RequestChannel – 각 큐의 요청 개수를 조회
   * @return 1~3 : 선택된 큐 번호, 0 : 모든 큐가 비어 있는 경우
   */
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

  /**
   * 현재 3개의 큐의 pass 중 가장 작은 pass 값을 반환한다.
   * - 큐가 비어있는지 여부는 상관 x
   *
   * Note: 이 메서드 내부에서는 별도의 락을 획득하지 않는다.
   * 이 메서드는 외부에서 이미 락을 잡은 상태에서 호출해야 한다.
   *
   * @param 없음
   * @return 모든 큐의 pass 값 중 최소값, 0 : 모든 pass 값이 초기값(Long.MaxValue)인 경우
   */
  def getMinPassValue(): Long = {
    // 이 함수 접근할 때는, 이미 다른 함수에서 lock 잡고 접근하기 때문에 lock 필요 x
    var minPassValue = Long.MaxValue
    for (i <- 0 until 3) {
      if (pass(i) < minPassValue) minPassValue = pass(i)
    }
    if (minPassValue == Long.MaxValue) 0L else minPassValue
  }

  /**
   * 모든 큐의 pass 값을 최소 pass 값 만큼 일괄 감소시켜,
   * 결과적으로 최소 pass 값이 0이 되도록 정규화한다.
   *
   * @param 없음
   * @return 없음
   */
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
}