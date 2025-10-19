package kafka.priorityscheduling

import kafka.network.RequestChannel
import org.slf4j.LoggerFactory
import kafka.priorityscheduling.TimeCheck

class StarvationCheck {
  private val pass: Array[Long] = Array(0L, 0L, 0L) // queue 별로 pass 값 0으로 정의 - lock 필요
  private val stride: Array[Long] = Array(5L, 3L, 2L) // 비율에 맞게 변경 필요. queue 별로 각자 다른 stride 값 정의. lock 필요 x. 읽어오기만 o.

  // 각 큐는 연속적으로 요청 최대 ~개까지만 처리 할 수 있다. 큐 별로 정해진 값
  private val occupyThreshold: Array[Long] = Array(5L, 5L, 5L) // 설정 필요 - 그냥 값임. 읽어오기만 o.
  private val timeCheck = new TimeCheck(800L)
  private val reqLog = LoggerFactory.getLogger("kafka.request.logger")

  /**
   * 현재 3개의 큐 중 pass 값이 가장 작은 큐를 선택한다.
   * - 비어있는 큐는 제외
   * - 선택된 큐의 starvationCount를 0으로 초기화하고
   * stride 값만큼 pass를 증가시킨다.
   *
   * @param rc RequestChannel – 각 큐의 요청 개수를 조회
   * @return 1~3 : 선택된 큐 번호, 0 : 모든 큐가 비어 있는 경우
   */
  def getMinPassQueueNum(sizes: Array[Int]): Int = {
    var minPassValue = Long.MaxValue
    var queueNum = 0 // 모든 Queue에 요청이 없는 경우, 0 반환 - 값 받아서 시스템에서 처리 필요

    var notEmptyQueueCount = 0

    if (sizes(2) != 0) {
      notEmptyQueueCount += 1
      if (pass(2) < minPassValue) {
        minPassValue = pass(2);
        queueNum = 3
      }
    }
    if (sizes(1) != 0) {
      notEmptyQueueCount += 1
      if (pass(1) < minPassValue) {
        minPassValue = pass(1);
        queueNum = 2
      }
    }
    if (sizes(0) != 0) {
      notEmptyQueueCount += 1
      if (pass(0) < minPassValue) {
        minPassValue = pass(0);
        queueNum = 1
      }
    }

    if (reqLog.isTraceEnabled) {
      if (queueNum == 0)
        reqLog.trace(s"[PICK] none; sizes=[P1=${sizes(0)}, P2=${sizes(1)}, P3=${sizes(2)}], minPass=$minPassValue")
      else {
        val idx = queueNum - 1
        reqLog.trace(s"[PICK] P$queueNum; size=${sizes(idx)}, minPass=$minPassValue | sizes=[P1=${sizes(0)}, P2=${sizes(1)}, P3=${sizes(2)}], pass=[P1=${pass(0)}, P2=${pass(1)}, P3=${pass(2)}]")
      }
    }

    // 요청이 하나도 없는 경우가 아닐 때
    if (queueNum != 0) {
      if (!isPassFalledBehind(queueNum) && notEmptyQueueCount != 1) {
        // priority queue 하나에만 request가 있을 때, 혼자 있을 때 pass 너무 커지는 걸 막기 위해 본인 제외 나머지 큐가 모두 비어있다면 pass 증가 x
        pass(queueNum - 1) += stride(queueNum - 1)
      }
    }
    queueNum
  }

  // futureRequestNumDiff가 threshold를 넘어선 경우, 다른 두 Queue의 pass가 너무 앞서나가서 계속 이 selected queue만 처리될 위기에 놓인 것.
  // selected의 pass를 다른 두 Queue만큼으로 끌어올린후 true 반환 or pass 변경하지 않고 false 반환
  private def isPassFalledBehind(selected: Int): Boolean = {
    var minPassValueOfOthers: Long = Long.MaxValue

    for (i <- 0 until 3) {
      if (i != (selected - 1) && pass(i) < minPassValueOfOthers) {
        minPassValueOfOthers = pass(i)
      }
    }

    // 앞으로 이 selected queue가 minPassValue기 때문에, 다른 queue들 pass만큼 올라가기 위해서 처리되어야 할 request 갯수
    val futureRequestNumDiff = (minPassValueOfOthers - pass(selected - 1)) / stride(selected - 1)

    if (futureRequestNumDiff >= occupyThreshold(selected - 1)) {
      pass(selected - 1) = minPassValueOfOthers
      return true
    }
    return false
  }

  /**
   * 모든 큐의 pass 값을 0으로 Reset 시킨다.
   *
   * @param 없음
   * @return 없음
   */
  private def passReset(): Unit = {
    for (i <- 0 until 3) {
      pass(i) = 0
    }
  }

  private def getTimeOutRiskyQueueNum(headAgeMsForPriorityQueues: Array[Long], // 비어있으면 -1
                                      timeoutMsForPriorityQueues: Array[Long]): Int = { // 모르면 -1
    var queueNum = 0
    var maxTimeElapsed = Long.MinValue

    for (i <- 0 until 3) {
      val headAge = headAgeMsForPriorityQueues(i)
      val timeout = timeoutMsForPriorityQueues(i)

      // request가 비어있지 않고, timeout을 아는 경우
      if (headAge >= 0 && timeout >= 0) {
        // timeout 시간의 60% 이상으로 head가 오래된 경우
        if (headAge >= ((timeout * 3L) / 5L) && headAge > maxTimeElapsed) {
          queueNum = i + 1
          maxTimeElapsed = headAge
        }
      }
    }
    queueNum
  }

  def scheduleAndPick(rc: RequestChannel,
                      headAgeMsForPriorityQueues: Array[Long],
                      timeoutMsForPriorityQueues: Array[Long]): Int = {
    var chosen = 0

    val s1 = rc.getRequestQueueP1Size()
    val s2 = rc.getRequestQueueP2Size()
    val s3 = rc.getRequestQueueP3Size()
    if (s1 == 0 && s2 == 0 && s3 == 0) return 0

    val sizes: Array[Int] = Array(s1, s2, s3)

    // 1. timeoutRisky Request 있는지 확인 후, 있을 경우 바로 처리 - starvation boosting 대신 사용
    chosen = getTimeOutRiskyQueueNum(headAgeMsForPriorityQueues, timeoutMsForPriorityQueues)
    if (chosen != 0) return chosen

    // 2. 필요 시 pass reset
    if (timeCheck.checkTimeMetThreshold()) {
      passReset()
    }

    // 3. 세 개의 queue 중 비어있지 않으면서, 가장 pass 작은 queueNum 선택
    chosen = getMinPassQueueNum(sizes)

    // 선택된 queueNum 반환
    chosen

  }
}