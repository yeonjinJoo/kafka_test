package kafka.priorityscheduling

import kafka.network.RequestChannel
import org.slf4j.LoggerFactory
import kafka.priorityscheduling.TimeCheck

class StarvationCheck {
  private val pass: Array[Long] = Array(0L, 0L, 0L)
  private val stride: Array[Long] = Array(5L, 3L, 2L)

  // 각 큐는 연속적으로 요청 최대 ~개까지만 처리 할 수 있다. 큐 별로 정해진 값
  private val occupyThreshold: Array[Long] = Array(5L, 5L, 5L)
  private val timeCheck = new TimeCheck(800L)
  private val reqLog = LoggerFactory.getLogger("kafka.request.logger")

  /**
   * 현재 3개의 큐 중 pass 값이 가장 작은 큐를 선택한다.
   *
   * 1) 비어있는 큐는 제외
   * 2) 선택된 큐가 있고, 해당 큐의 pass가 동료 큐 pass 대비 threshold 이상 뒤쳐지지 않으며
   * 비어있지 않은 큐가 2개 이상일 때만 선택된 큐의 pass에 stride를 더한다
   * // 비어있지 않은 큐가 선택된 큐 하나뿐이라면 pass 증가시키지 않음으로써 pass 독주로 인한 starvation을 방지한다
   *
   * @param sizes Array[Int] – 각 큐의 현재 대기 요청 개수를 담은 int array
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


  /**
   * 선택된 큐의 pass가 다른 큐들보다 과하게 뒤쳐졌다면 해당 큐의 pass를
   * 동료 큐들의 최소 pass 값으로 끌어올림으로써 (pass catch-up),
   * 한동안 하나의 큐만 계속해서 처리되는 경우를 방지한다.
   *
   * 1) 선택된 큐를 제외한 두 큐의 pass 중 최소값을 구한다
   * 2) 현재 선택된 큐가 그 최소값까지 따라가기 위해 필요한 향후 처리 건수를 계산한다
   * // 향후 처리 건수 = ( 두 큐의 pass 중 최소값 - 선택된 큐의 pass ) / 선택된 큐의 stride
   * 3) 향후 처리 건수가 occupyThreshold 이상이면 선택된 큐의 pass를 다른 두 큐의 pass 최소값으로 갱신하고 true 반환,
   * 그렇지 않으면 pass는 변경하지 않고 false 반환
   *
   * @param selected Int – 선택된 큐의 번호
   * @return 선택된 큐의 pass가 threshold 이상 뒤쳐졌다면 pass 갱신 후 true, 아니라면 false
   */
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

  /**
   * 각 큐의 헤드 요청의 waiting time과 timeout 정보를 이용해
   * 요청의 타임아웃 위험이 가장 큰 큐 번호를 선택한다
   *
   * 1) 큐가 비어있지 않고, 해당 큐 헤드 요청의 timeout 조건 값을 알고 있는 경우만 후보로 고려한다
   * 2) 위험 판정 기준 : 헤드 요청의 waiting time >= timeout * 0.6 (timeout 조건 값의 60% 이상 대기한 경우)
   * 3) 여러 후보가 있다면 경과 시간이 가장 큰 큐를 선택한다
   *
   * @param headAgeMsForPriorityQueues Array[Long] – 각 큐 헤드 요청의 대기 시간(ms), 큐가 비어있다면 -1
   * @param timeoutMsForPriorityQueues Array[Long] - 각 큐 헤드 요청의 timeout 조건 값(ms), 값을 모르면 -1
   * @return 1~3 : 위험 요청 가진 큐 번호, 0 : 위험 큐가 없는 경우
   */
  private def getTimeOutRiskyQueueNum(headAgeMsForPriorityQueues: Array[Long],
                                      timeoutMsForPriorityQueues: Array[Long]): Int = {
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

  /**
   * 우선순위 스케쥴링을 수행해 다음에 처리할 큐 번호를 선택한다
   *
   * 1) 타임아웃 위험 선처리 : 각 큐 헤드 요청의 대기시간/timeout 정보를 바탕으로, 위험 큐가 있으면 즉시 그 큐를 선택한다
   * 2) 주기적 pass 리셋 : 일정 시간이 지났다면 각 큐의 pass 값을 초기화한다 (장기 편향/값 과도하게 커지는 현상 방지)
   * 3) 가장 낮은 pass 가진 큐 선택 : 비어있지 않은 큐들 중 pass가 가장 작은 큐 선택한다
   *
   * @param rc                         RequestChannel - 각 큐 사이즈 조회에 사용
   * @param headAgeMsForPriorityQueues Array[Long] – 각 큐 헤드 요청의 대기 시간(ms), 큐가 비어있다면 -1
   * @param timeoutMsForPriorityQueues Array[Long] - 각 큐 헤드 요청의 timeout 조건 값(ms), 값을 모르면 -1
   * @return 1~3 : 선택된 큐 번호, 0 : 모든 큐가 비어있는 경우
   */
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