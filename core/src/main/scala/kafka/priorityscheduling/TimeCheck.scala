package kafka.priorityscheduling

import java.util.concurrent.locks.ReentrantLock
import kafka.priorityscheduling.StarvationCheck

class TimeCheck {
  private var lastNormalizationTime: Long = 0L // last pass normalization 된 시간
  private val lock = new ReentrantLock()
  private val timeThreshold: Long = 0L // time threshold 값 정의 - 그냥 값임. 읽어오기만 o.

  /**
   * 마지막 pass 정규화 시점으로부터 설정된 시간 timeThreshold가 경과했는지 확인하고,
   * 경과했다면 passNormalization()을 호출하여 모든 큐의 pass 값을 최소 pass 기준으로 재조정한다.
   *
   * @param sc StarvationCheck – passNormalization() 메서드를 호출해 pass 값을 재조정
   * @return 없음
   */
  def checkTimeMetThreshold(sc: StarvationCheck): Unit = {
    lock.lock()
    var isTimeMetThreshold = false
    try {
      val currentTime = System.currentTimeMillis()
      if (currentTime - lastNormalizationTime >= timeThreshold) {
        lastNormalizationTime = currentTime
        isTimeMetThreshold = true
      }
    } finally lock.unlock()

    // passNormalization 함수 호출. - 일정 시간 지났다면 모든 Queue의 pass를 min pass 기준으로 shift해주는. min pass가 0이 되도록.
    if (isTimeMetThreshold) sc.passNormalization()
  }
}