package kafka.priorityscheduling

import java.util.concurrent.locks.ReentrantLock
import kafka.priorityscheduling.StarvationCheck

class TimeCheck {
  // 변수 정의
  private var lastNormalizationTime = 0L // last pass normalization 된 시간
  private val lock = new ReentrantLock()
  private val timeThreshold = 0 // time threshold 값 정의 - 그냥 값임. 읽어오기만 o.

  // def 정의
  // timeThreshold 도달 시 passNormalization 실행
  def checkTimeMetThreshold(sc: StarvationCheck): Unit = {
    lock.lock()
    var isTimeMetThreshold = false
    try {
      val currentTime = System.currentTimeMillis()
      if (currentTime - lastNormalizationTime >= timeThreshold) {
        lastNormalizationTime = currentTime
        isTimeMetThreshold = true
      }
    } finally {
      lock.unlock()
    }

    // passNormalization 함수 호출. - 일정 시간 지났다면 모든 Queue의 pass를 min pass 기준으로 shift해주는. min pass가 0이 되도록.
    sc.passNormalization()
  }
}