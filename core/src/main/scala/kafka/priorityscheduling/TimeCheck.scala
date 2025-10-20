package kafka.priorityscheduling

class TimeCheck(timeThreshold: Long = 800L) {
  private var lastResetTime: Long = 0L // last pass 초기화 된 시간

  /**
   * 마지막 pass 초기화 시점으로부터 설정된 시간 timeThreshold가 경과했는지 확인
   *
   * @param 없음
   * @return timeThreshold를 넘겼다면 true, 아니라면 false
   */
  def checkTimeMetThreshold(): Boolean = {
    var isTimeMetThreshold = false
    val currentTime = System.nanoTime()
    if (((currentTime - lastResetTime) / 1_000_000) >= timeThreshold) {
      lastResetTime = currentTime
      isTimeMetThreshold = true
    }
    isTimeMetThreshold
  }
}