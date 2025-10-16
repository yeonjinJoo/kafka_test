package kafka.priorityscheduling

// time threshold 값을 class 생성할 때 받기
class TimeCheck(timeThreshold: Long = 800L) {
  private var lastNormalizationTime: Long = 0L // last pass normalization 된 시간
  //  private val timeThreshold: Long = 800L // time threshold 값 정의 - 그냥 값임. 읽어오기만 o.
  // 800ms에 한번씩 reset

  /**
   * 마지막 pass 정규화 시점으로부터 설정된 시간 timeThreshold가 경과했는지 확인
   *
   * @param sc 없음
   * @return timeThreshold를 넘겼다면 true, 아니라면 false
   */
  def checkTimeMetThreshold(): Boolean = {
    var isTimeMetThreshold = false
    val currentTime = System.currentTimeMillis()
    if (currentTime - lastNormalizationTime >= timeThreshold) {
      lastNormalizationTime = currentTime
      isTimeMetThreshold = true
    }
    isTimeMetThreshold
  }
}