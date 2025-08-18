package kafka.monitor.reader;

import kafka.monitor.MonitorLog;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class MonitorLogReadStrategy implements IMonitorLogReadStrategy {
  
  private final String filepath;

  private BufferedReader reader;

  public MonitorLogReadStrategy(String filepath) {
    this.filepath = filepath;
  }

  private void tryInitReader() {
    if (reader != null) return;
    try {
      reader = new BufferedReader(new FileReader(filepath));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void read(List<MonitorLog> tar) {
    tryInitReader();

    reader.lines().skip(1).forEach(e -> {
      tar.add(parseMonitorLog(e));
    });
  }

  private MonitorLog parseMonitorLog(String str) {
    // Log에 기록되는 순서. ConsoleMonitorLogWriteStrategy 확인. type, id, priority, timestamp, timestampNano, state 순서
    String[] splittedStr = str.split(",");
    if (splittedStr.length < 6) {
      return null;
    }

    try {
      String type = splittedStr[0];
      String messageId = splittedStr[1];
      Integer priority = splittedStr[2];
      long timestamp = Long.parseLong(splittedStr[3]);
      long timestampNano = Long.parseLong(splittedStr[4]);
      String state = splittedStr[5];

      // MonitorLog 구조체 순서. priority & state 순서 주의
      return new MonitorLog(type, messageId, state, priority, timestamp, timestampNano);
    } catch (Exception e) {
      return null;
    }
  }
}
