package com.ericsson.nms.dg.ddlapply;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: qbhasha
 * Date: 05/11/13
 * Time: 13:29
 */
public class SQLExecutionTimer {

  private static SQLExecutionTimer instance = null;
  final Map<String, String> time_record;
  protected int instanceNumber = 0;


  private SQLExecutionTimer() {
    this.time_record = new HashMap<>();
    instanceNumber = 1;
  }

  public static synchronized SQLExecutionTimer getInstance() {
    if (instance == null) {
      instance = new SQLExecutionTimer();
    }

    return instance;

  }

  public synchronized boolean updateTimingDetails(String command, String timing) {
    time_record.put(command, timing);
    return true;
  }

  public String printTimeRecords() {
    String dt = "";
    for (Map.Entry<String, String> item : time_record.entrySet()) {
      dt = dt + "\n" + " Command: " + item.getKey() + "value: " + item.getValue();
    }
    return dt;
  }

  public List<String> printCommandSpecificTimeRecords() {
    List<String> executionTimes = new ArrayList<>();
    String createRecord = "";
    String dropRecord = "";
    String alterRecord = "";
    String aggregateRecord = "";
    for(Map.Entry<String, String> item : time_record.entrySet()){
      if (item.getKey().toLowerCase().startsWith("create")) {
        createRecord = createRecord + item.getKey() + ", " + item.getValue() + "\n";
      } else if (item.getKey().toLowerCase().startsWith("drop")) {
        dropRecord = dropRecord + item.getKey() + ", " + item.getValue() + "\n";
      } else if (item.getKey().toLowerCase().startsWith("alter")) {
        alterRecord = alterRecord + item.getKey() + ", " + item.getValue() + "\n";
      } else if (item.getKey().toLowerCase().startsWith("aggregate")) {
        aggregateRecord = aggregateRecord + item.getKey() + ", " + item.getValue() + "\n" ;
      }
    }
    if (!createRecord.isEmpty()) {
      executionTimes.add(createRecord);
    }
    if (!dropRecord.isEmpty()) {
      executionTimes.add(dropRecord);
    }
    if (!alterRecord.isEmpty()) {
      executionTimes.add(alterRecord);
    }
    if (!aggregateRecord.isEmpty()) {
      executionTimes.add(aggregateRecord);
    }
    return executionTimes;
  }
}
