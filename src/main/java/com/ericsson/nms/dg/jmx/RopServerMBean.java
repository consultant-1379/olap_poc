package com.ericsson.nms.dg.jmx;

import com.ericsson.nms.dg.gen.RopTask;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 31/10/13
 * Time: 14:50
 */
public class RopServerMBean extends BaseMBean {

  private final List<RopTask> tasks;
  private long totalTime = 0;
  private int rops = 0;

  public RopServerMBean(final List<RopTask> tasks) {
    this.tasks = tasks;
  }

  public void incrementTotal(final long inc) {
    this.totalTime += inc;
  }

  public void incrementRops() {
    this.rops++;
  }

  @Override
  public Object getAttributeValue(final String attName) {
    switch (attName) {
      case "Executing":
        final List<String> info = new ArrayList<>(tasks.size());
        for (RopTask task : tasks) {
          if (task.isExecuting()) {
            info.add(task.getId());
          }
          task.isExecuting();
        }
        return info.toArray(new String[info.size()]);
      case "Generated":
        return this.rops;
      case "AverageTime":
        return this.totalTime / this.rops;
      default:
        throw new RuntimeException(new NoSuchFieldException(attName));
    }
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    final List<MBeanAttributeInfo> attrs = new ArrayList<>();
    attrs.add(new MBeanAttributeInfo("Generated", "int", "Total ROPs Generated", true, false, false));
    attrs.add(new MBeanAttributeInfo("Executing", "String[]", "Executing", true, false, false));
    attrs.add(new MBeanAttributeInfo("AverageTime", "int", "Average ROP Generate Time", true, false, false));
    return new MBeanInfo(
        getClass().getName(),
        "ROP Generator",
        attrs.toArray(new MBeanAttributeInfo[attrs.size()]),
        null,
        null,
        null
    );
  }
}
