package com.ericsson.nms.dg.jmx;

import com.ericsson.nms.dg.gen.LineAccessFile;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 30/10/13
 * Time: 11:00
 */
public class LineAccessFileMBean extends BaseMBean {
  private final LineAccessFile laf;
  private final String jmxName;

  public LineAccessFileMBean(final String jmxName, final LineAccessFile laf) {
    this.laf = laf;
    this.jmxName = jmxName;
  }

  @Override
  public Object getAttributeValue(final String attName) {
    switch (attName) {
      case "BackerID":
        return laf.getId();
      case "BackerFilePath":
        return laf.getAbsolutePath();
      case "BackerLineCount":
        return laf.getLineCount();
      case "CacheEnabled":
        return laf.cacheEnabled();
      case "CacheHits":
        return laf.getCacheHits();
      case "CacheMisses":
        return laf.getCacheMisses();
      case "TotalReads":
        return laf.getTotalReads();
      case "LockAquireWaitTime":
        return laf.getTotalLockAquireWaitTime();
      default:
        throw new RuntimeException(new NoSuchFieldException(attName));
    }
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    final List<MBeanAttributeInfo> attrs = new ArrayList<>();
    attrs.add(new MBeanAttributeInfo("BackerID", "String", "Backer ID", true, false, false));
    attrs.add(new MBeanAttributeInfo("BackerFilePath", "String", "File Path", true, false, false));
    attrs.add(new MBeanAttributeInfo("BackerLineCount", "int", "Line Count", true, false, false));
    attrs.add(new MBeanAttributeInfo("CacheEnabled", "boolean", "Cache Enabled", true, false, false));
    attrs.add(new MBeanAttributeInfo("CacheHits", "int", "Cache Hits", true, false, false));
    attrs.add(new MBeanAttributeInfo("CacheMisses", "int", "Cache Misses", true, false, false));
    attrs.add(new MBeanAttributeInfo("TotalReads", "int", "Total Reads", true, false, false));
    attrs.add(new MBeanAttributeInfo("LockAquireWaitTime", "long", "Total Lock Aquire Wait Time", true, false, false));
    return new MBeanInfo(
        getClass().getName(),
        this.jmxName,
        attrs.toArray(new MBeanAttributeInfo[attrs.size()]),
        null,
        null,
        null
    );
  }
}
