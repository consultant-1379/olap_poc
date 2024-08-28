package com.ericsson.nms.dg.jmx;

import com.ericsson.nms.dg.gen.TableROPGenerator;

import javax.management.Attribute;
import javax.management.AttributeNotFoundException;
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
 * Time: 12:21
 */
public class ROPGeneratorMBean extends BaseMBean {
  private final TableROPGenerator tableGenerator;
  private final String jmxName;

  public ROPGeneratorMBean(final String jmxName, final TableROPGenerator generator) {
    this.jmxName = jmxName;
    this.tableGenerator = generator;
  }

  @Override
  public Object getAttributeValue(final String attName) {
    switch (attName) {
      case "RopCompleted":
        return tableGenerator.isRopCompleted();
      case "TableID":
        return tableGenerator.getTableName();
      case "TotalGenerateTime":
        return tableGenerator.getTotalGenerateTime();
      case "AverageGenerateTime":
        return tableGenerator.getAverageGenerateTime();
      case "NumRopsGenerated":
        return tableGenerator.getNumGenerated();
      case "FileTotalWriteTime":
        return tableGenerator.getTotalFileWriteTime();
      case "FileLastWriteTime":
        return tableGenerator.getLastFileWriteTime();
      case "FileTotalIOWriteCount":
        return tableGenerator.getTotalFileIOWriteCount();
      case "FileROPWriteTime":
        return tableGenerator.getROPFileWriteTime();
      case "RowsPerROP":
        return tableGenerator.getRowsPerROP();
      case "RowsGeneratedCurrent":
        return tableGenerator.getRowGenerateCount();
      default:
        throw new RuntimeException(new NoSuchFieldException(attName));
    }
  }

  @Override
  public void setAttribute(final Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
    //
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    final List<MBeanAttributeInfo> attrs = new ArrayList<>();
    attrs.add(new MBeanAttributeInfo("TableID", "String", "Table ID", true, false, false));
    attrs.add(new MBeanAttributeInfo("RopCompleted", "boolean", "RopCompleted", true, false, false));
    attrs.add(new MBeanAttributeInfo("TotalGenerateTime", "String", "Total Generate Time", true, false, false));
    attrs.add(new MBeanAttributeInfo("AverageGenerateTime", "String", "Average Generate Time", true, false, false));
    attrs.add(new MBeanAttributeInfo("NumRopsGenerated", "String", "Num Rops Generated", true, false, false));
    attrs.add(new MBeanAttributeInfo("RowsPerROP", "String", "Rows per ROP file", true, false, false));
    attrs.add(new MBeanAttributeInfo("FileTotalWriteTime", "long", "...", true, false, false));
    attrs.add(new MBeanAttributeInfo("FileLastWriteTime", "long", "...", true, false, false));
    attrs.add(new MBeanAttributeInfo("FileTotalIOWriteCount", "long", "...", true, false, false));
    attrs.add(new MBeanAttributeInfo("FileROPWriteTime", "long", "...", true, false, false));
    attrs.add(new MBeanAttributeInfo("RowsGeneratedCurrent", "long", "...", true, false, false));
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

