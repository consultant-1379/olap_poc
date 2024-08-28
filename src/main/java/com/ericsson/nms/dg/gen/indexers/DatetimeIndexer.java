package com.ericsson.nms.dg.gen.indexers;

import com.ericsson.nms.dg.gen.LineAccessFile;
import com.ericsson.nms.dg.schema.DataTypeType;
import org.apache.logging.log4j.Level;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Handler for DATETIME data.
 * Time: 13:20
 */
public class DatetimeIndexer extends BaseDataIndexer {

  /**
   * If cardinality is greator than this value, cardinality is assumed to be infiniate i.e random values generated.
   * If less than this value, values are either pre-generated of runtime-generated
   */
  private final double CardinalityThreshold = Integer.parseInt(System.getProperty("DATETIME_PREGEN_MAX_CARD", "50000000"));
  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  /**
   * Runtime generated value store
   */
  private List<String> dataList = null;

  /**
   * Constructor
   *
   * @param id        Indexer ID
   * @param typeModel Data Type being modeled
   */
  public DatetimeIndexer(final String id, final DataTypeType typeModel) {
    super(id, typeModel);
  }

  /**
   * Used to get runtime/random values for a ROP
   * <i>If data is pre-generated, super class can look up the file</i>
   *
   * @param index The data index to get
   * @return Value to write to a ROP file
   */
  @Override
  public String getROPValue(final int index) {
    final String ts;
    if (dataList == null) {
      ts = sdf.format(new Date(System.currentTimeMillis()));
    } else {
      ts = dataList.get(index);
    }
    return ts;
  }

  /**
   * Set up the indexer
   *
   * @param sampleDataFile (Optional) The pre-generated data file.
   */
  public void initData(final LineAccessFile sampleDataFile) {
    if (getDataType().getDataCardinality() >= CardinalityThreshold) {
      log(Level.TRACE, "Cardinality " + getDataType().getDataCardinality() + " >= " + CardinalityThreshold +
          " Data will be randomly generated at runtime");
    } else if (sampleDataFile != null) {
      super.initData(sampleDataFile);
    } else {
      this.dataList = new ArrayList<>(getDataType().getDataCardinality());
      for (int i = 0; i < getDataType().getDataCardinality(); i++) {
        dataList.add(new Timestamp(System.currentTimeMillis() - (i * 1000)).toString());
      }
    }
  }

  @Override
  public String getRandomROPValue() {
    return super.getRandomROPValue();
  }

  @Override
  public String readFromFile(int lineNumber) {
    return super.readFromFile(lineNumber);
  }
}
