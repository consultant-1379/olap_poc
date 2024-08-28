package com.ericsson.nms.dg.gen.indexers;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.gen.LineAccessFile;
import com.ericsson.nms.dg.schema.DataTypeType;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.logging.log4j.Level;

import java.util.List;
import java.util.Random;

/**
 * Handler for varchar data.
 */
public class VarcharIndexer_1 extends BaseDataIndexer {

  /**
   * If cardinality is greator than this value, cardinality is assumed to be infiniate i.e random values generated.
   * If less than this value, values are either pre-generated of runtime-generated
   */
  private final double CardinalityThreshold = Integer.parseInt(System.getProperty("VARCHAR_PREGEN_MAX_CARD", "50000000"));
  /**
   * Runtime generated value store
   */
  private List<String> data = null;
  private String pseudoBase = null;
  private int RNG_MAX = 0;
  private Random RNG = null;

  /**
   * Constructor
   *
   * @param id        Indexer ID
   * @param typeModel Data Type being modeled
   */
  public VarcharIndexer_1(final String id, final DataTypeType typeModel) {
    super(id, typeModel);
  }

  /**
   * Set up the indexer
   *
   * @param sampleDataFile (Optional) The pre-generated data file.
   */
  @Override
  public void initData(final LineAccessFile sampleDataFile) {
    if (getDataType().getDataCardinality() >= CardinalityThreshold || sampleDataFile == null) {
      log(Level.INFO, "Cardinality " + getDataType().getDataCardinality() + " >= " + CardinalityThreshold +
          " Data will be randomly generated at runtime (or no sample data)");
      final int average = getDataType().getAvgDataLength() - Integer.toString(getDataType().getDataCardinality()).length();
      pseudoBase = RandomStringUtils.randomAlphabetic(average);
      RNG_MAX = (getDataType().getAvgDataLength() - average) * 2;
      RNG = new Random();
    }
    if (sampleDataFile != null) {
      super.initData(sampleDataFile);
    }
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
    final String value;
    if (data == null || data.isEmpty()) {
      if (getDataType().getSuppDataLength() == null) {
        throw new GenerateException("No DDL length for " + getId());
      }
      value = pseudoBase + RandomStringUtils.randomAlphabetic(RNG.nextInt(RNG_MAX));
    } else {
      value = data.get(index);
    }
    return value;
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
