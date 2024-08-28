package com.ericsson.nms.dg.gen.indexers;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.gen.LineAccessFile;
import com.ericsson.nms.dg.schema.DataTypeType;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.logging.log4j.Level;

import java.util.HashMap;
import java.util.Map;

/**
 * Handler for varchar data.
 */
public class VarcharIndexer extends BaseDataIndexer {

  /**
   * If cardinality is greator than this value, cardinality is assumed to be infiniate i.e random values generated.
   * If less than this value, values are either pre-generated of runtime-generated
   */
  private final double CardinalityThreshold = Integer.parseInt(System.getProperty("VARCHAR_PREGEN_MAX_CARD", "50000000"));
  /**
   * Runtime generated value store
   */
  private Map<Integer, String> generatedData = null;
  /**
   * Random string generator
   */
  private RandomDataGenerator randomDataGenerator = null;
  private NormalDistribution normalDistribution = null;

  private boolean cacheGeneratedValues = true;

  /**
   * Constructor
   *
   * @param id        Indexer ID
   * @param typeModel Data Type being modeled
   */
  public VarcharIndexer(final String id, final DataTypeType typeModel) {
    this(id, typeModel, true);
  }

  public VarcharIndexer(final String id, final DataTypeType typeModel, final boolean cache) {
    super(id, typeModel);
    this.cacheGeneratedValues = cache;
    randomDataGenerator = new RandomDataGenerator();
    normalDistribution = new NormalDistribution(typeModel.getAvgDataLength(), 10);
  }

  /**
   * Set up the indexer
   *
   * @param sampleDataFile (Optional) The pre-generated data file.
   */
  @Override
  public void initData(final LineAccessFile sampleDataFile) {
    if (sampleDataFile != null) {
      super.initData(sampleDataFile);
    } else {
      log(Level.INFO, "Cardinality " + getDataType().getDataCardinality() + " >= " + CardinalityThreshold +
          " Data will be randomly generated at runtime");
      this.generatedData = new HashMap<>();
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
    if (this.generatedData.containsKey(index)) {
      value = this.generatedData.get(index);
    } else {
      if (getDataType().getSuppDataLength() == null) {
        throw new GenerateException("No DDL length for " + getId());
      }
      final int length = (int) Math.ceil(normalDistribution.sample());
      if (length <= 0) {
        value = "";
      } else {
        value = randomDataGenerator.nextHexString(length);
      }
      if (cacheGeneratedValues) {
        this.generatedData.put(index, value);
      }
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
