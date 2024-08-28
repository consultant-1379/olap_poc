package com.ericsson.nms.dg.gen.indexers;

import com.ericsson.nms.dg.gen.LineAccessFile;
import com.ericsson.nms.dg.schema.DataTypeType;

/**
 * Handler for integer data.
 */
public class IntegerIndexer extends BaseDataIndexer {

  /**
   * Constructor
   *
   * @param id        Indexer ID
   * @param typeModel Data Type being modeled
   */
  public IntegerIndexer(final String id, final DataTypeType typeModel) {
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
    return Integer.toString(index);
  }

  /**
   * Set up the indexer
   *
   * @param sampleDataFile (Optional) The pre-generated data file.
   */
  public void initData(final LineAccessFile sampleDataFile) {
    //
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
