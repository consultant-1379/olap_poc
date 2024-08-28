package com.ericsson.nms.dg.gen.indexers;

import com.ericsson.nms.dg.gen.LineAccessFile;

/**
 * Indexer interface
 */
public interface Indexer {

  /**
   * Set up the data Indexer
   *
   * @param sampleDataFile (Optional) Data values are to be taken from this file
   */
  void initData(final LineAccessFile sampleDataFile);

  /**
   * Used to tell the Indexer a new ROP is starting
   *
   * @param rowsPerRop The number of rows being generated for the next ROP
   * @param ropId      A rop ID
   */
  void ropStart(final int rowsPerRop, final int ropId);

  /**
   * Used to tell the Indexer a row has been generated
   */
  void rowEnd();

  /**
   * If not using pre-generated data, get a value (runtime-generated/random)
   *
   * @param dataIndex The data index
   * @return Value to write to a ROP file
   */
  String getROPValue(final int dataIndex);

  String getRandomROPValue();

  int getRopCardinalityStartIndex();

  void setRopCardinalityStartIndex(final int startIndex);

  int getRopCardinalityEndIndex();

  void setRopCardinalityEndIndex(final int endIndex);

  int getGeneratedCardinality();

  void setGeneratedCardinality(final int generatedCardinality);

  long getGeneratedCount();

  void setGeneratedCount(final long generatedCount);
}
