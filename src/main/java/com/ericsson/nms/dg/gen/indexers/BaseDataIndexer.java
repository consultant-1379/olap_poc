package com.ericsson.nms.dg.gen.indexers;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.gen.LineAccessFile;
import com.ericsson.nms.dg.schema.DataTypeType;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;

/**
 * Class to handle the generation of ROP files so that cardinality is fulfilled based on the input DB xml
 */
public abstract class BaseDataIndexer implements Indexer {
  /**
   * Logger ...
   */
  protected final Logger logger = LogManager.getLogger("BaseDataIndexer");
  /**
   * Unique identifier for the handler
   */
  private final String id;
  /**
   * The datatype this indexer is handling
   */
  private final DataTypeType datatypeModel;
  /**
   * The start index of the next set of unique values to be used for the current ROP.
   */
  private int ropCardinalityStartIndex = 0;
  /**
   * The end index of the next set of unique values to be used for the current ROP.
   * <p/>
   * data-new-val == ropCardinalityEndIndex - ropCardinalityStartIndex
   */
  private int ropCardinalityEndIndex = 0;
  /**
   * The cardinality of a data type in the generated ROP files
   */
  private int generatedCardinality = 0;
  /**
   * Internal
   */
  private long generatedCount = 0;
  /**
   * Once |C| has been fulfilled in DB/ROPfile and random value can be used.
   */
  private Random randomIndex = null;
  /**
   * Used to keep track of the row count while generating ROP data
   */
  private int rowsGeneratedThisROP = 0;
  /**
   * An indexer can be backed by a file containing data, RandomAccessFile is used
   * to get values from that file
   */
  private LineAccessFile dataSourceFile = null;

  /**
   * Indexer object
   *
   * @param id        Unique ID for this data indexer
   * @param typeModel The data type being tracked
   */
  public BaseDataIndexer(final String id, final DataTypeType typeModel) {
    this.id = id;
    this.datatypeModel = typeModel;
  }

  /**
   * Get the data type being tracked
   *
   * @return The data type being tracked
   */
  protected DataTypeType getDataType() {
    return this.datatypeModel;
  }

  /**
   * Get this indexers ID
   *
   * @return This indexers ID
   */
  public String getId() {
    return this.id;
  }

  /**
   * A row was generated, next call with be for another row
   */
  public void rowEnd() {
    this.rowsGeneratedThisROP++;
  }

  /**
   * Log stuff
   *
   * @param level   The level to log at
   * @param message The message to log
   */
  protected void log(final Level level, final String message) {
    if (logger.isEnabled(level)) {
      logger.log(level, getId() + " -- " + message);
    }
  }

  /**
   * Initialize the data indexer.
   * If <code>sampleDataFile</code> is null, the subclass is expected to generate a value.
   * If <code>sampleDataFile</code> is not null then this indexer is reading values from that file i.e. pre-generated data
   *
   * @param sampleDataFile (Optional) The pre-generated data file.
   */
  @Override
  public void initData(final LineAccessFile sampleDataFile) {
    dataSourceFile = sampleDataFile;
  }

  /**
   * A new ROP is starting.
   *
   * @param rowsPerRop The number of rows being generated in this ROP
   * @param ropId      a ROP count
   */
  public void ropStart(final int rowsPerRop, final int ropId) {
    this.randomIndex = new Random();
    this.ropCardinalityStartIndex = this.ropCardinalityEndIndex;
    if (getDataType().getDataNewVal() == 0) {
      this.ropCardinalityEndIndex = getDataType().getDataCardinality();
    } else {
      this.ropCardinalityEndIndex += getDataType().getDataNewVal() - 1;
    }
    this.rowsGeneratedThisROP = 0;
  }

  /**
   * Get a value that conforms to the DB xml rules
   *
   * @return Valiue to be written to a ROP file
   */
  public String getRandomROPValue() {
    int dataIndex;
    if (getDataType().getDataNewVal() == 0 || this.rowsGeneratedThisROP < getDataType().getDataNewVal()) {
      if (this.generatedCardinality < getDataType().getDataCardinality()) {
        dataIndex = ++this.generatedCardinality;
      } else {
        dataIndex = (int) (this.generatedCount % getDataType().getDataCardinality());
      }
      this.generatedCount++;
    } else {
      try {
        final int range = this.ropCardinalityEndIndex > getDataType().getDataCardinality() ?
            getDataType().getDataCardinality() :
            this.ropCardinalityEndIndex;
        dataIndex = randomIndex.nextInt(range + 1);
      } catch (Throwable e) {
        e.printStackTrace();
        throw e;
      }
    }
    if (dataIndex == 0) {
      dataIndex = 1;
    }
    final String value;
    if (dataSourceFile == null) {
      value = getROPValue(dataIndex);
    } else {
      value = readFromFile(dataIndex);
    }
    return value;
  }

  public String readFromFile(int lineNumber) {
    try {
      if (lineNumber < 0) {
        System.out.println(getId() + "**** Negative line number for " + this.dataSourceFile.getAbsolutePath() + "*****");
        lineNumber = 0;
      }
      return this.dataSourceFile.readLine(lineNumber);
    } catch (Throwable e) {
      final String msg = getId() + " failed to seek to line " + lineNumber + " from file " +
          this.dataSourceFile.getAbsolutePath();
      System.out.println(msg);
      throw new GenerateException(msg, e);
    }
  }

  public int getRopCardinalityStartIndex() {
    return this.ropCardinalityStartIndex;
  }

  public void setRopCardinalityStartIndex(final int ropCardinalityStartIndex) {
    this.ropCardinalityStartIndex = ropCardinalityStartIndex;
  }

  public int getRopCardinalityEndIndex() {
    return this.ropCardinalityEndIndex;
  }

  public void setRopCardinalityEndIndex(final int ropCardinalityEndIndex) {
    this.ropCardinalityEndIndex = ropCardinalityEndIndex;
  }

  public int getGeneratedCardinality() {
    return this.generatedCardinality;
  }

  public void setGeneratedCardinality(final int generatedCardinality) {
    this.generatedCardinality = generatedCardinality;
  }

  public long getGeneratedCount() {
    return this.generatedCount;
  }

  public void setGeneratedCount(final long generatedCount) {
    this.generatedCount = generatedCount;
  }
}
