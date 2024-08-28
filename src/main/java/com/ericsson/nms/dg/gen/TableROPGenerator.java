package com.ericsson.nms.dg.gen;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.ddl.Util;
import com.ericsson.nms.dg.gen.indexers.Indexer;
import com.ericsson.nms.dg.schema.ColumnType;
import com.ericsson.nms.dg.schema.TableType;
import org.apache.commons.configuration.Configuration;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 25/10/13
 * Time: 13:43
 */
public class TableROPGenerator {
  private final int rowsPerROP;
  private final int maxRowsPerROPFile;
  private final String tableID;
  private final LinkedHashMap<String, Indexer> orderedDataIndexers = new LinkedHashMap<>();
  private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'kk-mm-ss-SSS");
  private final Configuration ropgenConfig;
  private long averageGenerateTime = 0;
  private long totalGenerateTime = 0;
  private int numGenerated = 0;
  private long fileTotalWriteTime = 0;
  private long lastWriteTime = 0;
  private long fileWriteCount = 0;
  private long fileROPWriteTime = 0;
  private int rowGenerateCount = 0;
  private boolean ropCompleted = false;
  private List<String> columnHeaders = null;
  private List<Boolean> wrappers = null;
  private long bytesWritten = 0;
  private ROPFileWriter fileWriter = null;
  private File outputFile = null;
  private Map<String, Integer> generatedFiles = null;

  public TableROPGenerator(final TableType tableDef, final int tableInstance, final int rowsPerROP,
                           final Map<String, Indexer> _dataIndexers, final Configuration ropgenConfig,
                           final Configuration dbConfig) {
    assert tableDef != null : "TableType instance can't be NULL";
    assert _dataIndexers != null : "Data Indexers can't be NULL";
    assert ropgenConfig != null : "Configuration can't be NULL";
    assert dbConfig != null : "Database can't be NULL";
    this.ropgenConfig = ropgenConfig;
    this.wrappers = new ArrayList<>();
    this.rowsPerROP = rowsPerROP;
    this.maxRowsPerROPFile = dbConfig.getInt(tableDef.getId() + ".max_rows_per_file", -1);
    this.tableID = Util.generateName(tableDef, tableInstance);
    final boolean includeHeaders = Boolean.valueOf(this.ropgenConfig.getString("rop.includeheaders", "false"));
    if (includeHeaders) {
      columnHeaders = new ArrayList<>();
    }
    for (ColumnType tableColumn : tableDef.getColumn()) {
      for (int columnInstance = 1; columnInstance <= tableColumn.getNColumns(); columnInstance++) {
        if (includeHeaders) {
          columnHeaders.add(Util.getColumnDataID(tableColumn, columnInstance));
        }
        switch (tableColumn.getDataType().getType()) {
          case "varchar":
          case "DATETIME":
            wrappers.add(true);
            break;
          default:
            wrappers.add(false);
        }
        final String handlerID = Util.getTableColumnDataID(tableDef, tableInstance, tableColumn, columnInstance);
        if (!_dataIndexers.containsKey(handlerID)) {
          throw new GenerateException(new NoSuchElementException(handlerID));
        }
        orderedDataIndexers.put(handlerID, _dataIndexers.get(handlerID));
      }
    }
  }

  public boolean isRopCompleted() {
    return this.ropCompleted;
  }

  @Override
  public String toString() {
    return Thread.currentThread().getName() + "-" + this.tableID;
  }

  public void generate(final File outputDir) throws IOException {
    final String time = dateFormatter.format(new Date(System.currentTimeMillis()));
    final String ropFilename = this.tableID + "_" + time + ".csv";
    if (!outputDir.exists()) {
      throw new GenerateException(new IOException("Directory " + outputDir + " does not exist"));
    }
    for (Map.Entry<String, Indexer> indexer : this.orderedDataIndexers.entrySet()) {
      indexer.getValue().ropStart(this.rowsPerROP, this.numGenerated);
    }
    final File ropFile = new File(outputDir, ropFilename);
    System.out.println("TRG: " + this + " generating " + this.rowsPerROP + " rows to file " + ropFile.getAbsoluteFile());
    System.out.println("\tTRG: " + this + " Max rows per file: " + this.maxRowsPerROPFile);
    this.ropCompleted = false;
    final long start = System.currentTimeMillis();
    generateRopFiles(ropFile);
    final long end = System.currentTimeMillis();
    this.ropCompleted = true;
    this.numGenerated++;
    final long genTime = end - start;
    this.totalGenerateTime += genTime;
    this.averageGenerateTime = this.totalGenerateTime / this.numGenerated;
    final double bps = bytesWritten / genTime;
    System.out.println("TRG: " + this + " took " + genTime + "msec to generate " + this.rowsPerROP + " rows (" +
        orderedDataIndexers.size() + " columns per row, bytes: " + bytesWritten + ", " + bps + " B/ms) to " + ropFile.getAbsolutePath());
    System.out.println("\tTRG: " + this + " Average of " + averageGenerateTime + "msec (" + numGenerated + ")");
  }

  public int getRowsPerROP() {
    return this.rowsPerROP;
  }

  public long getTotalGenerateTime() {
    return this.totalGenerateTime;
  }

  public long getAverageGenerateTime() {
    return this.averageGenerateTime;
  }

  public int getNumGenerated() {
    return this.numGenerated;
  }

  public long getTotalFileWriteTime() {
    return this.fileTotalWriteTime;
  }

  public long getLastFileWriteTime() {
    return this.lastWriteTime;
  }

  public long getTotalFileIOWriteCount() {
    return this.fileWriteCount;
  }

  public long getROPFileWriteTime() {
    return this.fileROPWriteTime;
  }

  public int getRowGenerateCount() {
    return this.rowGenerateCount;
  }

  private void writeRowToFile(final List<String> row) throws IOException, InterruptedException {
    if (this.fileWriter == null) {
      this.fileWriter = getWriter(outputFile);
      System.out.println(this + " writer " + this.fileWriter.toString());
    }
    this.fileWriteCount++;
    long a = System.currentTimeMillis();
    fileWriter.write(row, wrappers);
    lastWriteTime = System.currentTimeMillis() - a;
    fileROPWriteTime += lastWriteTime;
    fileTotalWriteTime += lastWriteTime;
  }

  public String getTableName() {
    return this.tableID;
  }

  private void writeRowsToFile(final List<List<String>> rows) throws IOException, InterruptedException {
    for (List<String> row : rows) {
      writeRowToFile(row);
    }
    rows.clear();
  }

  private ROPFileWriter getWriter(final File outputFile) throws IOException, InterruptedException {

    final String key_writeType = "rop.writer.type";
    final String key_batchSize = "rop.writer.batch.size";
    final String key_bufferSize = "rop.writer.buffer.size";

    final String writeType = this.ropgenConfig.getString(key_writeType);
    int batchSize = -1;
    int bufferSize = -1;
    final ROPFileWriter.MODE mode;
    switch (writeType) {
      case "direct":
        mode = ROPFileWriter.MODE.DIRECT;
        break;
      case "buffer":
        mode = ROPFileWriter.MODE.BUFFER;
        bufferSize = this.ropgenConfig.getInt(key_bufferSize);
        batchSize = this.ropgenConfig.getInt(key_batchSize, -1);
        break;
      case "boutput":
        mode = ROPFileWriter.MODE.BOUTPUT;
        bufferSize = this.ropgenConfig.getInt(key_bufferSize);
        break;
      case "raw_byte":
        mode = ROPFileWriter.MODE.RAW_BYTE;
        batchSize = this.ropgenConfig.getInt(key_batchSize, -1);
        break;
      case "batch":
        batchSize = this.ropgenConfig.getInt(key_batchSize);
        mode = ROPFileWriter.MODE.BATCH;
        break;
      default:
        throw new UnsupportedOperationException("Dont know how to build writers of type '" + writeType + "'");
    }
    return new ROPFileWriter(outputFile, this.maxRowsPerROPFile, this.rowsPerROP, mode, bufferSize, batchSize);
  }

  private void generateRopFiles(final File ropFile) throws IOException {
    fileROPWriteTime = 0;
    this.outputFile = ropFile;
    final List<List<String>> rows = new ArrayList<>(this.rowsPerROP);
    try {
      if (columnHeaders != null) {
        writeRowToFile(columnHeaders);
      }
      for (rowGenerateCount = 0; rowGenerateCount < this.rowsPerROP; rowGenerateCount++) {
        final List<String> cells = new ArrayList<>(this.orderedDataIndexers.size());
        for (Map.Entry<String, Indexer> rowIndexer : this.orderedDataIndexers.entrySet()) {
          final String value = rowIndexer.getValue().getRandomROPValue();
          cells.add(value);
          rowIndexer.getValue().rowEnd();
        }
        rows.add(cells);
        writeRowsToFile(rows);
      }
      writeRowsToFile(rows);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      try {
        this.fileWriter.close();
      } catch (Throwable e) {/**/}
    }
    this.bytesWritten = this.fileWriter.getBytesWritten();
    this.generatedFiles = this.fileWriter.getAbsolutePaths();
    this.fileWriter = null;
  }

  public Map<String, Integer> getGeneratedFiles() {
    return this.generatedFiles;
  }
}
