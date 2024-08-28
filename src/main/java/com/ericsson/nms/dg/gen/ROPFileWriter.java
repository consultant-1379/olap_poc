package com.ericsson.nms.dg.gen;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: eeipca
 * Date: 11/11/13
 * Time: 12:23
 */
public class ROPFileWriter implements AutoCloseable {
  private final MODE mode;
  private final String baseFileName;
  private final Map<String, Integer> writtenFiles = new HashMap<>();
  private Writer fileWriter = null;
  private OutputStream outputStream = null;
  private RandomAccessFile byteWriter = null;
  private List<List<String>> batches = null;
  private int batchSize = -1;
  private int bufferSize = -1;
  private long bytesWritten = 0;
  private int maxRowsPerFile = -1;
  private int totalRowsPerROP = -1;
  private int linesWritten = 0;
  private int fileCount = 1;
  private String currentFile = null;

  public ROPFileWriter(final File _filePath, final int maxRowsPerFile, final int maxTotalRows,
                       final MODE mode, final int bufferSize,
                       final int batchSize) throws IOException, InterruptedException {
    this.mode = mode;
    this.maxRowsPerFile = maxRowsPerFile;
    this.totalRowsPerROP = maxTotalRows;
    this.baseFileName = _filePath.getAbsolutePath();
    this.batchSize = batchSize;
    this.bufferSize = bufferSize;
    openFile();
  }

  private void openFile() throws IOException {
    close();
    currentFile = this.baseFileName;
    if (this.maxRowsPerFile != -1 && this.totalRowsPerROP > this.maxRowsPerFile) {
      currentFile = this.baseFileName + "." + (this.fileCount++);
    }
    System.out.println("Opening file " + this.currentFile);
    if (mode == MODE.DIRECT) {
      this.fileWriter = new FileWriter(currentFile, false);
    } else if (mode == MODE.BOUTPUT) {
      if (this.bufferSize <= 0) {
        this.bufferSize = 8192;
      }
      this.outputStream = new BufferedOutputStream(new FileOutputStream(currentFile, false), bufferSize);
    } else if (mode == MODE.BUFFER) {
      if (this.bufferSize <= 0) {
        this.bufferSize = 8192;
      }
      this.fileWriter = new BufferedWriter(new FileWriter(currentFile, false), bufferSize);
      if (this.batchSize > 0) {
        this.batches = new ArrayList<>(this.batchSize);
      }
    } else if (mode == MODE.RAW_BYTE) {
      this.byteWriter = new RandomAccessFile(currentFile, "rw");
      if (this.batchSize > 0) {
        this.batches = new ArrayList<>(this.batchSize);
      }
    } else if (mode == MODE.BATCH) {
      this.fileWriter = new FileWriter(currentFile, false);
      this.batches = new ArrayList<>(this.batchSize);
    } else {
      throw new EnumConstantNotPresentException(MODE.class, mode.toString());
    }
  }

  public long getBytesWritten() {
    return this.bytesWritten;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Mode{").append(this.mode.name()).append("}");
    sb.append(", BufferSize{").append(bufferSize).append("}");
    sb.append(", BatchSize{").append(batchSize).append("}");
    sb.append(", File{").append(getAbsolutePaths()).append("}");
    return sb.toString();
  }

  public String getType() {
    return mode.name();
  }

  private void writeRowToFile(final List<String> rowCells, final List<Boolean> wrapList) throws IOException {
    linesWritten++;
    final int sepSize = ",".getBytes().length;
    final int nlSize = "\n".getBytes().length;
    final Iterator<String> cellIterator = rowCells.iterator();
    final Iterator<Boolean> wrapInterator = wrapList.iterator();
    while (cellIterator.hasNext()) {
      final String cellValue = cellIterator.next();
      final boolean wrap = wrapInterator.next();
      bytesWritten += cellValue.length();
      if (this.fileWriter != null) {
        if (wrap) {
          this.fileWriter.write('\'');
        }
        this.fileWriter.write(cellValue);
        if (wrap) {
          this.fileWriter.write('\'');
        }
        if (cellIterator.hasNext()) {
          this.fileWriter.write(',');
          bytesWritten += sepSize;
        }
      } else if (this.byteWriter != null) {
        if (wrap) {
          this.byteWriter.write('\'');
        }
        this.byteWriter.write(cellValue.getBytes("UTF-8"));
        if (wrap) {
          this.byteWriter.write('\'');
        }
        if (cellIterator.hasNext()) {
          this.byteWriter.write(',');
          bytesWritten += sepSize;
        }
      } else if (this.outputStream != null) {
        if (wrap) {
          this.outputStream.write('\'');
        }
        for (char c : cellValue.toCharArray()) {
          this.outputStream.write(c);
        }
        if (wrap) {
          this.outputStream.write('\'');
        }
        if (cellIterator.hasNext()) {
          this.outputStream.write(',');
          bytesWritten += sepSize;
        }
      }
    }
    bytesWritten += nlSize;
    if (this.fileWriter != null) {
      this.fileWriter.write('\n');
    } else if (this.byteWriter != null) {
      this.byteWriter.write('\n');
    } else if (this.outputStream != null) {
      this.outputStream.write('\n');
    }
    if (this.maxRowsPerFile != -1 && this.linesWritten >= this.maxRowsPerFile) {
      openFile();
    }
  }

  private void writeRowsToFile(final List<List<String>> rows, final List<Boolean> wrap) throws IOException {
    if (rows == null || rows.isEmpty()) {
      return;
    }
    for (List<String> rowData : rows) {
      writeRowToFile(rowData, wrap);
    }
  }

  public void write(final List<String> rowCells, final List<Boolean> wrap) throws InterruptedException, IOException {
    if (batches == null) {
      writeRowToFile(rowCells, wrap);
    } else {
      batches.add(rowCells);
      if (this.batches.size() >= this.batchSize) {
        writeRowsToFile(this.batches, wrap);
        this.batches.clear();
      }
    }
  }

  @Override
  public void close() throws IOException {
    writeRowsToFile(this.batches, null);
    if (this.linesWritten > 0) {
      this.writtenFiles.put(this.currentFile, this.linesWritten);
    }
    if (this.outputStream != null) {
      this.outputStream.close();
      this.outputStream = null;
    }
    if (this.fileWriter != null) {
      this.fileWriter.close();
      this.fileWriter = null;
    }
    if (this.byteWriter != null) {
      this.byteWriter.close();
      this.byteWriter = null;
    }
    if (this.linesWritten == 0 && this.currentFile != null) {
      final File f = new File(this.currentFile);
      if (f.exists() && !f.delete()) {
        System.out.println("");
      }
    }
    if (this.currentFile != null) {
      System.out.println("Closed file " + this.currentFile);
    }
    this.linesWritten = 0;
  }

  public Map<String, Integer> getAbsolutePaths() {
    return Collections.unmodifiableMap(this.writtenFiles);
  }

  public static enum MODE {
    DIRECT, BUFFER, BATCH, RAW_BYTE, BOUTPUT
  }
}
