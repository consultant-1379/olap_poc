package com.ericsson.nms.dg.gen;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.ddl.Util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 24/10/13
 * Time: 14:55
 */
public class LineAccessFile implements AutoCloseable {
  private final List<Long> lineStartPositions;
  private final Semaphore fileLock = new Semaphore(1, true);
  private final File dataFile;
  private final RandomAccessFile fileAccess;
  private final String lafId;
  private Map<Integer, String> cachedValues = null;
  private int cacheMisses = 0;
  private int cacheHits = 0;
  private int totalReads = 0;
  private long totalLockAquireWaitTime = 0;


  public LineAccessFile(final String id, final File dataFile, final boolean cacheReads) throws GenerateException {
    this.lafId = id;
    this.dataFile = dataFile;
    try {
      this.fileAccess = new RandomAccessFile(this.dataFile, "r");
    } catch (FileNotFoundException e) {
      throw new GenerateException(e);
    }
    this.lineStartPositions = Util.readLineNumbers(dataFile.getAbsolutePath());
    if (cacheReads) {
      cachedValues = new HashMap<>(getLineCount());
    }
  }

  public String getId() {
    return this.lafId;
  }

  public void precacheData() {
    System.out.println("Precaching data from " + this.dataFile.getAbsolutePath());
    if (this.cachedValues == null) {
      cachedValues = new HashMap<>(getLineCount());
    }
    String line;
    int lineNumber = 1;
    try {
      while ((line = this.fileAccess.readLine()) != null) {
        this.cachedValues.put(lineNumber, line);
        lineNumber++;
      }
    } catch (IOException e) {
      throw new GenerateException("Failed to precache " + this.dataFile.getAbsolutePath(), e);
    }
  }

  public boolean cacheEnabled() {
    return this.cachedValues != null;
  }

  public String readLine(final int lineNumber) throws IOException, InterruptedException {
    this.totalReads++;
    if (cachedValues != null) {
      final String value = cachedValues.get(lineNumber);
      if (value != null) {
        this.cacheHits++;
        return value;
      }
    }
    this.cacheMisses++;
    // Get the byte the line starts at
    final long bytePosition = this.lineStartPositions.get(lineNumber - 1);
    final long mark = System.currentTimeMillis();
    // Try to get a lock on the file (so we're the only ones reading it)
    this.fileLock.tryAcquire(10, TimeUnit.SECONDS);
    this.totalLockAquireWaitTime += System.currentTimeMillis() - mark;
    try {
      // Jump to the line and read it
      this.fileAccess.seek(bytePosition);
      final String value = this.fileAccess.readLine();
      // Cache it if required
      if (this.cachedValues != null) {
        this.cachedValues.put(lineNumber, value);
      }
      return value;
    } finally {
      // Release the lock on the file
      this.fileLock.release();
    }
  }

  public void close() throws IOException {
    this.fileAccess.close();
  }

  public int getLineCount() {
    return this.lineStartPositions.size();
  }

  public int getCacheHits() {
    return this.cacheHits;
  }

  public int getCacheMisses() {
    return this.cacheMisses;
  }

  public int getTotalReads() {
    return this.totalReads;
  }

  public long getTotalLockAquireWaitTime() {
    return this.totalLockAquireWaitTime;
  }

  @Override
  public String toString() {
    final StringBuilder toString = new StringBuilder();
    toString.append("File:").append(getAbsolutePath()).append("\n");
    toString.append("\tTotal reads: ").append(this.totalReads).append("\n");
    toString.append("\tCache hits: ").append(this.cacheHits).append("\n");
    toString.append("\tLock aquire wait time: ").append(this.totalLockAquireWaitTime);
    return toString.toString();
  }

  public String getAbsolutePath() {
    return this.dataFile.getAbsolutePath();
  }
}
