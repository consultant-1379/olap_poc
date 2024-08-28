package com.ericsson.nms.dg.gen;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class RopTask implements Runnable {
  private final String ID;
  private final TableROPGenerator runner;
  private File outputDir = null;
  private boolean isExecuting = false;
  private Throwable error = null;

  RopTask(final String id, final TableROPGenerator runner) {
    this.ID = id;
    this.runner = runner;
  }

  public boolean isExecuting() {
    return isExecuting;
  }

  void setOutputDir(final File outputDir) {
    this.outputDir = outputDir;
  }

  public String getId() {
    return ID;
  }

  @Override
  public void run() {
    isExecuting = true;
    try {
      runner.generate(outputDir);
    } catch (Throwable e) {
      this.error = e;
      this.error.printStackTrace();
    } finally {
      isExecuting = false;
    }
  }

  public Map<String, Integer> getGeneratedFiles() {
    Map<String, Integer> files = runner.getGeneratedFiles();
    if (files == null) {
      files = new HashMap<>(0);
    }
    return files;
  }

  public Throwable getError() {
    return this.error;
  }

  public String getGeneratedTable() {
    return runner.getTableName();
  }
}