package com.ericsson.nms.dg.gen.scratch;

import com.ericsson.nms.dg.gen.ROPFileWriter;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 06/11/13
 * Time: 15:39
 */
public class FileWriteTest {


//  public static void main(String[] args) throws IOException {
//
//    final Properties props = new Properties();
//    props.load(new FileInputStream(args[0]));
//    final String fileSystems = props.getProperty("fileSystems");
//    final int filesToWrite = Integer.valueOf(props.getProperty("filesToWrite"));
//    final int filesInParallel = Integer.valueOf(props.getProperty("filesInParallel"));
//    final int linesToWrite = Integer.valueOf(props.getProperty("linesToWrite"));
//    final String dataLine = props.getProperty("dataLine");
//    final List<String> mounts = new ArrayList<>();
//    final int batchSize = Integer.valueOf(props.getProperty("batchSize"));
//    final int bufferSize = Integer.valueOf(props.getProperty("bufferSize"));
//
//    for (String fs : fileSystems.split(":")) {
//      final File tmp = new File(fs);
//      if (!tmp.exists()) {
//        tmp.mkdirs();
//      }
//      mounts.add(fs.trim());
//    }
//
//    System.out.println("File Systems: " + mounts.toString());
//    System.out.println("File Count: " + filesToWrite);
//    System.out.println("Parallel writes: " + filesInParallel);
//    System.out.println("Lines per file: " + linesToWrite);
//    System.out.println("batchSize: " + batchSize);
//    System.out.println("bufferSize: " + bufferSize);
//
//    threadedData(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts, batchSize);
////    threadedDirect(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts);
////    threadedBytes(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts, 0);
////    threadedBytes(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts, batchSize);
////    threadedBuffered(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts, bufferSize);
////    threadedBatches(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts, batchSize);
//
//  }
//
//  public static void single(final String[] args) throws IOException {
//    final Properties props = new Properties();
//    props.load(new FileInputStream(args[0]));
//
//    final String fileSystems = props.getProperty("fileSystems");
//    final boolean lockMounts = Boolean.valueOf(props.getProperty("lock.mounts"));
//    final int filesToWrite = Integer.valueOf(props.getProperty("filesToWrite"));
//    final int filesInParallel = Integer.valueOf(props.getProperty("filesInParallel"));
//    final int linesToWrite = Integer.valueOf(props.getProperty("linesToWrite"));
//    final String dataLine = props.getProperty("dataLine");
//
//
//    final String writeType = props.getProperty("writeType");
//    final int writeSize = Integer.valueOf(props.getProperty("writeSize"));
//
//
//    final List<String> mounts = new ArrayList<>();
//
//    for (String fs : fileSystems.split(":")) {
//      mounts.add(fs.trim());
//    }
//
//    System.out.println("Mount locking: " + lockMounts);
//    System.out.println("Parallel writes: " + filesInParallel);
//    System.out.println("File Count: " + filesToWrite);
//    System.out.println("Lines per file: " + linesToWrite);
//    System.out.println("writeType: " + writeType);
//    System.out.println("writeSize: " + writeSize);
//    long allStart = System.currentTimeMillis();
//    switch (writeType) {
//      case ("direct"):
//        threadedDirect(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts);
//        break;
//      case ("bytes"):
//        threadedBytes(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts, writeSize);
//        break;
//      case ("buffer"):
//        threadedBuffered(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts, writeSize);
//        break;
//      case ("batch"):
//        threadedBatches(filesInParallel, filesToWrite, dataLine, linesToWrite, mounts, writeSize);
//        break;
//      default:
//        System.err.println("Unknown write type '" + writeType + "'");
//    }
//    long allStop = System.currentTimeMillis();
//    System.out.println(" " + (allStop - allStart) + " msec to write " + filesToWrite);
//  }
//
//  private static void threadedBytes(final int filesInParallel, final int fileCount, final String line, final int lineCount,
//                                    final List<String> mounts, final int batchSize) throws IOException {
//    System.out.println("BYTES-" + batchSize);
//    final ExecutorService es = Executors.newFixedThreadPool(filesInParallel);
//    Iterator<String> mountIterator = mounts.iterator();
//    final AtomicLong totalWriteTime = new AtomicLong(0);
//    final long start = System.currentTimeMillis();
//    for (int i = 0; i < fileCount; i++) {
//      if (!mountIterator.hasNext()) {
//        mountIterator = mounts.iterator();
//      }
//      final String mount = mountIterator.next();
//      final File outputFile = File.createTempFile("bytes_" + batchSize + "_", ".txt", new File(mount));
//      final Runnable task = new Runnable() {
//        @Override
//        public void run() {
//          try {
//            final Map<String, Long> times = writeBytes(outputFile, lineCount, line, batchSize);
//            totalWriteTime.addAndGet(times.get("total_write_time"));
//          } catch (IOException | InterruptedException e) {
//            e.printStackTrace();
//          }
//        }
//      };
//      es.submit(task);
//    }
//    es.shutdown();
//    while (!es.isTerminated()) {
//      try {
//        Thread.sleep(1000);
//      } catch (InterruptedException e) {/**/}
//    }
//    final long totalTime = System.currentTimeMillis() - start;
//    System.out.println("BYTES-" + batchSize + ": Took an average of " + (totalWriteTime.get() / fileCount) + " to write a file");
//    System.out.println("BYTES-" + batchSize + ": Took " + totalTime + " to write " + fileCount + " files in " + filesInParallel + " threads");
//  }
//
//  private static void threadedBatches(final int filesInParallel, final int fileCount, final String line, final int lineCount,
//                                      final List<String> mounts, final int batchSize) throws IOException {
//    System.out.println("BATCH-" + batchSize);
//
//    final ExecutorService es = Executors.newFixedThreadPool(filesInParallel);
//    Iterator<String> mountIterator = mounts.iterator();
//    final AtomicLong totalWriteTime = new AtomicLong(0);
//    final long start = System.currentTimeMillis();
//    for (int i = 0; i < fileCount; i++) {
//      if (!mountIterator.hasNext()) {
//        mountIterator = mounts.iterator();
//      }
//      final String mount = mountIterator.next();
//      final File outputFile = File.createTempFile("batch_" + i + "__" + batchSize + "_", ".txt", new File(mount));
//      final Runnable task = new Runnable() {
//        @Override
//        public void run() {
//          try {
//            final Map<String, Long> times = writeBatches(outputFile, lineCount, line, batchSize);
//            totalWriteTime.addAndGet(times.get("total_write_time"));
//          } catch (IOException | InterruptedException e) {
//            e.printStackTrace();
//          }
//        }
//      };
//      es.submit(task);
//    }
//    es.shutdown();
//    while (!es.isTerminated()) {
//      try {
//        Thread.sleep(1000);
//      } catch (InterruptedException e) {/**/}
//    }
//    final long totalTime = System.currentTimeMillis() - start;
//    System.out.println("BATCH-" + batchSize + ": Took an average of " + (totalWriteTime.get() / fileCount) + " to write a file");
//    System.out.println("BATCH-" + batchSize + ": Took " + totalTime + " to write " + fileCount + " files in " + filesInParallel + " threads");
//  }
//
//  private static void threadedData(final int filesInParallel, final int fileCount, final String line, final int lineCount,
//                                   final List<String> mounts, final int batchSize) throws IOException {
//    System.out.println("DATA-" + batchSize);
//    final ExecutorService es = Executors.newFixedThreadPool(filesInParallel);
//    Iterator<String> mountIterator = mounts.iterator();
//    final AtomicLong totalWriteTime = new AtomicLong(0);
//    final long start = System.currentTimeMillis();
//    for (int i = 0; i < fileCount; i++) {
//      if (!mountIterator.hasNext()) {
//        mountIterator = mounts.iterator();
//      }
//      final String mount = mountIterator.next();
//      final File outputFile = File.createTempFile("data_" + i + "__" + batchSize + "_", ".txt", new File(mount));
//      final Runnable task = new Runnable() {
//        @Override
//        public void run() {
//          try {
//            final Map<String, Long> times = writeData(outputFile, lineCount, line, batchSize);
//            totalWriteTime.addAndGet(times.get("total_write_time"));
//          } catch (IOException e) {
//            e.printStackTrace();
//          }
//        }
//      };
//      es.submit(task);
//    }
//    es.shutdown();
//    while (!es.isTerminated()) {
//      try {
//        Thread.sleep(1000);
//      } catch (InterruptedException e) {/**/}
//    }
//    final long totalTime = System.currentTimeMillis() - start;
//    System.out.println("DATA-" + batchSize + ": Took an average of " + (totalWriteTime.get() / fileCount) + " to write a file");
//    System.out.println("DATA-" + batchSize + ": Took " + totalTime + " to write " + fileCount + " files in " + filesInParallel + " threads");
//  }
//
//  private static void threadedBuffered(final int filesInParallel, final int fileCount, final String line, final int lineCount,
//                                       final List<String> mounts, final int bufferSize) throws IOException {
//    System.out.println("BUFFERED-" + bufferSize);
//    Iterator<String> mountIterator = mounts.iterator();
//    final AtomicLong totalWriteTime = new AtomicLong(0);
//    final ExecutorService executor = Executors.newFixedThreadPool(filesInParallel);
//    final long start = System.currentTimeMillis();
//    for (int i = 0; i < fileCount; i++) {
//      if (!mountIterator.hasNext()) {
//        mountIterator = mounts.iterator();
//      }
//      final String mount = mountIterator.next();
//      final File outputFile = File.createTempFile("buffered_" + bufferSize + "_", ".txt", new File(mount));
//      executor.submit(
//          new Runnable() {
//            @Override
//            public void run() {
//              try {
//                final Map<String, Long> times = writeBuffered(outputFile, lineCount, line, bufferSize);
//                totalWriteTime.addAndGet(times.get("total_write_time"));
//              } catch (Throwable e) {
//                e.printStackTrace();
//              }
//            }
//          });
//    }
//    executor.shutdown();
//    while (!executor.isTerminated()) {
//      try {
//        Thread.sleep(1000);
//      } catch (InterruptedException e) {/**/}
//    }
//    final long totalTime = System.currentTimeMillis() - start;
//    System.out.println("BUFFERED-" + bufferSize + ": Took an average of " + (totalWriteTime.get() / fileCount) + " to write " + fileCount + " a file");
//    System.out.println("BUFFERED-" + bufferSize + ": Took " + totalTime + " to write " + fileCount + " files in " + filesInParallel + " threads");
//  }
//
//  private static void threadedDirect(final int filesInParallel, final int fileCount, final String line, final int lineCount, final List<String> mounts) throws IOException {
//    System.out.println("DIRECT");
//
//    final ExecutorService es = Executors.newFixedThreadPool(filesInParallel);
//    Iterator<String> mountIterator = mounts.iterator();
//    final AtomicLong totalWriteTime = new AtomicLong(0);
//    final long start = System.currentTimeMillis();
//    for (int i = 0; i < fileCount; i++) {
//      if (!mountIterator.hasNext()) {
//        mountIterator = mounts.iterator();
//      }
//      final String mount = mountIterator.next();
//      final File outputFile = File.createTempFile("direct_", ".txt", new File(mount));
//      final Runnable task = new Runnable() {
//        @Override
//        public void run() {
//          try {
//            final Map<String, Long> times = writeDirect(outputFile, lineCount, line);
//            totalWriteTime.addAndGet(times.get("total_write_time"));
//          } catch (IOException | InterruptedException e) {
//            e.printStackTrace();
//          }
//        }
//      };
//      es.submit(task);
//    }
//    es.shutdown();
//    while (!es.isTerminated()) {
//      try {
//        Thread.sleep(1000);
//      } catch (InterruptedException e) {/**/}
//    }
//    final long totalTime = System.currentTimeMillis() - start;
//    System.out.println("DIRECT-THREAD: Took an average of " + (totalWriteTime.get() / fileCount) + " to write a file");
//    System.out.println("DIRECT-THREAD: Took " + totalTime + " to write " + fileCount + " files in " + filesInParallel + " threads");
//  }
//
//  private static Map<String, Long> writeDirect(final File outputFile, final int lineCount, final String line) throws IOException, InterruptedException {
//    long start = System.currentTimeMillis();
//    System.out.println("\tWriting to file " + outputFile.getAbsolutePath());
//    ROPFileWriter fileWriter = null;
//    try {
//      fileWriter = new ROPFileWriter(outputFile, -1, lineCount, ROPFileWriter.MODE.DIRECT, -1, -1);
//      for (int i = 0; i < lineCount; i++) {
//        fileWriter.write(line + "\n");
//      }
//      final long taken = System.currentTimeMillis() - start;
//      System.out.println("\tTook " + taken + "mSec to write " + lineCount + " lines to " + fileWriter.getAbsolutePaths());
//      final Map<String, Long> times = new HashMap<>();
//      times.put("total_write_time", taken);
//      return times;
//    } finally {
//      if (fileWriter != null) {
//        fileWriter.close();
//      }
//    }
//  }
//
//  private static Map<String, Long> writeBatches(final File outputFile, final int lineCount,
//                                                final String line, final int batchSize) throws IOException, InterruptedException {
//    long start = System.currentTimeMillis();
//    System.out.println("\tWriting to file " + outputFile.getAbsolutePath());
//    ROPFileWriter fileWriter = null;
//    try {
//      fileWriter = new ROPFileWriter(outputFile, -1, lineCount, ROPFileWriter.MODE.BATCH, -1, batchSize);
//      for (int i = 0; i < lineCount; i++) {
//        fileWriter.write(line + "\n");
//      }
//      final long taken = System.currentTimeMillis() - start;
//      System.out.println("\tTook " + taken + "mSec to write " + lineCount + " lines to " + fileWriter.getAbsolutePaths());
//      final Map<String, Long> times = new HashMap<>();
//      times.put("total_write_time", taken);
//      return times;
//    } finally {
//      if (fileWriter != null) {
//        fileWriter.close();
//      }
//    }
//  }
//
//  private static Map<String, Long> writeBuffered(final File outputFile, final int lineCount,
//                                                 final String line, final int bufferSize) throws IOException, InterruptedException {
//    long start = System.currentTimeMillis();
//    System.out.println("\tWriting to file " + outputFile.getAbsolutePath());
//    ROPFileWriter fileWriter = null;
//    try {
//      fileWriter = new ROPFileWriter(outputFile, -1, lineCount, ROPFileWriter.MODE.BUFFER, bufferSize, -1);
//      for (int i = 0; i < lineCount; i++) {
//        fileWriter.write(line + "\n");
//      }
//      final long taken = System.currentTimeMillis() - start;
//      System.out.println("\tTook " + taken + "mSec to write " + lineCount + " lines to " + fileWriter.getAbsolutePaths());
//      final Map<String, Long> times = new HashMap<>();
//      times.put("total_write_time", taken);
//      return times;
//    } finally {
//      if (fileWriter != null) {
//        fileWriter.close();
//      }
//    }
//  }
//
//  private static Map<String, Long> writeBytes(final File outputFile, final int lineCount,
//                                              final String line, final int batchSize) throws IOException, InterruptedException {
//    long start = System.currentTimeMillis();
//    System.out.println("\tWriting to file " + outputFile.getAbsolutePath());
//    ROPFileWriter fileWriter = null;
//    try {
//      fileWriter = new ROPFileWriter(outputFile, -1, lineCount, ROPFileWriter.MODE.RAW_BYTE, -1, batchSize);
//      for (int i = 0; i < lineCount; i++) {
//        fileWriter.write(line + "\n");
//      }
//      final long taken = System.currentTimeMillis() - start;
//      System.out.println("\tTook " + taken + "mSec to write " + lineCount + " lines to " + fileWriter.getAbsolutePaths());
//      final Map<String, Long> times = new HashMap<>();
//      times.put("total_write_time", taken);
//      return times;
//    } finally {
//      if (fileWriter != null) {
//        fileWriter.close();
//      }
//    }
//  }
//
//  private static Map<String, Long> writeData(final File outputFile, final int lineCount, final String line,
//                                             final int batchSize) throws IOException {
//    long start = System.currentTimeMillis();
//    System.out.println("\tWriting to file " + outputFile.getAbsolutePath());
//    final List<String> batches = new ArrayList<>(batchSize);
//    try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(outputFile, false))) {
//      for (int i = 0; i < lineCount; i++) {
//        batches.add(line + "\n");
//        if (batches.size() > batchSize) {
//          System.out.println("W->");
//          final StringBuilder sb = new StringBuilder();
//          for (String s : batches) {
//            sb.append(s);
//          }
//          dos.writeBytes(sb.toString());
//          System.out.println("W-<");
//          batches.clear();
//        }
//      }
//      final long taken = System.currentTimeMillis() - start;
//      System.out.println("\tTook " + taken + "mSec to write " + lineCount + " lines to " + outputFile.getAbsolutePath());
//      final Map<String, Long> times = new HashMap<>();
//      times.put("total_write_time", taken);
//      return times;
//    }
//  }
}