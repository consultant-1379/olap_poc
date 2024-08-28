package com.ericsson.nms.dg.ddlapply;

import com.ericsson.nms.dg.DDLApplyException;
import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.ddl.DDLWriter;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 06/12/13
 * Time: 09:03
 * To change this template use File | Settings | File Templates.
 */
public class ApplyAggregation extends ApplyDDLInDatabase {

  private static String AGG_SQL_CMD_FILE_LOC = "";
  private static int NARROW_TABLE_COUNT=20;
  private static int MEDIUM_TABLE_COUNT=80;
  private static int WIDE_TABLE_COUNT=10;
  private static String dbNAME = "exa";

  /**
   * @param ds
   */
  public void processDDLCommands(DataSource ds) {
    final File[] testFiles = new File(AGG_SQL_CMD_FILE_LOC).listFiles();



    Arrays.sort(testFiles, new Comparator<File>() {
      public int compare(File f1, File f2) {
        return Long.valueOf(f1.lastModified()).compareTo(
            f2.lastModified());
      }
    });


    List<DBWorkerThread> narrowAggCmdList = new ArrayList<>();
    List<DBWorkerThread> mediumAggCmdList = new ArrayList<>();
    List<DBWorkerThread> wideAggCmdList = new ArrayList<>();
    for (File file : testFiles) {
      if (file.getName().toLowerCase().startsWith("narrow") && file.getName().toLowerCase().contains(dbNAME) ) {
        final List<String> l = getAggregationContents(file,NARROW_TABLE_COUNT, "narrow_table_" );
        for (String cmd : l) {
          narrowAggCmdList.add(new DBWorkerThread(ds, cmd));
        }
      } else if (file.getName().toLowerCase().startsWith("medium") && file.getName().toLowerCase().contains(dbNAME)) {
        final List<String> l = getAggregationContents(file, MEDIUM_TABLE_COUNT,"medium_table_" );
        for (String cmd : l) {
          mediumAggCmdList.add(new DBWorkerThread(ds, cmd));
        }
      } else if(file.getName().toLowerCase().startsWith("wide") && file.getName().toLowerCase().contains(dbNAME)) {
        final List<String> l = getAggregationContents(file, WIDE_TABLE_COUNT,"wide_table_" );
        for (String cmd : l) {
          wideAggCmdList.add(new DBWorkerThread(ds, cmd));
        }
      }
    }

  //  wideAggCmdList.get(0).run();
  //   narrowAggCmdList.get(0).run();
    final ThreadPoolExecutor n_executor = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    final ThreadPoolExecutor m_executor = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    final ThreadPoolExecutor w_executor = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    for (int x = 1; x <= ITERATIONS; x++) {
      executeThreads(narrowAggCmdList, n_executor);
      executeThreads(mediumAggCmdList, m_executor);
      executeThreads(wideAggCmdList, w_executor);
    }
    n_executor.shutdown();
    while (!n_executor.isTerminated()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {/**/}
    }
    m_executor.shutdown();
    while (!m_executor.isTerminated()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {/**/}
    }
    w_executor.shutdown();
    while (!w_executor.isTerminated()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {/**/}
    }
    for (DBWorkerThread thread : narrowAggCmdList) {
      double average = thread.getTotalExecutionTime() / thread.getIteration();
      System.out.println("\nAVERAGE: " + average + ", ITERATION: " + ", COMMAND: " + thread.getCommand());
    }

    for (DBWorkerThread thread : mediumAggCmdList) {
      double average = thread.getTotalExecutionTime() / thread.getIteration();
      System.out.println("\nAVERAGE: " + average + ", ITERATION: " +  ", COMMAND: " + thread.getCommand());
    }

    for (DBWorkerThread thread : wideAggCmdList) {
      double average = thread.getTotalExecutionTime() / thread.getIteration();
      System.out.println("\nAVERAGE: " + average + ", ITERATION: " +  ", COMMAND: " + thread.getCommand());
    }


    System.out.println("Finished all threads");
    SQLExecutionTimer q = SQLExecutionTimer.getInstance();
    System.out.println("OP: \n" + q.printTimeRecords());

    writeCommandExecutionTiming(q.printCommandSpecificTimeRecords());

    writeExecutionTiming(narrowAggCmdList, "AGGREGATE_NARROW");
    writeExecutionTiming(mediumAggCmdList, "AGGREGATE_MEDIUM");
    writeExecutionTiming(wideAggCmdList, "AGGREGATE_WIDE");

  }


  private static List<String> getAggregationContents(File aFile, int tableCount, String tableType) {
    final List<String> cmdList = new ArrayList<>();
    String replaceString1 = "@tablename";
    String replaceString2 = "@date_id1";
    String replaceString3 = "@date_id2";
    String aggCmd = "";
    java.util.Date utilDate = new Date();
    java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
    try {
      try (BufferedReader input = new BufferedReader(new FileReader(aFile))) {

        String line ;
        while ((line = input.readLine()) != null) {

          aggCmd = aggCmd + line + " ";

        }

          for(int i = 1; i <= tableCount; i++) {
            String modCmd = aggCmd.replaceAll(replaceString1, tableType + i);
            modCmd = modCmd.replaceAll(replaceString2, getPreviousDate().toString() );
            modCmd = modCmd.replaceAll(replaceString3,    sqlDate.toString()) ;
            cmdList.add(modCmd);

          }
      }
    } catch (IOException ex) {
      throw new DDLApplyException("Couldn't get the contents from DDL file :\n", ex);
    }
    return cmdList;
  }

  private static java.sql.Date getPreviousDate() {
    java.sql.Date d = new java.sql.Date(new Date().getTime());
    System.out.println(d);
// Establish a Calendar object
    Calendar cal = Calendar.getInstance();

// Set the Calendar object to your date
    cal.setTime(d);

// Subtracts 5 days from the date
// cal.add(Calendar.DATE, -5);

// Increments the date by one
//    cal.roll(Calendar.DATE, true);

// Decrements the date by one
   cal.roll(Calendar.DATE, false);

// Convert the Calendar object back to a Date
    Date newDate = new Date(cal.getTime().getTime());

    java.sql.Date nd = new java.sql.Date(newDate.getTime());

    return nd;
  }

  private void writeExecutionTiming(List<DBWorkerThread> commandList, String operationType) {
    DDLWriter timeRecorder = new DDLWriter();
    timeRecorder.writeToFile(OUTPUT_FILE_LOCATION, commandList, operationType, OS_ENV_DELIMETER);
  }

  private void writeCommandExecutionTiming(List<String> timerecords) {
    DDLWriter timeRecorder = new DDLWriter();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

    Date d = new Date();
    String dt = dateFormat.format(d).toString();
    for (String s : timerecords) {
      if (s.toLowerCase().startsWith("create")) {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "CREATE" + dt +".csv", s);
      }
      if (s.toLowerCase().startsWith("drop")) {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "DROP" + dt +".csv", s);
      }
      if (s.toLowerCase().startsWith("alter")) {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "ALTER" + dt +".csv", s);
      }
      if (s.toLowerCase().startsWith("aggregate") && s.toLowerCase().contains("narrow")) {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "Narrow_Table_Aggregate" + dt +".csv", s);
      }
      if (s.toLowerCase().startsWith("aggregate") && s.toLowerCase().contains("medium")) {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "Medium_Table_Aggregate" + dt +".csv", s);
      }
      if (s.toLowerCase().startsWith("aggregate") && s.toLowerCase().contains("wide")) {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "Wide_Table_Aggregate" + dt +".csv", s);
      }
    }
  }

  /**
   * Print the usage
   */
  private static void usage() {
    System.out.println("-t -- DDL Operation Type Install(create and Drop DDLs)/Upgrade(Alter DDL)/Aggregate ");
    System.out.println("-o -- Directory to write file containing Execution timings to");
    System.out.println("-c -- Config file");
    System.out.println("-d -- DB prefix (info/exa/para");
  }

  /**
   * Process the arguements passed in form shell/script to a map
   *
   * @param args Args from shell/script
   * @return Map of arg name to arg value.
   */
  private static Map<String, String> processArgs(final String[] args) {
    final Map<String, String> argMap = new HashMap<>();
    for (int index = 0; index < args.length; index++) {
      if (args[index].startsWith("-")) {
        if (index >= args.length) {
          throw new IndexOutOfBoundsException("Argument " + args[index] + " has no value");
        }
        argMap.put(args[index], args[index + 1]);
      }
    }
    return argMap;
  }


  public static void main(String[] args) {

    if (args == null || args.length == 0) {
      usage();
      System.exit(0);
    }
    final Map<String, String> options = processArgs(args);
    if (!options.containsKey("-t")) {
      System.out.println("No DDL Operation Category Specified!");
      usage();
      System.exit(0);
    }
    if (!options.containsKey("-o")) {
      System.out.println("No output directory specified!");
      usage();
      System.exit(0);
    }
    if (!options.containsKey("-c")) {
      System.out.println("No config file specified!");
      usage();
      System.exit(0);
    }
    if (!options.containsKey("-d")) {
      System.out.println("No DB Prefix specified!");
      usage();
      System.exit(0);
    }

    final String operationType = options.get("-t");
    final String outputDirectory = options.get("-o");
    dbNAME = options.get("-d");

    final File oDir = new File(outputDirectory);
    if (!oDir.exists()) {
      if (!oDir.mkdirs()) {
        throw new GenerateException(new IOException("Failed to create output directory " + oDir.getAbsolutePath()));
      }
    }
    final String propFile = options.get("-c");
    final ApplyAggregation adid = new ApplyAggregation();
    final DataSource ds;
    try {
      DBConnectionPool dbcpool = new DBConnectionPool(propFile);
      ds = dbcpool.setUp();
    } catch (Exception e) {
      throw new DDLApplyException("Couldn't get data source and connection pool :\n", e);
    }
    retrieveProperties(propFile, operationType);
    AGG_SQL_CMD_FILE_LOC = SQL_FILE_LOCATION;
    adid.processDDLCommands(ds);
    System.out.println("Finished all threads");
  }
}
