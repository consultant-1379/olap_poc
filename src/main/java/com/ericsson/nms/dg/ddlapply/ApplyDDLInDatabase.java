package com.ericsson.nms.dg.ddlapply;

import com.ericsson.nms.dg.DDLApplyException;
import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.ddl.DDLWriter;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: qbhasha
 * Date: 05/11/13
 * Time: 13:28
 */
public class ApplyDDLInDatabase {

  protected static int ITERATIONS;
  protected static int THREAD_POOL_SIZE;
  protected static String OUTPUT_FILE_LOCATION;
  protected static String SQL_FILE_LOCATION;
  protected static String OS_ENV_DELIMETER;
  static Properties prop = new Properties();

  public ApplyDDLInDatabase() {
  }

  public static void retrieveProperties(String propFile, String operationType) {
    // load a properties file
    try {
      prop.load(new FileInputStream(propFile));
    } catch (IOException e1) {
      throw new DDLApplyException("Couldn't locate the property file :\n", e1);
    }
    ITERATIONS = Integer.parseInt(prop.getProperty("iterations"));
    if (operationType.equalsIgnoreCase("INSTALL")) {
      SQL_FILE_LOCATION = prop.getProperty("install_output_dir");
    } else if (operationType.equalsIgnoreCase("UPGRADE")) {
      SQL_FILE_LOCATION = prop.getProperty("upgrade_output_dir");
    } else {
      SQL_FILE_LOCATION = prop.getProperty("sql_fileLocation");
    }
    OUTPUT_FILE_LOCATION = prop.getProperty("output_fileLocation");
    THREAD_POOL_SIZE = Integer.parseInt(prop.getProperty("thread_pool_size"));
    String del = prop.getProperty("os_env");
    if (del.equalsIgnoreCase("windows")) {
      OS_ENV_DELIMETER = "\\";
    } else {
      OS_ENV_DELIMETER = "/";
    }

  }

  private static List<String> getContents(File aFile) {
    final List<String> cmdList = new ArrayList<>();
    try {
      try (BufferedReader input = new BufferedReader(new FileReader(aFile))) {
        String line;
        while ((line = input.readLine()) != null) {
          cmdList.add(line);
        }
      }
    } catch (IOException ex) {
      throw new DDLApplyException("Couldn't get the contents from DDL file :\n", ex);
    }
    return cmdList;
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
    final String operationType = options.get("-t");
    final String outputDirectory = options.get("-o");
    final File oDir = new File(outputDirectory);
    if (!oDir.exists()) {
      if (!oDir.mkdirs()) {
        throw new GenerateException(new IOException("Failed to create output directory " + oDir.getAbsolutePath()));
      }
    }
    final String propFile = options.get("-c");
    final ApplyDDLInDatabase adid = new ApplyDDLInDatabase();
    final DataSource ds;
    try {
      DBConnectionPool dbcpool = new DBConnectionPool(propFile);
      ds = dbcpool.setUp();
    } catch (Exception e) {
      throw new DDLApplyException("Couldn't get data source and connection pool :\n", e);
    }
    retrieveProperties(propFile, operationType);
    adid.processDDLCommands(ds);
    System.out.println("Finished all threads");
  }

  protected static void executeThreads(List<DBWorkerThread> createCommandList, ThreadPoolExecutor executor) {
    for (Runnable t : createCommandList) {
      executor.submit(t);
    }
    while (!executor.getQueue().isEmpty()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new DDLApplyException("Executor Monitor Interrupted :\n", e);
      }
    }
  }

  /**
   * Print the usage
   */
  private static void usage() {
    System.out.println("-t -- DDL Operation Type Install(create and Drop DDLs)/Upgrade(Alter DDL) ");
    System.out.println("-o -- Directory to write file containing Execution timings to");
    System.out.println("-c -- Config file");
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

  /**
   * @param ds
   */
  public void processDDLCommands(DataSource ds) {
    final File[] testFiles = new File(SQL_FILE_LOCATION).listFiles();

    Arrays.sort(testFiles, new Comparator<File>() {
      public int compare(File f1, File f2) {
        return Long.valueOf(f1.lastModified()).compareTo(
            f2.lastModified());
      }
    });


    final List<DBWorkerThread> createCommandList = new ArrayList<>();
    List<DBWorkerThread> dropCommandList = new ArrayList<>();
    for (File file : testFiles) {
      if (file.getName().toLowerCase().startsWith("create")) {
        final List<String> l = getContents(file);
        for (String cmd : l) {
          createCommandList.add(new DBWorkerThread(ds, cmd));
        }
      } else if (file.getName().toLowerCase().startsWith("drop")) {
        final List<String> l = getContents(file);
        for (String cmd : l) {
          dropCommandList.add(new DBWorkerThread(ds, cmd));
        }
      }
    }

    final ThreadPoolExecutor c_executor = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    final ThreadPoolExecutor d_executor = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    for (int x = 1; x <= ITERATIONS; x++) {
      executeThreads(createCommandList, c_executor);
      executeThreads(dropCommandList, d_executor);
    }
    c_executor.shutdown();
    while (!c_executor.isTerminated()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {/**/}
    }
    d_executor.shutdown();
    while (!c_executor.isTerminated()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {/**/}
    }

    for (DBWorkerThread thread : createCommandList) {
      double average = thread.getTotalExecutionTime() / thread.getIteration();
      System.out.println("\nAVERAGE: " + average + ", ITERATION: " + thread.getIteration() + ", COMMAND: " + thread.getCommand());
    }


    System.out.println("Finished all threads");
    SQLExecutionTimer q = SQLExecutionTimer.getInstance();
    System.out.println("OP: \n" + q.printTimeRecords());

    writeCommandExecutionTiming(q.printCommandSpecificTimeRecords());

    writeExecutionTiming(createCommandList, "CREATE");
    writeExecutionTiming(dropCommandList, "DROP");


  }

  private void aggregationSQLCMD(String tableId) {
    String cmd =  "INSERT INTO narrow_table_agg \n" +
        "(tablename, column_string1_col__1_1, column_string2_col__1_1, column_string3_col__1_1, column_string4_col__1_1,\n" +
        "column_string5_col__1_1, column_string6_col__1_1, column_string7_col__1_1, column_string8_col__1_1, column_int1_col__4_1,\n" +
        "column_int1_col__4_2, column_int1_col__4_3, column_int1_col__4_4, column_int2_col__2_1, column_int2_col__2_2, column_int3_col__1_1,\n" +
        "column_int4_col__1_1, column_long1_col__4_1, column_long1_col__4_2, column_long1_col__4_3, column_long1_col__4_4, column_long2_col__2_1,\n" +
        "column_long2_col__2_2, column_long3_col__1_1, column_long4_col__1_1, column_string9_col__1_1, column_string10_col__1_1, column_string11_col__1_1, column_string12_col__1_1, column_string13_col__1_1,\n" +
        "column_string14_col__1_1, column_string15_col__1_1,column_string16_col__1_1,, column_int5_col__4_1, column_int5_col__4_2, column_int5_col__4_3, column_int5_col__4_4, column_int6_col__1_1,\n" +
        "column_int7_col__1_1, column_int8_col__2_1, column_int8_col__2_2, cocolumn_string11_col__1_1, column_string12_col__1_1, column_string13_col__1_1, column_string14_col__1_1, column_string15_col__1_1,\n" +
        "column_lumn_long5_col__4_1, column_long5_col__4_2, column_long5_col__4_3,\n" +
        "column_long5_col__4_4, column_long6_col__1_1, column_long7_col__1_1, column_long8_col__2_1, column_long8_col__2_2, column_datetime1_col__1_1,\n" +
        "column_datetime2_col__1_1, column_datetime3_col__1_1) \n" +
        "select @tablename,\n" +
        "max(column_string1_col__1_1),\n" +
        "max(column_string2_col__1_1),\n" +
        "max(column_string3_col__1_1),\n" +
        "max(column_string4_col__1_1),\n" +
        "max(column_string5_col__1_1),\n" +
        "max(column_string6_col__1_1),\n" +
        "max(column_string7_col__1_1),\n" +
        "max(column_string8_col__1_1),\n" +
        "sum(column_int1_col__4_1),\n" +
        "sum(column_int1_col__4_2),\n" +
        "sum(column_int1_col__4_3),\n" +
        "sum(column_int1_col__4_4),\n" +
        "sum(column_int2_col__2_1),\n" +
        "sum(column_int2_col__2_2),\n" +
        "sum(column_int3_col__1_1),\n" +
        "sum(column_int4_col__1_1),\n" +
        "sum(column_long1_col__4_1),\n" +
        "sum(column_long1_col__4_2),\n" +
        "sum(column_long1_col__4_3),\n" +
        "sum(column_long1_col__4_4),\n" +
        "sum(column_long2_col__2_1),\n" +
        "sum(column_long2_col__2_2),\n" +
        "sum(column_long3_col__1_1),\n" +
        "sum(column_long4_col__1_1),\n" +
        "max(column_string9_col__1_1),\n" +
        "max(column_string10_col__1_1), \n" +
        "max(column_string11_col__1_1),\n" +
        "max(column_string12_col__1_1),\n" +
        "max(column_string13_col__1_1),\n" +
        "max(column_string14_col__1_1),\n" +
        "max(column_string15_col__1_1),\n" +
        "max(column_string16_col__1_1),\n" +
        "sum(column_int5_col__4_1),\n" +
        "sum(column_int5_col__4_2),\n" +
        "sum(column_int5_col__4_3),\n" +
        "sum(column_int5_col__4_4),\n" +
        "sum(column_int6_col__1_1),\n" +
        "sum(column_int7_col__1_1),\n" +
        "sum(column_int8_col__2_1),\n" +
        "sum(column_int8_col__2_2),\n" +
        "sum(column_long5_col__4_1),\n" +
        "sum(column_long5_col__4_2),\n" +
        "sum(column_long5_col__4_3),\n" +
        "sum(column_long5_col__4_4),\n" +
        "sum(column_long6_col__1_1),\n" +
        "sum(column_long7_col__1_1),\n" +
        "sum(column_long8_col__2_1),\n" +
        "sum(column_long8_col__2_2),\n" +
        "max(column_datetime1_col__1_1),\n" +
        "max(column_datetime2_col__1_1),\n" +
        "max(column_datetime3_col__1_1) \n" +
        "from @tablename \n" +
        "where column_datetime3_col__1_1 BETWEEN DATE '@date_id1' AND '@date_id2'; \n";


  }

  private void writeExecutionTiming(List<DBWorkerThread> commandList, String operationType) {
    DDLWriter timeRecorder = new DDLWriter();
    timeRecorder.writeToFile(OUTPUT_FILE_LOCATION, commandList, operationType, OS_ENV_DELIMETER);
  }

  private void writeCommandExecutionTiming(List<String> timerecords) {
    DDLWriter timeRecorder = new DDLWriter();
    for (String s : timerecords) {
      if (s.toLowerCase().startsWith("create")) {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "CREATE.csv", s);
      }
      if (s.toLowerCase().startsWith("drop")) {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "DROP.csv", s);
      }
      if (s.toLowerCase().startsWith("alter")) {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "ALTER.csv", s);
      }
    }
  }
}
