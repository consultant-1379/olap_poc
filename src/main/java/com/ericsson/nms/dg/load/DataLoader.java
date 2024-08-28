package com.ericsson.nms.dg.load;

import com.ericsson.nms.dg.DDLApplyException;
import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.ddl.DDLWriter;
import com.ericsson.nms.dg.ddlapply.DBConnectionPool;
import com.ericsson.nms.dg.ddlapply.DBWorkerThread;
import com.ericsson.nms.dg.ddlapply.SQLExecutionTimer;

import javax.sql.DataSource;
import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 26/11/13
 * Time: 08:37
 * To change this template use File | Settings | File Templates.
 */
public class DataLoader {
  protected static int ITERATIONS;
  protected static int THREAD_POOL_SIZE;
  protected static String OUTPUT_FILE_LOCATION;
  protected static String SQL_FILE_LOCATION;
  protected static String OS_ENV_DELIMETER;
  protected static HashSet<String> commandSet = new HashSet<String>();
  protected static String DBURL;

  static Properties prop = new Properties();

  private static DBConnectionManager dbconmanager = null;
  public DataLoader () {

  }
  public static void retrieveProperties(String propFile, String operationType) {
    // load a properties file
    try {
      prop.load(new FileInputStream(propFile));
      //"C:\\olap_poc\\git-merge-by-me\\olap_poc\\src\\main\\resources\\conf\\file_infobright.conf"));
    } catch (FileNotFoundException e1) {
      throw new DDLApplyException("Couldn't locate the property file :\n", e1);

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
    if(del.equalsIgnoreCase("windows")) {
      OS_ENV_DELIMETER = "\\";
    }
    else {
      OS_ENV_DELIMETER = "/";
    }
    DBURL = prop.getProperty("dburl");
  }
  public static void main(String[] args) {

    if (args == null || args.length == 0) {
      usage();
      System.exit(0);
    }
    final Map<String, String> options = processArgs(args);
    if (!options.containsKey("-n")) {
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
    final int operationType = Integer.parseInt(options.get("-n"));
    final String outputDirectory = options.get("-o");
    final File oDir = new File(outputDirectory);
    if (!oDir.exists()) {
      if (!oDir.mkdirs()) {
        throw new GenerateException(new IOException("Failed to create output directory " + oDir.getAbsolutePath()));
      }
    }
    final String propFile = options.get("-c");


    //String = "C:\\olap_poc\\git-merge-by-me\\olap_poc\\src\\main\\resources\\conf\\file_infobright.conf";
    DataLoader dl = new DataLoader();
    DataSource ds = null;
    retrieveProperties(propFile, "load");
    try {
       dbconmanager = new DBConnectionManager(operationType, propFile, DBURL);
    } catch (Exception e) {
      throw new DDLApplyException("Couldn't get data source and connection pool :\n", e);
    }


    dl.processLoading(dbconmanager);
    System.out.println("Finished all threads");
//    SQLExecutionTimer q = SQLExecutionTimer.getInstance();
//    System.out.println("OP: \n" + q.printTimeRecords());

  }

  private void processLoading(DBConnectionManager dbm) {
    final List<DBLoaderThread> loadCommandList = new ArrayList<>();
    int rop = 1;
    File file = new File(SQL_FILE_LOCATION + OS_ENV_DELIMETER + "rop_" + rop + "_cmds.sql");


        List<String> l = getLoadCommand(1, file);
        for (String cmd : l) {
          loadCommandList.add(new DBLoaderThread(dbm.getDataSource(), cmd));
        }

    final ThreadPoolExecutor l_executor = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

    executeThreads(loadCommandList, l_executor);

    l_executor.shutdown();
    while(!l_executor.isTerminated()){
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {/**/}
    }
    double total = 0;
    for (DBLoaderThread thread : loadCommandList) {
      double average = thread.getTotalExecutionTime(); // thread.getIteration();
       total = total +  thread.getTotalExecutionTime();
      System.out.println("\nAVERAGE: " + average + ", ITERATION: " + thread.getIteration() + ", COMMAND: " + thread.getCommand());
    }
    System.out.println("\n\n\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Total Time Taken :     " + total);

    System.out.println("Finished all threads");
    SQLExecutionTimer q = SQLExecutionTimer.getInstance();
    System.out.println("OP: \n" + q.printTimeRecords());

    writeCommandExecutionTiming(q.printCommandSpecificTimeRecords());

    writeExecutionTiming(loadCommandList, "LOAD");
  }

  private void writeExecutionTiming(List<DBLoaderThread> commandList, String operationType) {
    DDLWriter timeRecorder = new DDLWriter();
    //timeRecorder.writeToFile(OUTPUT_FILE_LOCATION, commandList, operationType,OS_ENV_DELIMETER, "test");
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
      else {
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "Load.csv", s);
      }
    }
  }


  private List getLoadCommand(int rop, File f) {
    List <String> cmd = new ArrayList<>();
    List<String> l = getContents(f);
    for (String c : l) {
      cmd.add(c);
    }


    return cmd;
  }

  protected static void executeThreads(List<DBLoaderThread> createCommandList, ThreadPoolExecutor executor) {
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


  private static List getContents(File aFile) {

    StringBuilder contents = new StringBuilder();

    List<String> cmdList = new ArrayList<>();

    try {

      BufferedReader input = new BufferedReader(new FileReader(aFile));
      try {
        String line = null; // not declared within while loop
        while ((line = input.readLine()) != null) {
          cmdList.add(line);
        }
      } finally {
        input.close();
      }
    } catch (IOException ex) {
      throw new DDLApplyException("Couldn't get the contents from DDL file :\n", ex);

    }

    return cmdList;
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
}
