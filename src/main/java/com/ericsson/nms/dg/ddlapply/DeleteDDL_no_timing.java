package com.ericsson.nms.dg.ddlapply;

import com.ericsson.nms.dg.DDLApplyException;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: ealferg
 * Date:
 * Time:
 */
public class DeleteDDL_no_timing {

  protected static String DB_NAME;
  protected static int DAYS_BACK;
  protected static int THREAD_POOL_SIZE;
  protected static String OS_ENV_DELIMETER;
  private static int NARROW_TABLE_COUNT;
  protected static int MEDIUM_TABLE_COUNT;
  private static int WIDE_TABLE_COUNT;
    static {
        MEDIUM_TABLE_COUNT = 80;
        NARROW_TABLE_COUNT = 20;
        WIDE_TABLE_COUNT = 10;
    }

    static Properties prop = new Properties();


  public DeleteDDL_no_timing() {
  }

  public static void retrieveProperties(String propFile) {
    // load a properties file
    try {
      prop.load(new FileInputStream(propFile));
    } catch (IOException e1) {
      throw new DDLApplyException("Couldn't locate the property file :\n", e1);
    }

    DAYS_BACK = Integer.parseInt(prop.getProperty("days_back", "7"));
    THREAD_POOL_SIZE = Integer.parseInt(prop.getProperty("thread_pool_size"));
    DB_NAME = prop.getProperty("db_name");
    String del = prop.getProperty("os_env");
    if (del.equalsIgnoreCase("windows")) {
      OS_ENV_DELIMETER = "\\";
    } else {
      OS_ENV_DELIMETER = "/";
    }

  }


  public static void main(String[] args) {

    if (args == null || args.length == 0) {
      usage();
      System.exit(0);
    }
    final Map<String, String> options = processArgs(args);

    if (!options.containsKey("-c")) {
      System.out.println("No config file specified!");
      usage();
      System.exit(0);
    }


    final String propFile = options.get("-c");
    final DeleteDDL_no_timing adid = new DeleteDDL_no_timing();
    final DataSource ds;
    try {
      DBConnectionPool dbcpool = new DBConnectionPool(propFile);
      ds = dbcpool.setUp();
    } catch (Exception e) {
      throw new DDLApplyException("Couldn't get data source and connection pool :\n", e);
    }
    retrieveProperties(propFile);
    adid.processDDLCommands(ds);
    System.out.println("Finished all threads");
  }

  protected static void executeThreads(List<DBWorkerThread> deletecmdCommandList, ThreadPoolExecutor executor) {
    for (Runnable t : deletecmdCommandList) {
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
      System.out.println("days_back in the config file defines how far back from current datetime the delete point will be set to delete older data then it");
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

      String[] table_type = {"narrow", "medium", "wide"};
      int TABLE_COUNT = 0;
      List<String> ls;
      List<DBWorkerThread> deletecmdCommandList = new ArrayList<>();
      for (String s : table_type) {
          if (s.equals("narrow")) {
              TABLE_COUNT = NARROW_TABLE_COUNT;

          } else if (s.equals("medium")) {
              TABLE_COUNT = MEDIUM_TABLE_COUNT;

          } else if (s.equals("wide")) {
              TABLE_COUNT = WIDE_TABLE_COUNT;

          }

          ls = generatecommandlist(TABLE_COUNT, s, DAYS_BACK);
          for (String cmd : ls) {
              deletecmdCommandList.add(new DBWorkerThread(ds, cmd));
              System.out.println(cmd);
          }
      }




      final ThreadPoolExecutor d_executor = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());


      executeThreads(deletecmdCommandList, d_executor);
      d_executor.shutdown();
      while (!d_executor.isTerminated()) {
          try {
              Thread.sleep(1000);
      /**/
          } catch (InterruptedException e) {/**/}
      }


/*      for (DBWorkerThread thread : deletecmdCommandList) {
          double average = thread.getTotalExecutionTime() / thread.getIteration();
          System.out.println("\nAVERAGE: " + average + ", ITERATION: " + thread.getIteration() + ", COMMAND: " + thread.getCommand());
      }   */

      System.out.println("Finished all threads");


  }



    private static List<String> generatecommandlist( int tableCount, String tableType, int DAYS_BACK) {
        final List<String> cmdList = new ArrayList<>();

       DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        int step_back = DAYS_BACK * -1;
        cal.add(Calendar.DATE, step_back);


        String date_replace;
        date_replace = dateFormat.format(cal.getTime());
       // date_replace = "2010/12/04 16:05:59";

        for(int i = 1; i <= tableCount; i++) {
            String aggCmd = "delete from table_name_holder where column_datetime3_col__1_1 <= 'date_holder'";
            aggCmd=aggCmd.replaceAll("table_name_holder",tableType+"_table_"+i);
                    aggCmd=aggCmd.replaceAll("date_holder", date_replace);
            if (DB_NAME.contains("EXASOL_DB")) {
                aggCmd=aggCmd.replaceAll("<=", "<= TIMESTAMP").replaceAll("/", "-");
            }
                    cmdList.add(aggCmd);
                }


        return cmdList;

    }
}