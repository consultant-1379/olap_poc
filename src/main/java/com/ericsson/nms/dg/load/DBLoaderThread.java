package com.ericsson.nms.dg.load;

import com.ericsson.nms.dg.DDLApplyException;
import com.ericsson.nms.dg.ddlapply.SQLExecutionTimer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 26/11/13
 * Time: 13:30
 * To change this template use File | Settings | File Templates.
 */
public class DBLoaderThread implements Runnable{



  private java.sql.Connection connection;

  private String command;

  private double totalExecutionTime;

  private int iteration;

  private DataSource dataSource;

  public DBLoaderThread(DataSource dataSource, String ddlcmd) {
    this.dataSource = dataSource;

    this.command = ddlcmd;
    this.totalExecutionTime = 0;
    this.iteration = 0;
  }

  @Override
  public void run() {
    Connection conn = null;
    String clName="";
    System.out.println(Thread.currentThread().getName() + " Start. Command = " + command);
    try {
      conn = dataSource.getConnection();
      clName = conn.getClientInfo().toString();
    } catch (SQLException e) {
      throw new DDLApplyException("Unable to get connection from connection pool :\n", e);
    }
    processCommand(conn, command);

    System.out.println(Thread.currentThread().getName() + " Con URL-" + clName + " End.");
  }

  /**
   * Performs the execution of the DDL command on JDBC connection object provided.
   *
   * It also records the start time and finish time along with average time.
   *
   * @param conn JDBC connection object from the connection pool.
   * @param ddlcmd DDL command to be executed.
   */
  private void processCommand(Connection conn, String ddlcmd) {
    String timing_details;
    String command;
    try {
      java.sql.Statement stmt = conn.createStatement();
      long start_time = System.nanoTime();
      stmt.executeUpdate(ddlcmd);
      long end_time = System.nanoTime();
      double exec_sec = (end_time - start_time) / 1e6;
      timing_details = start_time + " , " + end_time + " , " + exec_sec;
      // sb.append(x + "," + command + "," + exec_sec + "\n");
      this.totalExecutionTime = this.totalExecutionTime + exec_sec;

        command = ddlcmd.substring(0, ddlcmd.indexOf("FROM"));
      SQLExecutionTimer sqlET = SQLExecutionTimer.getInstance();

      sqlET.updateTimingDetails(command + "_ITERATION" + this.getIteration(), timing_details);
      //Thread.sleep(5000);
    } catch (SQLException e) {
      throw new DDLApplyException("SQL Exception while executing the statement :\n" + ddlcmd + "\n", e);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (Throwable t) {
          /* Ignore this... */
        }
      }
    }
  }

  @Override
  public String toString() {
    return this.command;
  }

  /**
   * Retrieves the execution time it took for all the performed tasks.
   *
   * @return the execution time it took for all the performed tasks.
   */
  public double getTotalExecutionTime() {
    return this.totalExecutionTime;
  }

  /**
   * Number of times the thread has been executed
   * Should be same as the number of iterations defined in properties file.
   * @return iteration count
   */
  public int getIteration() {
    return this.iteration;
  }

  /**
   * Retrieves the command assigned for execution by this thread
   * @return command under the execution scope.
   */
  public String getCommand() {
    return this.command;
  }
}
