package com.ericsson.nms.dg.ddlapply;

import com.ericsson.nms.dg.GenerateException;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;


/**
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 01/11/13
 * Time: 09:41
 * To change this template use File | Settings | File Templates.
 */
public class DBConnectionPool {
  protected static Statement stmt;
  protected static String DRIVER = "com.mysql.jdbc.Driver";
  protected static String URL = "jdbc:mysql://localhost/sampledb";
  protected static String USERNAME = "root";
  protected static String PASSWORD = "";
  protected static int ITERATIONS;
  protected static String SQL_FILE_LOCATION;
  protected static String OUTPUT_FILE_LOCATION;
  protected static int MAX_CONNECTIONS;

  private GenericObjectPool connectionPool = null;
  protected static StringBuilder sb = new StringBuilder();
  protected static HashSet<String> commandSet = new HashSet<String>();

  Properties prop = new Properties();

  public DBConnectionPool(GenericObjectPool connectionPool, Properties prop) {

    this.connectionPool = connectionPool;
    this.prop = prop;
  }

  public DBConnectionPool(String propFile) throws Exception {
    retrieveConnectionProperties(propFile);
   setUp();
  }
  public DBConnectionPool(String propFile, String url) throws Exception {
    retrieveConnectionProperties(propFile);
    this.URL = url;
    setUp();
  }



  public void retrieveConnectionProperties(String propFile) {
    // load a properties file
    try {
      prop.load(new FileInputStream(propFile));
          //"C:\\olap_poc\\git-merge-by-me\\olap_poc\\src\\main\\resources\\conf\\file_infobright.conf"));
    } catch (FileNotFoundException e1) {
      throw new GenerateException(e1);

    } catch (IOException e1) {
      throw new GenerateException(e1);

    }

    USERNAME = prop.getProperty("userid");
    PASSWORD = prop.getProperty("password");
    DRIVER = prop.getProperty("driver");
    URL = prop.getProperty("url");
    ITERATIONS = Integer.parseInt(prop.getProperty("iterations"));
    SQL_FILE_LOCATION = prop.getProperty("sql_fileLocation");
    OUTPUT_FILE_LOCATION = prop.getProperty("output_fileLocation");

    String tmp = prop.getProperty("max_connections", "20");
    MAX_CONNECTIONS = Integer.parseInt(tmp);
  }

   public DataSource setUp() throws Exception {
    //
    // Load JDBC Driver class.
    //
    Class.forName(DBConnectionPool.DRIVER).newInstance();

    //
    // Creates an instance of GenericObjectPool that holds our
    // pool of connections object.
    //
    connectionPool = new GenericObjectPool();
    connectionPool.setMaxActive(MAX_CONNECTIONS);

    //
    // Creates a connection factory object which will be use by
    // the pool to create the connection object. We passes the
    // JDBC url info, username and password.
    //
    ConnectionFactory cf = new DriverManagerConnectionFactory(
        DBConnectionPool.URL,
        DBConnectionPool.USERNAME,
        DBConnectionPool.PASSWORD);

    //
    // Creates a PoolableConnectionFactory that will wraps the
    // connection object created by the ConnectionFactory to add
    // object pooling functionality.
    //
    PoolableConnectionFactory pcf =
        new PoolableConnectionFactory(cf, connectionPool,
            null, null, false, true);
    return new PoolingDataSource(connectionPool);
  }

  public DataSource setUp(String url) throws Exception {

    Class.forName(DBConnectionPool.DRIVER).newInstance();


    connectionPool = new GenericObjectPool();
    connectionPool.setMaxActive(MAX_CONNECTIONS);


    ConnectionFactory cf = new DriverManagerConnectionFactory(
        url,
        DBConnectionPool.USERNAME,
        DBConnectionPool.PASSWORD);


    PoolableConnectionFactory pcf =
        new PoolableConnectionFactory(cf, connectionPool,
            null, null, false, true);
    return new PoolingDataSource(connectionPool);
  }


  public GenericObjectPool getConnectionPool() {
    return connectionPool;
  }


  /**
   * Prints connection pool status.
   */
  private void printStatus() {
    System.out.println("Max   : " + getConnectionPool().getMaxActive() + "; " +
        "Active: " + getConnectionPool().getNumActive() + "; " +
        "Idle  : " + getConnectionPool().getNumIdle());
  }

  public int getActiveConnections () {
    return getConnectionPool().getNumActive();
  }

  public int getIdleConnections() {
    return getConnectionPool().getNumIdle();
  }

  public String getURL() {

    return URL;
  }

}
