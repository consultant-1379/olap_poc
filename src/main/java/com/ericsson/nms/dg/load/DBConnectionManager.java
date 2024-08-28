package com.ericsson.nms.dg.load;

import com.ericsson.nms.dg.DDLApplyException;
import com.ericsson.nms.dg.ddlapply.DBConnectionPool;

import javax.sql.DataSource;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 26/11/13
 * Time: 09:21
 * To change this template use File | Settings | File Templates.
 */
public class DBConnectionManager {

  DataSource ds = null;
  private static List <DataSource> dsList = new ArrayList<>();
  private static List <DBConnectionPool> connectionPoolList = new ArrayList<>();
  private DataSource dataSource;
  private static HashMap < DBConnectionPool , DataSource > conDetails = new HashMap<>();
  private static int dbpoolNumber = 0;



  public  DBConnectionManager (int number, String propFile, String url) {
    String [] urlString = url.split(",");
    for (int i = 0; i< number ; i ++) {
      try {
        DBConnectionPool cp = new DBConnectionPool(propFile, urlString[i]);
        //connectionPoolList.add(new DBConnectionPool(propFile, urlString[i]));

        createConnections(urlString[i], cp);
        connectionPoolList.add(cp);
      } catch (Exception e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }


  }

  public void createConnections(String s, DBConnectionPool cp) {

     System.out.println("Connection properties for the pool--> " + cp.getURL());
      try {
        DataSource ds1 = cp.setUp(s);
        dsList.add(ds1);
        conDetails.put(cp,ds1);
        System.out.println("Connection properties for the pool--> " + cp.getURL());
      } catch (Exception e) {
        throw new DDLApplyException("Couldn't get data source and connection pool :\n", e);
      }

  }


  public DataSource getDataSource() {
    int actCon = -1;
    int actidle = 0;
    if(dbpoolNumber == 0) {
      dbpoolNumber = 1;
    }

    DBConnectionPool poolObj = null;

    poolObj = connectionPoolList.get(dbpoolNumber%4);
//    for (DBConnectionPool dbconp : conDetails.keySet()) {
//      if (actCon < dbconp.getActiveConnections() ) {
//        actCon = dbconp.getActiveConnections();
//        poolObj = dbconp;
//      }
//    }
    System.out.println("Returned object is " + poolObj.getURL());
    dbpoolNumber++;
    return conDetails.get(poolObj);
  }
}
