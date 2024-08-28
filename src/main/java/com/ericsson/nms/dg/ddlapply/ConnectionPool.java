package com.ericsson.nms.dg.ddlapply;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Vector;

/**
 * User: qbhasha
 * Date: 31/10/13
 * Time: 14:20
 */
public class ConnectionPool implements Runnable
{
  // Number of initial connections to make.
  private int initialConnectionCount = 5;

  // A list of available connections for use.
  private Vector<Connection> availableConnections = new Vector<>();

  // A list of connections being used currently.
  private Vector<Connection> usedConnections = new Vector<>();

  // The URL string used to connect to the database
  private String urlString = null;

  // The username used to connect to the database
  private String userName = null;

  // The password used to connect to the database
  private String password = null;


  public ConnectionPool(String url, String user, String passwd) throws SQLException
  {
    // Initialize the required parameters
    urlString = url;
    userName = user;
    password = passwd;

    for(int cnt=0; cnt<initialConnectionCount; cnt++)
    {
      // Add a new connection to the available list.
      availableConnections.addElement(getConnection());
    }

    // Create the cleanup thread
    final Thread cleanupThread = new Thread(this);
    cleanupThread.setDaemon(true);
    cleanupThread.start();
  }

  private Connection getConnection() throws SQLException
  {
    return DriverManager.getConnection(urlString, userName, password);
  }

  public synchronized Connection checkout() throws SQLException
  {
    final Connection newConnxn;

    if(availableConnections.size() == 0)
    {
      //out of connections. Create one more.
      newConnxn = getConnection();
      // Add this connection to the "Used" list.
      usedConnections.addElement(newConnxn);

    }
    else
    {
      // Connections exist !
      // Get a connection object
      newConnxn = availableConnections.lastElement();
      // Remove it from the available list.
      availableConnections.removeElement(newConnxn);
      // Add it to the used list.
      usedConnections.addElement(newConnxn);
    }

    // Either way, we should have a connection object now.
    return newConnxn;
  }


  public synchronized void checkin(Connection c)
  {
    if(c != null)
    {
      // Remove from used list.
      usedConnections.removeElement(c);
      // Add to the available list
      availableConnections.addElement(c);
    }
  }

  public int availableCount()
  {
    return availableConnections.size();
  }

  public void run()
  {
    try
    {
      while(true)
      {
        synchronized(this)
        {
          while(availableConnections.size() > initialConnectionCount)
          {
            // Clean up extra available connections.
            Connection c = availableConnections.lastElement();
            availableConnections.removeElement(c);

            // Close the connection to the database.
            c.close();
          }

          // Clean up is done
        }

        System.out.println("CLEANUP : Available Connections : " + availableCount());

        // Now sleep for 1 minute
        Thread.sleep(60000);
      }
    }
    catch(SQLException sqle)
    {
      sqle.printStackTrace();
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }
}


