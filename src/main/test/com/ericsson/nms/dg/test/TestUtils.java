package com.ericsson.nms.dg.test;

/**
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 22/10/13
 * Time: 12:11
 * To change this template use File | Settings | File Templates.
 */
public abstract class TestUtils {
  public static final String FILESEPARATOR = System.getProperty("file.separator");
  public static final String curDir = System.getProperty("user.dir");

//  private static final String path = System.getProperty("os.name", "").startsWith("Windows") ? (curDir
//      + FILESEPARATOR + "resources" + FILESEPARATOR + "test" + FILESEPARATOR).replace("\\", "\\\\")
//      : "";
  private static final String path = "C:\\olap_poc\\olap_poc\\src\\main\\resources\\conf"   ;

  public static String getPath() {
    return path;
  }
}
