package com.ericsson.nms.dg.ddl;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.schema.DatabaseType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * For Generation of
 *
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 02/12/13
 * Time: 12:00
 * To change this template use File | Settings | File Templates.
 */
public class DIMTableDDLGenerator extends DefaultDDLGenerator {

  private static final String DB_NAME_KEY = "db_name";
  private static final String COL_NAME_KEY = "dim_table_column_name";
  private final List<String> ddlCmd = new ArrayList<>();
  protected String DB_NAME = "";
  protected String COL_NAME = "";
  protected Properties dbconfig;
  protected int DIM_TABLE_COUNT=0;
  protected int DIM_TABLE_COLUMN_COUNT=0;
  protected String DIM_T_DATA_TYPES="";


  /**
   * Accepts the XML tree as an object and generates the DDL.
   *
   * @param schemaDef XML Based Table object
   * @param dbconfig  configuration file for the DB to be used.
   * @throws com.ericsson.nms.dg.GenerateException
   */
  public void generate(final DatabaseType schemaDef, final Properties dbconfig) throws GenerateException {
    this.dbconfig = dbconfig;
    DB_NAME = dbconfig.getProperty(DB_NAME_KEY);
    if (DB_NAME == null || DB_NAME.length() == 0) {
      throw new GenerateException("No " + DB_NAME_KEY + " entry found in config!");
    }
    COL_NAME = dbconfig.getProperty(COL_NAME_KEY);
    DIM_TABLE_COUNT = Integer.parseInt(dbconfig.getProperty("dim_table_count"),10);
    DIM_TABLE_COLUMN_COUNT = Integer.parseInt(dbconfig.getProperty("dim_table_column_count"), 10);
    DIM_T_DATA_TYPES = dbconfig.getProperty("dim_table_column_datatype", "NONE");




      generateDIMDDL();
      ddlCmd.clear();


    System.out.println("\n\n Output generated is : " + ddlCmd.toString());
  }

  private void generateDIMDDL() {
    final DDLWriter writer = new DDLWriter();

    for (int i = 1; i <= DIM_TABLE_COUNT; i++) {
      final String cmd = "CREATE TABLE DIM_TABLE_" + i + " ("+ getDIMTableCreateStatement(i) + ");";
      System.out.println(cmd);
      ddlCmd.add(cmd);
    }
    writer.writeToFile(DB_NAME, "CREATE", "DIM", ddlCmd, dbconfig);

  }



  private String getDIMTableCreateStatement(int i) {
    String command = "";

    if(DIM_T_DATA_TYPES.equalsIgnoreCase("NONE")) {
      System.out.println("NO DATA TYPES PROVIDED FOR DIM TABLES " + i);
    }

    String [] urlString = DIM_T_DATA_TYPES.split(",");
    for(int a = 1; a <= DIM_TABLE_COLUMN_COUNT; a++) {
      if(a== DIM_TABLE_COLUMN_COUNT) {
        command = command +  COL_NAME +  a + " " + getDataType(urlString[2]);
      }
      else {
      command = command + COL_NAME +  a + " " +  getDataType(urlString[a%2]) + ", ";
      }
    }

    return command;
  }

  private String getDataType(String s) {
    String dt;
    if (DB_NAME.equalsIgnoreCase("infobright") && s.equalsIgnoreCase("TIMESTAMP")) {
      dt = "DATETIME";
    }
    else if(s.equalsIgnoreCase("VARCHAR")) {
      dt = s + "(256)";
    }
    else {
      dt = s;
    }

    return dt;  //To change body of created methods use File | Settings | File Templates.
  }
}
