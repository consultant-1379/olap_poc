package com.ericsson.nms.dg.ddl;

import com.ericsson.nms.dg.DdlGenerator;
import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.schema.ColumnType;
import com.ericsson.nms.dg.schema.DataTypeType;
import com.ericsson.nms.dg.schema.DatabaseType;
import com.ericsson.nms.dg.schema.TableType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Default class for DDL Generation. Provides methods for generating statements for
 * 1. Create Table
 * 2. Drop Table
 * 3. Alter Table
 * For any variations this class must be extended and methods to be over ridden to
 * attain intended functionality.
 * <p/>
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 17/10/13
 * Time: 11:55
 * To change this template use File | Settings | File Templates.
 */
public class DefaultDDLGenerator implements DdlGenerator {

  private static final String DB_NAME_KEY = "db_name";
  private static final String COL_NAME_KEY = "alter_table_column_name";
  private final List<String> ddlCmd = new ArrayList<>();
  protected String DB_NAME = "";
  protected String COL_NAME = "";
  protected Properties dbconfig;

  /**
   * Accepts the XML tree as an object and generates the DDL.
   *
   * @param schemaDef XML Based Table object
   * @param dbconfig  configuration file for the DB to be used.
   * @throws GenerateException
   */
  public void generate(final DatabaseType schemaDef, final Properties dbconfig) throws GenerateException {
    this.dbconfig = dbconfig;
    DB_NAME = dbconfig.getProperty(DB_NAME_KEY);
    if (DB_NAME == null || DB_NAME.length() == 0) {
      throw new GenerateException("No " + DB_NAME_KEY + " entry found in config!");
    }
    COL_NAME = dbconfig.getProperty(COL_NAME_KEY);

    for (TableType table : schemaDef.getTable()) {
      generateDDL(table);
      ddlCmd.clear();
      //generateTableTypeDDLCreate(table);
    }

    System.out.println("\n\n Output generated is : " + ddlCmd.toString());
  }

  /**
   * Generates explicit DDL statements for each table for specific operations.
   * Operations are
   * 1. CREATE
   * 2. DROP
   * 3. ALTER
   *
   * @param table Table object for which DDL needs to be generated.
   */
  private void generateDDL(TableType table) {
    generateTableTypeDDLCreate(table);
    generateTableTypeDDLDrop(table);
    generateTableTypeDDLAlter(table);
  }

  /**
   * Creates the DDL statements for CREATE operation based upon the table type ie. narrow wide or medium.
   *
   * @param dbtable Table object for which DDL needs to be generated.
   */
  void generateTableTypeDDLCreate(final TableType dbtable) {
    final DDLWriter writer = new DDLWriter();
    final int tableCount = dbtable.getNTables();
    for (int i = 1; i <= tableCount; i++) {
      final String cmd = getTableCreateStatement(dbtable, i);
      System.out.println(cmd);
      ddlCmd.add(cmd);
    }
    writer.writeToFile(DB_NAME, "CREATE", dbtable.getId(), ddlCmd, dbconfig);
  }

  String getTableCreateStatement(final TableType dbtable, final int tableInstance) {
    final String tableName = dbtable.getId() + "_" + dbtable.getNamePrefix() + "_" + tableInstance;
    return getTableCreateStatement(dbtable, tableName);
  }

  String getTableCreateStatement(final TableType dbtable, final String tableName) {
    final StringBuilder sb = new StringBuilder();
    final String ddlCreateCMD = dbconfig.getProperty("create_command");
    sb.append(ddlCreateCMD).append(" ").append(tableName).append(" (");
    final List<String> columns = formatColumns(dbtable.getColumn(), tableName);
    final Iterator<String> iter = columns.iterator();
    while (iter.hasNext()) {
      final String colFormat = iter.next();
      sb.append(colFormat);
      if (iter.hasNext()) {
        sb.append(",");
      }
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Performs the formatting of the DDL statements for all operations.
   *
   * @param columns   Column object with in a table.
   * @param tableName Table name holding the column.
   * @return list of comma separated column names(data type)
   */
  List<String> formatColumns(final List<ColumnType> columns, final String tableName) {
    final List<String> formatted = new ArrayList<>();
    for (ColumnType column : columns) {
      final int columnCount = column.getNColumns();
      for (int c = 1; c <= columnCount; c++) {
        final String columnName = Util.generateName(column, c);
        final String formattedColumn = formatColumn(columnName, column.getDataType(), tableName);
        formatted.add("\t" + formattedColumn);
      }
    }
    return formatted;
  }

  String formatColumn(final String columnName, final DataTypeType dataType, final String table) {
    final String formattedDataType = formatDatatype(dataType, table, columnName);
    return columnName + " " + formattedDataType;
  }

  /**
   * Retrieves the data type of the column which is intended to be altered using ALTER command.
   *
   * @param columns   Column object with in a table.
   * @param tableName Table name holding the column.
   * @return List of data type of the column
   */
  public List<String> retrieveColumnsDataType(final List<ColumnType> columns, final String tableName) {
    final List<String> formatted = new ArrayList<>();
    for (ColumnType column : columns) {
      final int columnCount = column.getNColumns();
      for (int c = 1; c <= columnCount; c++) {
        final String dataType = formatDatatype(column.getDataType(), tableName, "Generating Alter Command");
        if (formatted.isEmpty() || !formatted.contains(dataType)) {
          formatted.add(dataType);
        }
      }
    }
    return formatted;
  }

  /**
   * Performs the conversion of the DATA TYPE depending upon the Database.
   * Different databases use different syntax for primitive data types. So this method
   * insulates the schema and XML from database specific syntax.
   *
   * @param dataType   Column Data-type
   * @param tableName  Table name holding the column.
   * @param columnName Column Name
   * @return formatted column name with data type
   */
  public String formatDatatype(final DataTypeType dataType, final String tableName, final String columnName) {
    switch (dataType.getType()) {
      case "varchar":
        return getDBVarChar(dataType.getMaxDataLength());
      case "int":
        return getDBInteger();
      case "long":
        return getDBBigInt();
      case "DATETIME":
        return getDBDateTime();
      default:
        throw new GenerateException("Unknown data type '" + dataType.getType() + "' for " + tableName + ":" + columnName);
    }
  }

  String getDBDateTime() {
    return "DATETIME";
  }

  String getDBBigInt() {
    return "BIGINT";
  }

  String getDBInteger() {
    return "INTEGER";
  }

  String getDBVarChar(Integer maxDataLength) {
    return "VARCHAR(" + maxDataLength + ")";
  }

  /**
   * Creates the DDL statements for DROP operation based upon the table type ie. narrow wide or medium.
   *
   * @param dbtable Table object for which DDL needs to be generated.
   */
  private void generateTableTypeDDLDrop(TableType dbtable) {
    List<String> dropCmd = new ArrayList<>();
    DDLWriter writer = new DDLWriter();
    String cmd = "";
    final String tableId = dbtable.getId();
    final String ddlType = dbconfig.getProperty("drop_command");

    final String prefix = dbtable.getNamePrefix();
    final int tableCount = dbtable.getNTables();
    for (int i = 1; i <= tableCount; i++) {
      final String tableName = tableId + "_" + prefix + "_" + i;
      System.out.println(ddlType + " " + tableName);
      cmd = cmd + ddlType + " " + tableName;
      dropCmd.add(cmd);
      cmd = "";
    }
    writer.writeToFile(DB_NAME, ddlType, tableId, dropCmd, dbconfig);
  }

  /**
   * Creates the DDL statements for ALTER operation based upon the table type ie. narrow wide or medium.
   * Scenario 1:
   * ALTER TABLE t1 CHANGE b b BIGINT;
   * t1 = table name
   * b is column name
   * <p/>
   * Scenario 2
   * ALTER TABLE t1 ADD col1 TIMESTAMP, col2 varchar(256);
   * Adding 2 columns in table t1
   * <p/>
   * Scenario3:
   * ALTER TABLE t1 DROP COLUMN c, DROP COLUMN d;
   *
   * @param dbtable Table object for which DDL needs to be generated.
   */
  public void generateTableTypeDDLAlter(TableType dbtable) {
    List<String> alterAddCmd = new ArrayList<>();
    List<String> alterDropCmd = new ArrayList<>();
    List<String> alterModifyCmd;
    List<String> dataTypes = new ArrayList<>();
    List<String> alterDropCmdWithDataType = new ArrayList<>();
    DDLWriter writer = new DDLWriter();
    String cmd = "";
    final String tableId = dbtable.getId();
    final String ddlType = dbconfig.getProperty("alter_command");
    final String alterCmdOption = dbconfig.getProperty("alter_command_option", "MODIFY");
    int dataTypenumber = 10;

    final String prefix = dbtable.getNamePrefix();
    final int tableCount = dbtable.getNTables();
    for (int i = 1; i <= tableCount; i++) {
      final String tableName = tableId + "_" + prefix + "_" + i;
      System.out.println("ALTER TABLE " + tableName + " ");
      cmd = cmd + ddlType + " TABLE" + " " + tableName + " ";

      dataTypes = retrieveColumnsDataType(dbtable.getColumn(), tableName);

      for (String dt : dataTypes) {
        alterAddCmd.add(cmd + "ADD " + COL_NAME + "_" + dataTypenumber + " " + dt);
        alterDropCmd.add(cmd + "DROP " + COL_NAME + "_" + dataTypenumber);
        alterDropCmdWithDataType.add(cmd + "DROP " + COL_NAME + "_" + dataTypenumber + "#" + dt);
        dataTypenumber++;
      }
      cmd = "";
    }
    alterModifyCmd = generateTableTypeDDLAlterModify(dbtable, alterCmdOption);
    writer.writeToFile(DB_NAME, ddlType + "_ADD", tableId, alterAddCmd, dbconfig);
    writer.writeToFile(DB_NAME, ddlType + "_ADD", tableId, alterAddCmd, dbconfig, dataTypes);
    writer.writeToFile(DB_NAME, ddlType + "_DROP", tableId, alterDropCmd, dbconfig);
    writer.writeToFile(DB_NAME, ddlType + "_DROP", tableId, alterDropCmdWithDataType, dbconfig, dataTypes);
    writer.writeToFile(DB_NAME, ddlType + "_MODIFY", tableId, alterModifyCmd, dbconfig);
  }

  /**
   * Generating the Alter command for the existing columns.
   * Changes are done from VARCHAR(128) to VARCHAR(256) and INTEGER to BIGINT.
   * Changing the other DATATYPES may cause the table/data corruption.
   *
   * @param dbtable Table object for which DDL needs to be generated.
   * @return List of DDL commands for ALTER TABLE operation.
   */
  protected List<String> generateTableTypeDDLAlterModify(TableType dbtable, String alterCommandOption) {
    List<String> modCol;
    final String tableId = dbtable.getId();
    final String prefix = dbtable.getNamePrefix();
    final int tableCount = dbtable.getNTables();
    final Map<Column, Column> modifiedColumns = getColumnsListForModification(dbtable);
    modCol = generateSQLForAlter(modifiedColumns, tableId, prefix, tableCount, alterCommandOption);

    return modCol;
  }

  /**
   * Identifying the columns which will be altered as part of ALTER TABLE OPERATION.
   *
   * @param dtable Table object for which DDL needs to be generated.
   * @return List of Columns to be modified.
   */
  Map<Column, Column> getColumnsListForModification(TableType dtable) {
    final Map<Column, Column> modifiedColumns = new HashMap<>();
    boolean intFound = false;
    boolean vcFound = false;

    int columnIndex = 0;
    for (ColumnType column : dtable.getColumn()) {
      final int columnCount = column.getNColumns();
      for (int c = 1; c <= columnCount; c++, columnIndex++) {
        final String columnName = Util.generateName(column, c);
        final String columnType = column.getDataType().getType();
        final int columnSize = column.getDataType().getMaxDataLength();
        if (columnType.equals("int") && !intFound) {
          final Column oldCol = new Column(columnName, "int", columnSize);
          final Column newCol = new Column(columnName, "long", 8);
          modifiedColumns.put(oldCol, newCol);
          intFound = true;
          continue;
        }
        if (columnType.equals("varchar") && columnSize == 128 && !vcFound) {
          final Column oldCol = new Column(columnName, "varchar", columnSize);
          final Column newCol = new Column(columnName, "varchar", 256);
          modifiedColumns.put(oldCol, newCol);
          vcFound = true;
        }
      }
    }
    return modifiedColumns;
  }

  /**
   * Generates the ALTER SQL/DDL statement for a given table for identified columns.
   *
   * @param modifiedColumns    List of columns to be modified.
   * @param tableId            Table Identifier
   * @param prefix             Column Name prefix
   * @param tableCount         Number of tables
   * @param alterCommandOption SQL keyword to be used (CHANGE?MODIFY)
   * @return List of generated SQL/DDL commands.
   */
  List<String> generateSQLForAlter(final Map<Column, Column> modifiedColumns, final String tableId, final String prefix,
                                   final int tableCount, final String alterCommandOption) {
    final List<String> modCol = new ArrayList<>();
    String cmd;
    for (int i = 1; i <= tableCount; i++) {
      final String tableName = tableId + "_" + prefix + "_" + i;
      cmd = "ALTER TABLE " + tableName + " ";
      for (Column col : modifiedColumns.values()) {
        if (alterCommandOption.equalsIgnoreCase("CHANGE")) {
          modCol.add(cmd + "CHANGE " + col.name + " " + col.name + " " + formatDatatype(col.type, tableName, col.name));
        } else {
          modCol.add(cmd + "MODIFY " + col.name + " " + formatDatatype(col.type, tableName, col.name));
        }
      }
    }
    return modCol;
  }

  class Column {
    final String name;
    final DataTypeType type = new DataTypeType();

    private Column(String n, String t, int s) {
      this.name = n;
      this.type.setType(t);
      this.type.setMaxDataLength(s);
    }
  }
}
