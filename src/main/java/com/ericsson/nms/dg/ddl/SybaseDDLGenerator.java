package com.ericsson.nms.dg.ddl;

import com.ericsson.nms.dg.DdlGenerator;
import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.schema.ColumnType;
import com.ericsson.nms.dg.schema.DataTypeType;
import com.ericsson.nms.dg.schema.DatabaseType;
import com.ericsson.nms.dg.schema.TableType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

//file based imports


public class SybaseDDLGenerator implements DdlGenerator {

  List <String> ddlCmd =  new ArrayList<String>();
  final String DB_NAME = "SYBASE";


  public void generate(final DatabaseType schemaDef, final Properties dbConfig) throws GenerateException {
    for (TableType table : schemaDef.getTable()) {
      generateTableTypeDDLCreate(table);
    }
    System.out.println("\n\n Output generated is : " + ddlCmd.toString());
  }

  /**
   * Creates the DDLs based upon the table type ie. narrow wide or medium.
   *
   * @param dbtable The table schema definition
   */
  private void generateTableTypeDDLCreate(final TableType dbtable) {

    DDLWriter writer = new DDLWriter();
    String cmd = "";
    final String tableId = dbtable.getId();
    final String ddlType = "CREATE";

    final String prefix = dbtable.getNamePrefix();
    final int tableCount = dbtable.getNTables();
    for (int i = 1; i <= tableCount; i++) {
      final String tableName = tableId + "_" + prefix + "_" + i;
      System.out.println("CREATE TABLE " + tableName + " (");
      cmd = cmd + "CREATE TABLE " + tableName + " (";
      final List<String> columns = formatColumns(dbtable.getColumn(), tableName);
      final Iterator<String> iter = columns.iterator();
      while (iter.hasNext()) {
        final String colFormat = iter.next();
        cmd = cmd + colFormat;
        System.out.print(colFormat);
        if (iter.hasNext()) {
          System.out.println(",");
          cmd = cmd + ",";
        } else {
          System.out.println();
        }
      }
      System.out.println(")");
      cmd = cmd + ")";
      ddlCmd.add(cmd);
      cmd = "";
      writer.writeToFile(DB_NAME, ddlType, tableId, ddlCmd);
    }
  }

  private List<String> formatColumns(final List<ColumnType> columns, final String tableName) {
    final List<String> formatted = new ArrayList<>();
    for (ColumnType column : columns) {
      final int columnCount = column.getNColumns();
      for (int c = 1; c <= columnCount; c++) {
        final String columnName = Util.generateName(column, c);
        //columnIndex++;
        final String dataType = formatDatatype(column.getDataType(), tableName, columnName);
        formatted.add("\t" + columnName + " " + dataType);
      }
    }
    return formatted;
  }

  private String formatDatatype(final DataTypeType dataType, final String tableName, final String columnName) {
    switch (dataType.getType()) {
      case "varchar":
        return "VARCHAR(" + dataType.getMaxDataLength() + ")";
      case "int":
        return "INTEGER";
      case "long":
        return "BIGINT";
      case "DATETIME":
        return "DATETIME";
      default:
        throw new GenerateException("Unknown data type '" + dataType.getType() + "' for " + tableName + ":" + columnName);
    }
  }

}
