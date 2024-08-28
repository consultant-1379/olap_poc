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

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 16/10/13
 * Time: 15:08
 */
public class GenericDdlGenerator implements DdlGenerator {
  private static final char NL = '\n';
  private static final char COL_SEP = ',';

  @Override
  public void generate(final DatabaseType schemaDef, final Properties dbConfig) throws GenerateException {
    for (TableType table : schemaDef.getTable()) {
      final String createDdl = generateTableTypeDDLCreate(table);
      System.out.println(createDdl);
    }
  }

  private String generateTableTypeDDLCreate(final TableType tableDef) {
    final int tableCount = tableDef.getNTables();
    final StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= tableCount; i++) {
      final String tableName = Util.generateName(tableDef, i);
      final String tableCreatePrefix = formatTableCreate(tableName);
      sb.append(tableCreatePrefix).append(" (").append(NL);
      final List<String> columns = formatColumns(tableDef.getColumn(), tableName);
      final Iterator<String> iter = columns.iterator();
      while (iter.hasNext()) {
        final String colFormat = iter.next();
        sb.append(colFormat);
        if (iter.hasNext()) {
          sb.append(COL_SEP);
        } else {
          sb.append(NL);
        }
      }
      sb.append(");").append(NL);
    }
    return sb.toString();
  }

  private List<String> formatColumns(final List<ColumnType> columns, final String tableName) {
    final List<String> formatted = new ArrayList<>();
    for (ColumnType column : columns) {
      final int columnCount = column.getNColumns();
      for (int c = 1; c <= columnCount; c++) {
        final String columnName = Util.generateName(column, c);
        final String dataType = formatDatatype(column.getDataType(), tableName, columnName);
        formatted.add("\t" + columnName + " " + dataType);
      }
    }
    return formatted;
  }

  private String formatDatatype(final DataTypeType dataType, final String tableName, final String columnName) {
    if (dataType.getType().equalsIgnoreCase("varchar")) {
      if (dataType.getSuppDataLength() == null) {
        throw new GenerateException("No SuppDataLength defined for " + tableName + ":" + columnName);
      }
      return formatTypeVarchar(dataType.getSuppDataLength());
    } else if (dataType.getType().equalsIgnoreCase("int")) {
      return formatTypeInteger();
    } else if (dataType.getType().equalsIgnoreCase("long")) {
      return formatTypeLong();
    } else if (dataType.getType().equalsIgnoreCase("DATETIME")) {
      return formatTypeDatetime();
    } else {
      throw new GenerateException("Unknown data type '" + dataType.getType() + "' for " + tableName + ":" + columnName);
    }
  }

  protected String formatTypeVarchar(final int max_length) {
    return "VARCHAR(" + max_length + ")";
  }

  protected String formatTypeInteger() {
    return "INTEGER";
  }

  protected String formatTypeLong() {
    return "BIGINT";
  }

  protected String formatTypeDatetime() {
    return "TIMESTAMP";
  }

  protected String formatTableCreate(final String tableName) {
    return "CREATE TABLE " + tableName;
  }

}
