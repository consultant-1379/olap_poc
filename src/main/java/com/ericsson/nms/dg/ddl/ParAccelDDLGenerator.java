package com.ericsson.nms.dg.ddl;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.schema.ColumnType;
import com.ericsson.nms.dg.schema.DataTypeType;
import com.ericsson.nms.dg.schema.TableType;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class is intended to extend the DefaultsDDLGenerator
 * and to override specific methods to suit ParAccel
 * DDL generation.

 * Date: 18/10/13
 */
public class ParAccelDDLGenerator extends DefaultDDLGenerator {

    private class Column {
        String name;
        DataTypeType type = new DataTypeType();

        private Column (String n, String t, int s) {
            this.name = n;
            this.type.setType(t);
            this.type.setMaxDataLength(s);
        }
    }

    @Override
    public String formatDatatype(final DataTypeType dataType, final String tableName, final String columnName) {
        switch (dataType.getType()) {
            case "varchar":
                return "VARCHAR(" + dataType.getMaxDataLength() + ")";
            case "int":
                return "INTEGER";
            case "long":
                return "BIGINT";
            case "DATETIME":
                return "TIMESTAMP";
            default:
                throw new GenerateException("Unknown data type '" + dataType.getType() + "' for " + tableName + ":" + columnName);
        }
    }

  private String commaSeparatedColumnNames(List<String> columnNameList ){
    String concat = "";
    for(String l : columnNameList) {

      concat = concat + l + ", ";

    }
    concat = concat.substring(0, concat.lastIndexOf(","));
    return concat;
  }
    /**
     * Creates the DDL statements for ALTER operation based upon the table type ie. narrow, medium and wide.
     *
     * Scenario 1:
     *      Alter table add column
     *
     * Scenario 2:
     *      Alter table drop column
     *
     * Scenario 3:
     *      Alter table modify column data type. To achieve that we need to:
     *          * unload the table
     *          * truncate the table
     *          * drop the column
     *          * add the column
     *          * copy (load) the table
     *
     * @param dbtable Table object
     */
    @Override
    public void generateTableTypeDDLAlter (TableType dbtable) {
        List<String> addCol = new ArrayList<>();
        List<String> dropCol = new ArrayList<>();
        List<String> modCol = new ArrayList<>();
        List<String> dataTypes = new ArrayList<>();
        DDLWriter writer = new DDLWriter();
        String cmd;
        final String tableId = dbtable.getId();
        final String alter_cmd = dbconfig.getProperty("alter_command");
        final String ddlType = "ALTER";
        int dataTypeNumber = 10;
        final String prefix = dbtable.getNamePrefix();
        final int tableCount = dbtable.getNTables();

        /**
         * Variables for alter column data type
         */
        List<Column> modifiedColumns = new ArrayList<>();
        List<String> modifiedColumnNames = new ArrayList<>();
        String selectStatement = "select ";
        boolean intFound = false;
        boolean vcFound = false;


        /**
         * Alter column data type cycle
         * It generates the select statement and picks an int column for modification
         */
        int columnIndex = 0;
        for (ColumnType column : dbtable.getColumn()) {
            final int columnCount = column.getNColumns();

            for (int c = 1; c <= columnCount; c++, columnIndex++) {
                final String columnName = Util.generateName(column,  c);
                final String columnType = column.getDataType().getType();
                final int columnSize = column.getDataType().getMaxDataLength();
                if (columnType.equals("int") && !intFound) {
                    Column newCol = new Column(columnName, "long", 8);
                    modifiedColumns.add(newCol);
                    modifiedColumnNames.add(newCol.name);
                    intFound = true;
                    continue;
                }
                if (columnType.equals("varchar") && columnSize == 128 && !vcFound) {
                    Column newCol = new Column(columnName, "varchar", 256);
                    modifiedColumns.add(newCol);
                    modifiedColumnNames.add(newCol.name);
                    vcFound = true;
                    continue;
                }
                selectStatement = selectStatement + columnName + ", ";
            }
        }
         selectStatement = selectStatement + commaSeparatedColumnNames(modifiedColumnNames);
        //selectStatement = selectStatement + StringUtils.join(modifiedColumnNames, ", ");

        /**
         * For all tables of the type, generate the 'add column' and 'drop column' commands
         */
        for (int i = 1; i <= tableCount; i++) {
            final String tableName = tableId + "_" + prefix + "_" + i;
//            System.out.println("ALTER TABLE " + tableName + " ");
            cmd = alter_cmd + " " + tableName + " ";

            dataTypes = retrieveColumnsDataType(dbtable.getColumn(), tableName);

            for(String dt : dataTypes ) {
                addCol.add(cmd + "ADD COLUMN " + COL_NAME + "_" + dataTypeNumber + " " + dt);
                dropCol.add(cmd + "DROP COLUMN " + COL_NAME + "_" + dataTypeNumber);
                dataTypeNumber++;
            }

            modCol.add(unloadTable(DB_NAME, tableName, selectStatement));
            modCol.add(truncateTable(tableName));
            for(Column col: modifiedColumns) {
                modCol.add(dropColumn(tableName, col.name));
                modCol.add(addColumn(tableName, col.name, formatDatatype(col.type, tableName, col.name)));
            }
            modCol.add(copyTable(tableName));

        }
        writer.writeToFile(DB_NAME, ddlType + "_addCol", tableId , addCol, dbconfig);
        writer.writeToFile(DB_NAME, ddlType + "_addCol", tableId , addCol, dbconfig, dataTypes);
        writer.writeToFile(DB_NAME, ddlType + "_dropCol", tableId , dropCol, dbconfig);
        writer.writeToFile(DB_NAME, ddlType + "_modCol", tableId , modCol, dbconfig);
    }

    /** UNLOAD command example
     *
     *      unload ('<select statement>') to '<database_name/table_name>' diststyle none;
     *
     * @param dbName Database name
     * @return String
     */
    private String unloadTable(String dbName, String tableName, String selectStatement) {
        return "UNLOAD ('" + selectStatement + " from " + tableName + "') to '"
                + dbName + "/" + tableName + "' diststyle none";
    }

    /** DDL generator for 'truncate table' command
     *
     * @param tableName Table name
     * @return String
     */
    private String truncateTable(String tableName) {
        return "TRUNCATE TABLE " + tableName;
    }

    /** DDL generator for 'drop column' command
     *
     * @param dbTable Table name
     * @param column Column name
     * @return String
     */
    private String dropColumn(String dbTable, String column) {
        return "ALTER TABLE " + dbTable + " DROP COLUMN " + column;
    }

    /**
     *
     * @param dbTable Table name
     * @param column Column name
     * @param dataType New column type
     * @return SQL command to drop column
     */
    private String addColumn(String dbTable, String column, String dataType) {
        return "ALTER TABLE " + dbTable + " ADD COLUMN " + column + " " + dataType;
    }

    /** COPY table example
     *
     *      copy users from 'copied' parallel delimiter '|';
     */
    private String copyTable(String dbTable) {
        return "copy " + dbTable + " from 'copied' parallel delimiter '|'";
    }
}
