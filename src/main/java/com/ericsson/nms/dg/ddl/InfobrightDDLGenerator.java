package com.ericsson.nms.dg.ddl;

import com.ericsson.nms.dg.schema.TableType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Class is intended to extend the DefaultsDDLGenerator
 * and to override specific methods to suit InfoBright
 * DDL generation.
 * So far it seems that MySQL standard is used, and is sufficient.
 * Created with IntelliJ IDEA.
 * User: eweroli
 * Date: 21/10/13
 * Time: 09:43
 */
public class InfobrightDDLGenerator extends DefaultDDLGenerator {

  @Override
  protected List<String> generateTableTypeDDLAlterModify(final TableType dbtable, final String alterCommandOption) {

    final String alterType = super.dbconfig.getProperty("infobright.altermodify.type", "select_table");


    final Map<Column, Column> modifiedColumns = getColumnsListForModification(dbtable);
    final Map<String, String> replaces = new HashMap<>();
    String pipeName = "1.pipe";
    for (Map.Entry<Column, Column> modifiedColumn : modifiedColumns.entrySet()) {
      final String oldDef = formatColumn(modifiedColumn.getKey().name, modifiedColumn.getKey().type, "?");
      final String newDef = formatColumn(modifiedColumn.getValue().name, modifiedColumn.getValue().type, "?");
      replaces.put(oldDef, newDef);
    }
    final List<String> alterCommands = new ArrayList<>();
    for (int tableInstance = 1; tableInstance <= dbtable.getNTables(); tableInstance++) {
      final String originalTableName = dbtable.getId() + "_" + dbtable.getNamePrefix() + "_" + tableInstance;
      final StringBuilder sql = new StringBuilder();
      if (alterType.equalsIgnoreCase("select_table")) {
        final String alterTableName = "ALTER_" + originalTableName;
        final String createSQL = getTableCreateStmt(dbtable, alterTableName, replaces);
        sql.append(createSQL).append(";\n");
        sql.append("INSERT INTO ").append(alterTableName).append(" SELECT * FROM ").append(originalTableName).append(";\n");
        sql.append("DROP TABLE ").append(originalTableName).append(";\n");
        sql.append("RENAME TABLE ").append(alterTableName).append(" TO ").append(originalTableName).append(";\n");
        alterCommands.add(sql.toString());
      } else if (alterType.equalsIgnoreCase("dlp")) {
        final String alterTableName = "ALTER_" + originalTableName;
        final String createSQL = getTableCreateStmt(dbtable, alterTableName, replaces);
        sql.append(createSQL).append(";\n");
        sql.append("alter_table_pipe_extr_load ").append(pipeName + " ").
            append(super.dbconfig.getProperty("db_ip") + " ").
            append(super.dbconfig.getProperty("schema_name") + " ").
            append(super.dbconfig.getProperty("userid") + " ").
            append(super.dbconfig.getProperty("password") + " ").
            append(alterTableName).append(";\n");
        sql.append("DROP TABLE ").append(originalTableName).append(";\n");
        sql.append("RENAME TABLE ").append(alterTableName).append(" TO ").append(originalTableName).append(";\n");
      }
      else {
        final String dataFile = "/tmp/" + originalTableName + "_export.csv";
        final String createSQL = getTableCreateStmt(dbtable, originalTableName, replaces);
        sql.append("SELECT * INTO OUTFILE '").append(dataFile);
        sql.append("' FROM ").append(originalTableName).append(";\n");
        sql.append("DROP TABLE ").append(originalTableName).append(";\n");
        sql.append(createSQL).append(";\n");
        sql.append("LOAD DATA INFILE '").append(dataFile).append("' INTO TABLE ").append(originalTableName).append(";\n");
      }
      alterCommands.add(sql.toString());
    }
    return alterCommands;
  }

  private String getTableCreateStmt(final TableType dbtable, final String tableName, final Map<String, String> replaces) {
    String createSQL = getTableCreateStatement(dbtable, tableName);
    for (Map.Entry<String, String> colReplace : replaces.entrySet()) {
      final int index = createSQL.indexOf(colReplace.getKey());
      final String tmp = createSQL.substring(0, index) + colReplace.getValue();
      final String rest = createSQL.substring(index + colReplace.getKey().length());
      createSQL = tmp + rest;
    }
    return createSQL;
  }
}