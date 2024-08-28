package com.ericsson.nms.dg.ddl;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.schema.DataTypeType;

/**
 * Created with IntelliJ IDEA.
 * User: econdun
 * Date: 22/10/13
 * Time: 16:07
 * To change this template use File | Settings | File Templates.
 */
public class ExasolDDLGenerator extends DefaultDDLGenerator{
    /**
     *  Overloaded method.
     *  If the supported data types should differ from MySQL
     *  changes can be included in this method.
     *  So far no changes detected.
     * @param dataType
     * @param tableName
     * @param columnName
     * @return
     */
    @Override
    public String formatDatatype(final DataTypeType dataType, final String tableName, final String columnName) {
        switch (dataType.getType()) {
            case "varchar":
                // return "VARCHAR(" + dataType.getSuppDataLength() + ")";
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
}
