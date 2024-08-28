package com.ericsson.nms.dg.gen.formatters;

import com.ericsson.nms.dg.GenerateException;

import java.util.List;

/**
 * Used to format Postgresql CSV format files
 */
public class PostresqlDataFormatter implements DataFormatter {

  private String formatType = null;

  @Override
  public void init(final String formatType) {
    this.formatType = formatType;
  }

  @Override
  public String formatRowData(final List<Object> rowData, final List<String> cellTypes) throws GenerateException {
    if (rowData.size() != cellTypes.size()) {
      throw new GenerateException("Data list size and cell definition list size dont match! " + rowData.size() + " != " + cellTypes.size());
    }
    final StringBuilder line = new StringBuilder();

    for (int i = 0; i < rowData.size(); i++) {
      final Object cellData = rowData.get(i);
      final String cellType = cellTypes.get(i);
      final String formatted = format(cellData, cellType);
      line.append(formatted);
      if (this.formatType.equalsIgnoreCase("csv") && i < rowData.size()) {
        line.append(",");
      }
    }
    return line.toString().trim();
  }

  public String format(final Object data, final String cellTypeClass) {
    if (data == null) {
      return "null";
    }

    switch (cellTypeClass) {
      case "java.lang.Integer":
      case "java.lang.Long":
        return data.toString();
      case "java.lang.String":
      case "java.sql.Timestamp":
        return "\"" + data.toString() + "\"";
      default:
        throw new RuntimeException(new UnsupportedOperationException(data.getClass().getName()));
    }
  }
}
