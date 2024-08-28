package com.ericsson.nms.dg.gen.formatters;

import com.ericsson.nms.dg.GenerateException;

import java.util.List;

/**
 * Data formatter interface.
 * The ROP Generator class will use implementors of this iunterface to format rows in ROP files
 */
public interface DataFormatter {

  void init(final String formatType);

  /**
   * Format a row of data
   *
   * @param rowData    List of data objects to be formatted
   * @return String to be written to file
   * @throws GenerateException Errors
   */
  String formatRowData(final List<Object> rowData, final List<String> cellTypes) throws GenerateException;

  String format(final Object data, final String cellTypeClass);
}
