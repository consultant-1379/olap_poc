package com.ericsson.nms.dg;

/**
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 01/11/13
 * Time: 14:55
 * To change this template use File | Settings | File Templates.
 */
public class DDLApplyException extends RuntimeException {

  public DDLApplyException(final Throwable cause) {
    super(cause);
  }
  public DDLApplyException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public DDLApplyException(final String message) {
    super(message);
  }
}
