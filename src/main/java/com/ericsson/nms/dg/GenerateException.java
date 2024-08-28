package com.ericsson.nms.dg;

/**
 * User: eeipca
 */
public class GenerateException extends RuntimeException {
  private int code = -1;

  public GenerateException(final Throwable cause) {
    super(cause);
  }

  public GenerateException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public GenerateException(final String message) {
    super(message);
  }

  public GenerateException(final String message, final int code) {
    super(message);
    this.code = code;
  }

  public int getCode() {
    return this.code;
  }
}
