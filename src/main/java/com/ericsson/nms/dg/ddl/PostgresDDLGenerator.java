package com.ericsson.nms.dg.ddl;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 16/10/13
 * Time: 15:06
 */
public class PostgresDDLGenerator extends GenericDdlGenerator {

  protected String formatTypeInteger() {
    return "integer";
  }
}
