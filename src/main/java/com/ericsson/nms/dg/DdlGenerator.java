package com.ericsson.nms.dg;

import com.ericsson.nms.dg.schema.DatabaseType;

import java.util.Properties;

/**
 * User: eeipca
 */
public interface DdlGenerator {

  void generate(final DatabaseType schemaDef, final Properties dbConfig) throws GenerateException;
}
