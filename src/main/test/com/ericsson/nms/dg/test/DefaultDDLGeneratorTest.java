package com.ericsson.nms.dg.test;

import com.ericsson.nms.dg.DataConfigParser;
import com.ericsson.nms.dg.ddl.DefaultDDLGenerator;
import com.ericsson.nms.dg.schema.ColumnType;
import com.ericsson.nms.dg.schema.DataTypeType;
import com.ericsson.nms.dg.schema.DatabaseType;
import com.ericsson.nms.dg.schema.TableType;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 22/10/13
 * Time: 11:49
 * To change this template use File | Settings | File Templates.
 */
public class DefaultDDLGeneratorTest extends TestCase {

  String xmlFile = "C:\\olap_poc\\olap_poc\\src\\main\\resources\\conf\\datagen.xml";
  String opDir = "C:\\Temp\\OP";
  String cfgFile = "C:\\olap_poc\\olap_poc\\src\\main\\resources\\conf\\DB_Default.conf" ;
  String schemaFile = "C:\\olap_poc\\olap_poc\\src\\main\\resources\\xsd\\datagen.xsd" ;
  DataConfigParser dg = new DataConfigParser();
  final DatabaseType data = dg.parse(xmlFile, schemaFile, DatabaseType.class);
  final List<TableType> ltt =data.getTable();
  final List <ColumnType> lct = ltt.get(0).getColumn();


  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testGenerate() throws Exception {
    DataConfigParser dcp = new DataConfigParser();
    assertNotNull(dcp);
    final DatabaseType data = dg.parse(xmlFile, schemaFile, DatabaseType.class);
    assertNotNull(data);
    final List<TableType> ltt =data.getTable();
    assertNotNull(ltt);
    final List <ColumnType> lct = ltt.get(0).getColumn();
    assertNotNull(lct);
    DefaultDDLGenerator dg = new DefaultDDLGenerator();
    assertNotNull(dg);
    dg.generate(data,null);

    assertNotNull(opDir);
  }


  @Test
  public void testFormatDatatype() throws Exception {
    DefaultDDLGenerator dg = new DefaultDDLGenerator();
    DataTypeType dataType = lct.get(0).getDataType();
    assertEquals("VARCHAR(256)", dg.formatDatatype(dataType,"MyTable", "MyColumn"));

  }
  @Test
  public void testFormatColumns() throws Exception {

  }

}
