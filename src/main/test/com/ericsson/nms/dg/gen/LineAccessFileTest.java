
package com.ericsson.nms.dg.gen;

import com.ericsson.nms.dg.schema.DataTypeType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Date: 30/11/13
 * Time: 13:48
 */
public class LineAccessFileTest {
  private static File DATAFILE = new File(new File(System.getProperty("java.io.tmpdir")), "data.txt");
  private static List<Long> LONGS = new ArrayList<>();

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.out.println("Datafile is " + DATAFILE.getAbsolutePath());
    if (!DATAFILE.getParentFile().exists() && !DATAFILE.getParentFile().mkdirs()) {
      Assert.fail("Failed to create temp directory " + DATAFILE.getParentFile());
    }
    final DataTypeType def = new DataTypeType();
    def.setType("int");
    def.setDataCardinality(10);
    for (long i = 1; i <= def.getDataCardinality(); i++) {
      LONGS.add(i);
    }
    DataSetGenerator.generateIntSet(def, DATAFILE);
  }

  @AfterClass
  public static void afterClass() {
    if (!DATAFILE.delete()) {
      System.out.println("Couldn't delete " + DATAFILE.getAbsolutePath());
    }
  }

  @Test
  public void testLineAccess() throws IOException, InterruptedException {
    try (final LineAccessFile laf = new LineAccessFile("test", DATAFILE, true)) {
      laf.precacheData();
      for (int i = 1; i <= LONGS.size(); i++) {
        final String line = laf.readLine(i);
        System.out.println("line:" + i + ") " + line);
      }
    }
  }
}
