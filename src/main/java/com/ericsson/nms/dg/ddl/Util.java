package com.ericsson.nms.dg.ddl;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.schema.ColumnType;
import com.ericsson.nms.dg.schema.DataTypeType;
import com.ericsson.nms.dg.schema.TableType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 15/10/13
 * Time: 13:09
 */
public class Util {


  public static File getDataIDFile(final DataTypeType dataType, final String baseDir) {
    final String dataId = getDataTypeID(dataType);
    return new File(baseDir, dataId + ".data.txt");
  }

  public static String getDataTypeID(DataTypeType dataType) {
    return dataType.getType().toLowerCase() + "_c" + dataType.getDataCardinality() + "_m" +
        dataType.getMaxDataLength() + "_a" + dataType.getAvgDataLength();
  }

  public static String getColumnDataID(final ColumnType columnDef, final int instance) {
    return Util.generateName(columnDef, instance) + "::" + getDataTypeID(columnDef.getDataType());
  }

  public static String getTableColumnDataID(final TableType tableDef, final int tableIntance, final ColumnType columnDef, final int instance) {
    return generateName(tableDef, tableIntance) + "::" + getColumnDataID(columnDef, instance);
  }

  public static String generateName(final TableType tableDef, final int instanceCount) {
    return tableDef.getId() + "_" + tableDef.getNamePrefix() + "_" + instanceCount;
  }

  public static String generateName(final ColumnType columnDef, final int instanceCount) {
    return "column_" + columnDef.getId() + "_" + columnDef.getNamePrefix() + "_" + columnDef.getNColumns() + "_" + instanceCount;
  }

  public static void writeLineNumbers(final String outputFile, final List<Long> lineStartBytes) {
    final File lineFile = new File(outputFile + ".lines");
    if (lineFile.exists()) {
      lineFile.delete();
    }
    try (ObjectOutputStream lineWriter = new ObjectOutputStream(new FileOutputStream(lineFile, false))) {
      lineWriter.writeObject(lineStartBytes);
    } catch (IOException e) {
      throw new GenerateException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static List<Long> readLineNumbers(final String dataSourceFile) {
    try (ObjectInputStream reader = new ObjectInputStream(new FileInputStream(dataSourceFile + ".lines"))) {
      return (List<Long>) reader.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new GenerateException(e);
    }
  }

  public static Map<String, String> getFileSystemInfo(final String fsPath) throws IOException, InterruptedException {
    final List<String> command = Arrays.asList("/bin/df", "-h", "-P", fsPath);
    final ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(true);
    final Map<String, String> info = new HashMap<>();
    try {
      final Process p = pb.start();
      final StringBuilder stdout = new StringBuilder();
      String line;
      final BufferedReader processStdout = new BufferedReader(new InputStreamReader(p.getInputStream()));
      while ((line = processStdout.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0) {
          continue;
        }
        stdout.append(line).append("\n");
      }
      final int code = p.waitFor();
      if (code != 0) {
        throw new IOException(stdout.toString().trim());
      }
      final String[] parts = stdout.toString().split("\n")[1].split("\\s+");

      info.put("filesystem", parts[0]);
      info.put("size", parts[1]);
      info.put("used_size", parts[2]);
      info.put("available", parts[3]);
      info.put("used_percent", parts[4].replace("%", ""));
      info.put("mount_path", parts[5]);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return info;
  }
}