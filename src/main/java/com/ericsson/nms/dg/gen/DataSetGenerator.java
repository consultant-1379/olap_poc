package com.ericsson.nms.dg.gen;

import com.ericsson.nms.dg.DataConfigParser;
import com.ericsson.nms.dg.ddl.Util;
import com.ericsson.nms.dg.gen.indexers.VarcharIndexer;
import com.ericsson.nms.dg.schema.ColumnType;
import com.ericsson.nms.dg.schema.DataTypeType;
import com.ericsson.nms.dg.schema.DatabaseType;
import com.ericsson.nms.dg.schema.TableType;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 22/10/13
 * Time: 11:53
 */
public class DataSetGenerator {
  /**
   * Print the usage
   */
  private static void usage(final Options options) {
    final HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("DG", options);
  }

  /**
   * Main
   *
   * @param args Shell args
   * @throws NoSuchMethodException
   * @throws IOException
   */
  public static void main(final String[] args) throws NoSuchMethodException, IOException, ParseException, ConfigurationException {
    final Options argopts = new Options();
    argopts.addOption("config", true, "RopGen server config");


    if (args == null || args.length == 0) {
      usage(argopts);
      System.exit(2);
    }

    final CommandLine options;
    try {
      final CommandLineParser cliParser = new PosixParser();
      options = cliParser.parse(argopts, args);
    } catch (ParseException e) {
      usage(argopts);
      throw e;
    }


    final String configFile = options.getOptionValue("config");
    final Configuration configuration = new PropertiesConfiguration(configFile);

    final int maxCardinality = configuration.getInt("pregen.max_card", 50000000);
    final String xml = configuration.getString("table_def");
    final String dataDir = configuration.getString("sample_dir");

    final DatabaseType dbSchema = DataConfigParser.parse(xml, DataConfigParser.SCHEMA_TABLEDEF_FILE, DatabaseType.class);

    final File outputDir = new File(dataDir);
    if (!outputDir.exists()) {
      if (!outputDir.mkdirs()) {
        throw new IOException("Failed to create " + outputDir.getAbsolutePath());
      }
    }
    generate(dbSchema, outputDir.getAbsolutePath(), maxCardinality);
  }

  private static void generate(final DatabaseType dbSchema, final String outputdir, final int maxCardinality) throws NoSuchMethodException, IOException {

    final List<String> completed = new ArrayList<>();

    for (TableType table : dbSchema.getTable()) {
      for (ColumnType column : table.getColumn()) {
        final String dataType = column.getDataType().getType().toLowerCase();
        final String dataId = Util.getDataTypeID(column.getDataType());
        if (completed.contains(dataId)) {
          System.out.println("Data set for " + dataId + " already generated.");
          continue;
        }
        if (column.getDataType().getDataCardinality() >= maxCardinality) {
          System.out.println("Skipping generating data set for " + dataId + ", cardinality too large.");
          continue;
        }
        System.out.println("Generating data set for " + dataId);
        final File outputFile = Util.getDataIDFile(column.getDataType(), outputdir);
        System.out.println("\t" + outputFile.getAbsolutePath());
        final long start = System.currentTimeMillis();
        switch (dataType) {
          case "varchar":
            generateStringSet(column.getDataType(), outputFile);
            break;
          case "int":
            generateIntSet(column.getDataType(), outputFile);
            break;
          case "long":
            generateLongSet(column.getDataType(), outputFile);
            break;
          case "datetime":
            generateDatetimeSet(column.getDataType(), outputFile);
            break;
          default:
            throw new NoSuchMethodException(dataType);
        }
        final long taken = System.currentTimeMillis() - start;
        System.out.println("\tTook " + taken + " mSec");
        completed.add(dataId);
      }
    }
  }

  public static void generateDatetimeSet(final DataTypeType typeDef, final File outputFile) throws IOException {
    if (outputFile.exists() && !outputFile.delete()) {
      throw new IOException("Failed to delete old " + outputFile.getAbsolutePath());
    }
    final List<Long> lineStartBytes = new ArrayList<>();
    try (RandomAccessFile dataWriter = new RandomAccessFile(outputFile, "rw")) {
      for (int i = 0; i < typeDef.getDataCardinality(); i++) {
        final long timestamp = System.currentTimeMillis() - (i * 1000);
        lineStartBytes.add(dataWriter.getFilePointer());
        dataWriter.write(new Timestamp(timestamp).toString().getBytes());
        dataWriter.write("\n".getBytes());
      }
    }
    Util.writeLineNumbers(outputFile.getAbsolutePath(), lineStartBytes);
  }

  public static void generateLongSet(final DataTypeType typeDef, final File outputFile) throws IOException {
    if (outputFile.exists() && !outputFile.delete()) {
      throw new IOException("Failed to delete old " + outputFile.getAbsolutePath());
    }
    final List<Long> lineStartBytes = new ArrayList<>();
    try (RandomAccessFile dataWriter = new RandomAccessFile(outputFile, "rw")) {
      for (int i = 0; i < typeDef.getDataCardinality(); i++) {
        lineStartBytes.add(dataWriter.getFilePointer());
        dataWriter.write(Long.toString(i).getBytes());
        dataWriter.write("\n".getBytes());
      }
    }
    Util.writeLineNumbers(outputFile.getAbsolutePath(), lineStartBytes);
  }

  public static void generateIntSet(final DataTypeType typeDef, final File outputFile) throws IOException {
    if (outputFile.exists() && !outputFile.delete()) {
      throw new IOException("Failed to delete old " + outputFile.getAbsolutePath());
    }
    final List<Long> lineStartBytes = new ArrayList<>();
    try (RandomAccessFile dataWriter = new RandomAccessFile(outputFile, "rw")) {
      for (int i = 0; i < typeDef.getDataCardinality(); i++) {
        lineStartBytes.add(dataWriter.getFilePointer());
        dataWriter.write(Integer.toString(i).getBytes());
        dataWriter.write("\n".getBytes());
      }
    }
    Util.writeLineNumbers(outputFile.getAbsolutePath(), lineStartBytes);
  }

  public static void generateStringSet(final DataTypeType typeDef, final File outputFile) throws IOException {
    if (outputFile.exists() && !outputFile.delete()) {
      throw new IOException("Failed to delete old " + outputFile.getAbsolutePath());
    }
    final List<Long> lineStartBytes = new ArrayList<>();
    final VarcharIndexer indexer = new VarcharIndexer("gen", typeDef, false);
    indexer.initData(null);
    int total = 0;
    int max = 0;
    int min = 0;
    try (RandomAccessFile dataWriter = new RandomAccessFile(outputFile, "rw")) {
      for (int i = 0; i < typeDef.getDataCardinality(); i++) {
        final String data = indexer.getROPValue(i);
        final int length = data.length();
        if (max < length) {
          max = length;
        }
        if (min > length) {
          min = length;
        }
        total += length;
        lineStartBytes.add(dataWriter.getFilePointer());
        dataWriter.write(data.getBytes());
        dataWriter.write("\n".getBytes());
      }
    }
    System.out.println("\tFinished generation, wirting line index file ...");
    Util.writeLineNumbers(outputFile.getAbsolutePath(), lineStartBytes);
    System.out.println("\tMax " + max);
    System.out.println("\tMin " + min);
    System.out.println("\tAverage " + (total / typeDef.getDataCardinality()));
  }
}
