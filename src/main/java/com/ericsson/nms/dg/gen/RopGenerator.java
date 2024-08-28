package com.ericsson.nms.dg.gen;

import com.ericsson.nms.dg.DataConfigParser;
import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.ddl.Util;
import com.ericsson.nms.dg.gen.indexers.Indexer;
import com.ericsson.nms.dg.gen.indexers.Indexers;
import com.ericsson.nms.dg.jmx.LineAccessFileMBean;
import com.ericsson.nms.dg.jmx.ROPGeneratorMBean;
import com.ericsson.nms.dg.jmx.RopServerMBean;
import com.ericsson.nms.dg.schema.ColumnType;
import com.ericsson.nms.dg.schema.DataSpreadType;
import com.ericsson.nms.dg.schema.DataTypeType;
import com.ericsson.nms.dg.schema.DatabaseType;
import com.ericsson.nms.dg.schema.LoadIntervalType;
import com.ericsson.nms.dg.schema.RowsPerLoadType;
import com.ericsson.nms.dg.schema.TableDefType;
import com.ericsson.nms.dg.schema.TableType;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 25/10/13
 * Time: 10:43
 */
public class RopGenerator {


  private static Logger logger = LogManager.getLogger("DataGen");

  /**
   * Print the usage
   */
  private static void usage(final Options options) {
    final HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("DG", options);
  }


  private static CommandLine setupArgs(final String[] args) throws ParseException {
    final Options argopts = new Options();
    final Option oconfig = new Option("config", true, "RopGen server config (ropgen.properties)");
    oconfig.setType(String.class);
    oconfig.setArgName("config_file");
    oconfig.setRequired(true);

    final Option odb_config = new Option("db_config", true, "Database config (DB_<E|I|P|>.conf");
    odb_config.setType(String.class);
    odb_config.setArgName("config_file");
    odb_config.setRequired(true);

    final Option orsi = new Option("rsi", true, "Rop start index i.e. the ROP number to start generating from e.g. " +
        "-rsi=42 with start generating from rop-42.txt (inclusive)");
    orsi.setType(int.class);
    orsi.setArgName("index");
    orsi.setRequired(false);

    final Option omr = new Option("mr", true, "Max rops to generate before stopping (see -wait also)");
    omr.setType(int.class);
    omr.setArgName("max");
    omr.setRequired(false);

    final Option ocstate = new Option("cstate", true, "Continue on using previous cardinality state (Y|N). " +
        "If starting from scratch this should be N (-cstate=N). If the datagen is killed and you want to continue " +
        "from a certain point e.g. ROP-5 then -rsr=5 -cstate=Y");
    ocstate.setType(char.class);
    ocstate.setArgName("Y|N");
    ocstate.setRequired(false);

    final Option owait = new Option("window", false, "If using the -mr option, this option will cause the datagen to" +
        " wait for the earliest previous ROP to be deleted before generating the next ROP e.g. '-mr 10 -wait' will " +
        "generate rops 1..10 and then wait for ROP-1 to be deleted before generating ROP-11. This looks for the " +
        "rop-X.txt files to determine if a ROPs' data has been deleted.");
    owait.setRequired(false);


    argopts.addOption(oconfig);
    argopts.addOption(odb_config);
    argopts.addOption(orsi);
    argopts.addOption(omr);
    argopts.addOption(owait);
    argopts.addOption(ocstate);

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
    if (!options.hasOption("config")) {
      usage(argopts);
      System.exit(2);
    }
    if (!options.hasOption("db_config")) {
      usage(argopts);
      System.exit(2);
    }
    if (!options.hasOption("cstate") || !(options.getOptionValue("cstate").matches("Y|N"))) {
      System.out.println("No 'cstate' option specified, needs to one of Y|N" +
          "\n\t(Y: continue from last know state, N restart back at zero)");
      usage(argopts);
      System.exit(2);
    }

    return options;
  }


  private static List<File> getOutputDirectories(final Configuration ropgenConfig) {
    final String[] outputDirs = ropgenConfig.getString("rop_output_basedir").trim().split(":");
    final List<File> outputDirctories = new ArrayList<>(outputDirs.length);
    for (String dir : outputDirs) {
      final File ropDirectory = new File(dir);
      if (!ropDirectory.exists()) {
        if (!ropDirectory.mkdirs()) {
          throw new GenerateException("Couldn't create directory " + ropDirectory.getAbsoluteFile());
        }
      }
      outputDirctories.add(ropDirectory);
    }
    return outputDirctories;
  }

  public static void main(final String[] args) throws ParseException, ConfigurationException, IOException, InterruptedException {

    final CommandLine options = setupArgs(args);
    boolean reloadIndexerStates = false;
    if (options.getOptionValue("cstate").equals("Y")) {
      reloadIndexerStates = true;
    }


    final String configFile = options.getOptionValue("config");
    final String db_config = options.getOptionValue("db_config");
    final int ropStartIndex = Integer.valueOf(options.getOptionValue("rsi", "1"));
    final int maxRopCount = Integer.valueOf(options.getOptionValue("mr", "0"));

    final boolean ropgenWindow = options.hasOption("window");


    final Configuration ropgenConfig = new PropertiesConfiguration(configFile);
    final Configuration dbConfig = new PropertiesConfiguration(db_config);
    final String dbSchema = ropgenConfig.getString("table_def");
    final String loadingSchema = ropgenConfig.getString("data_spread");
    final String preGenDir = ropgenConfig.getString("sample_dir");
    final String ropDuration = ropgenConfig.getString("rop_duration");
    final String ropListDir = ropgenConfig.getString("rop_list_dir");
    final boolean initializeIndexers = ropgenConfig.getBoolean("indexers.initialize", true);
    final int maxFsUsage = Integer.valueOf(ropgenConfig.getString("max_fs_usage", "95"));


    final List<File> outputDirctories = getOutputDirectories(ropgenConfig);
    if (!System.getProperty("path.separator").equals(";")) {
      printFileSystemUsage(outputDirctories);
    }

    if (!System.getProperty("path.separator").equals(";")) {
      printFileSystemUsage(outputDirctories);
    }


    final boolean cacheData = ropgenConfig.getBoolean("cache.pregenerated.data", true);
    final boolean preCacheData = ropgenConfig.getBoolean("cache.precache.pregenerated.data", false);
    final List<String> preCacheIgnoreList = Arrays.asList(ropgenConfig.getStringArray("cache.precache.pregenerated.ignore"));

    final DataSpreadType loadingDef = DataConfigParser.parse(loadingSchema, DataConfigParser.SCHEMA_LOADDEF_FILE, DataSpreadType.class);
    final DatabaseType dbDef = DataConfigParser.parse(dbSchema, DataConfigParser.SCHEMA_TABLEDEF_FILE, DatabaseType.class);

    int threadPoolSize = 0;
    for (TableType table : dbDef.getTable()) {
      threadPoolSize += table.getNTables();
    }
    System.out.println("Max possible thread pool size is " + threadPoolSize);
    final int threadCount = ropgenConfig.getInt("rop.threads", -1);
    if (threadCount > 0) {
      threadPoolSize = threadCount;
      System.out.println("Setting thread pool size to " + threadPoolSize + " (rop.threads=" + threadCount + ")");
    }
    final int maxThreads = Integer.valueOf(System.getProperty("max.threads", "2"));
    if (threadPoolSize > maxThreads) {
      System.out.println("Limiting thread count to max.threads (" + maxThreads + ")");
      threadPoolSize = maxThreads;
    }
    System.out.println("Creating executor service for " + threadPoolSize + " parallel rop generators.");
    LoadIntervalType ropDef = null;
    for (LoadIntervalType rop : loadingDef.getRop()) {
      if (rop.getId().equalsIgnoreCase(ropDuration)) {
        ropDef = rop;
        break;
      }
    }
    if (ropDef == null) {
      throw new GenerateException("No ROP matching " + ropDuration + " found in " + loadingSchema);
    }

    long start = System.currentTimeMillis();
    final Map<String, LineAccessFile> pregenDataReaders = loadPregenerated(preGenDir, dbDef, preCacheData, cacheData,
        preCacheIgnoreList);
    log(Level.INFO, "Took " + (System.currentTimeMillis() - start) + "mSec to load " +
        pregenDataReaders.size() + " backing data files.");

    final Map<String, Indexer> dataIndexers = createIndexers(dbDef, pregenDataReaders, initializeIndexers);
    log(Level.INFO, "Took " + (System.currentTimeMillis() - start) + "mSec to load " +
        dataIndexers.size() + " indexers.");

    System.out.println("Registering LAF MBeans");
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (final Map.Entry<String, LineAccessFile> laf : pregenDataReaders.entrySet()) {
      try {
        final String jmxName = "com.ericsson.nms.datagen.LAF:type=" + laf.getKey();
        final ObjectName name = new ObjectName(jmxName);
        final LineAccessFileMBean bean = new LineAccessFileMBean(jmxName, laf.getValue());
        mbs.registerMBean(bean, name);
      } catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
        throw new GenerateException("Failed to regeister MBean for " + laf.getKey(), e);
      }
    }

    final Map<String, TableDefType> ropMap = new HashMap<>();
    for (TableDefType table : ropDef.getTable()) {
      int perc = 0;
      for (RowsPerLoadType rpl : table.getRowsPerLoad()) {
        perc += rpl.getTablePercent();
      }
      if (perc > 100) {
        throw new GenerateException("Total percentage " + perc + " > 100% for tables types '" +
            table.getType() + "' in ROP '" + ropDef.getId() + "'");
      }
      ropMap.put(table.getType(), table);
    }
    final Map<String, TableROPGenerator> ropGenerators = getRopGenerators(dbDef, dataIndexers, ropMap, ropgenConfig, dbConfig);

    System.out.println("Registering RopGen MBeans");
    try {
      for (final Map.Entry<String, TableROPGenerator> ropg : ropGenerators.entrySet()) {
        try {
          final String jmxName = "com.ericsson.nms.datagen.ROPGEN:type=" + ropg.getKey();
          final ObjectName name = new ObjectName(jmxName);
          final ROPGeneratorMBean bean = new ROPGeneratorMBean(jmxName, ropg.getValue());
          mbs.registerMBean(bean, name);
        } catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
          throw new GenerateException("Failed to regeister MBean for " + ropg.getKey(), e);
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
    System.out.println("Done");

    System.out.println("Creating tasks...");
    final List<RopTask> tasks = new ArrayList<>(ropGenerators.size());
    for (final Map.Entry<String, TableROPGenerator> generator : ropGenerators.entrySet()) {
      tasks.add(new RopTask(generator.getKey(), generator.getValue()));
    }


    System.out.println("Registering server MBean");
    final RopServerMBean serverBean = new RopServerMBean(tasks);
    try {
      final ObjectName name = new ObjectName("com.ericsson.nms.datagen.ROPSERV:type=ROPServer");
      mbs.registerMBean(serverBean, name);
    } catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
      throw new GenerateException("Failed to regeister MBean for ROP Server", e);
    }

    System.out.println("Starting thread-pool watcher ...");
    final AtomicReference<ThreadPoolExecutor> monitorObject = new AtomicReference<>(null);
    final Thread esMonitor = new Thread() {
      @Override
      public void run() {
        while (true) {
          final ThreadPoolExecutor pool = monitorObject.get();
          if (pool != null) {
            System.out.println(
                String.format("[monitor] [%d/%d] Active: %d, Completed: %d, Task: %d, isShutdown: %s, isTerminated: %s",
                    pool.getPoolSize(),
                    pool.getCorePoolSize(),
                    pool.getActiveCount(),
                    pool.getCompletedTaskCount(),
                    pool.getTaskCount(),
                    pool.isShutdown(),
                    pool.isTerminated()));
          }
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {/**/}
        }
      }
    };
    esMonitor.setDaemon(true);
    esMonitor.start();
    int ropIndex = ropStartIndex;
    int ropsGenerated = 0;
    if (reloadIndexerStates) {
      loadIndexerState(dataIndexers, preGenDir);
    }
    final File stopFlag = new File(ropListDir, "stopgen");
    while (true) {
      if (shouldStopDataGen(maxRopCount, ropgenWindow, ropsGenerated, ropListDir, ropStartIndex, stopFlag)) {
        break;
      }
      start = System.currentTimeMillis();
      System.out.println("RG:============================================================================");
      System.out.println("RG: Starting ROP-" + ropIndex + " @ " + new Date());
      System.out.println("RG:============================================================================");
      try {
        generateROP(ropIndex, threadPoolSize, maxFsUsage, monitorObject, outputDirctories, tasks, ropListDir);
      } catch (Throwable t) {
        t.printStackTrace();
        break;
      }
      final long ropGenTime = System.currentTimeMillis() - start;
      final double mins = ((double) ropGenTime / 1000) / 60;
      System.out.println("RG:============================================================================");
      System.out.println("RG: Finished ROP-" + ropIndex + " @ " + new Date());
      System.out.println("RG: ROP-" + ropIndex + " took " + ropGenTime + " msec (" + mins + " minutes)");
      System.out.println("RG:============================================================================");
      ropIndex++;
      ropsGenerated++;
      serverBean.incrementRops();
      serverBean.incrementTotal(ropGenTime);
      saveIndexerState(dataIndexers, preGenDir);
    }
  }

  private static boolean shouldStopDataGen(final int maxRopCount, final boolean ropgenWindow, final int ropsGenerated,
                                           final String ropListDir, final int ropStartIndex, final File stopFlag) {
    boolean stop = false;
    if (maxRopCount != 0) {
      if (ropgenWindow && ropsGenerated >= maxRopCount) {
        final File watchFile;
        if (maxRopCount == 1) {
          watchFile = getRopReportFile(ropListDir, ropsGenerated);
        } else {
          int idex;
          if (ropStartIndex == 1) {
            idex = ropsGenerated - maxRopCount;
          } else {
            idex = (ropsGenerated - maxRopCount) + ropStartIndex;
          }
          if (idex <= 0) {
            idex = 1;
          }
          watchFile = getRopReportFile(ropListDir, idex);
        }
        if (watchFile.exists()) {
          System.out.println("Waiting for " + watchFile.getAbsolutePath() + " to be deleted before generating next ROP");
          while (watchFile.exists() && !stopFlag.exists()) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {/**/}
          }
        }
      } else if (ropsGenerated >= maxRopCount) {
        System.out.println("Max rop count (-mr=" + maxRopCount + ") has been reached, stopping ROP datagen");
        stop = true;
      }
    }
    if (stopFlag.exists()) {
      System.out.println("Stop flagfile " + stopFlag.getAbsolutePath() + " found, stopping datagen.");
      stop = true;
    }
    return stop;
  }

  private static void saveIndexerState(final Map<String, Indexer> indexers, final String sampleDataDir) throws IOException {
    final File stateFile = new File(sampleDataDir, ".state");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(stateFile, false))) {
      for (Map.Entry<String, Indexer> indexer : indexers.entrySet()) {
        writer.write(indexer.getKey()
            + "@" + indexer.getValue().getRopCardinalityStartIndex()
            + "@" + indexer.getValue().getRopCardinalityEndIndex()
            + "@" + indexer.getValue().getGeneratedCardinality()
            + "@" + indexer.getValue().getGeneratedCount());
        writer.newLine();
      }
    }
  }

  private static void loadIndexerState(final Map<String, Indexer> indexers, final String sampleDataDir) throws IOException {
    final File stateFile = new File(sampleDataDir, ".state");
    if (stateFile.exists()) {
      System.out.println("Loading indexer state from " + stateFile.getAbsolutePath());
      try (BufferedReader reader = new BufferedReader(new FileReader(stateFile))) {
        String line;
        while ((line = reader.readLine()) != null) {
          final String[] bits = line.split("@");
          if (indexers.containsKey(bits[0])) {
            final Indexer indexer = indexers.get(bits[0]);
            indexer.setRopCardinalityStartIndex(Integer.valueOf(bits[1]));
            indexer.setRopCardinalityEndIndex(Integer.valueOf(bits[2]));
            indexer.setGeneratedCardinality(Integer.valueOf(bits[3]));
            indexer.setGeneratedCount(Long.valueOf(bits[4]));
          }
        }
      }
    }
  }

  private static void generateROP(final int ropIndex, final int threadPoolSize, final int maxFsUsage,
                                  final AtomicReference<ThreadPoolExecutor> monitorObject,
                                  final List<File> outputDirctories,
                                  final List<RopTask> tasks,
                                  final String ropListDir) throws IOException, InterruptedException {
    final ThreadPoolExecutor executorService = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    monitorObject.set(executorService);

    final List<File> dirs = new ArrayList<>(outputDirctories);
    Iterator<File> mountIterator = dirs.iterator();
    if (!System.getProperty("path.separator").equals(";")) {
      printFileSystemUsage(outputDirctories);
      while (mountIterator.hasNext()) {
        final File path = mountIterator.next();
        final Map<String, String> details = Util.getFileSystemInfo(path.getAbsolutePath());
        if (Integer.valueOf(details.get("used_percent")) >= maxFsUsage) {
          System.out.println("RG:Filesystem " + details.get("filesystem") + " usage >= 90%, removing (" + details.toString() + ")");
          mountIterator.remove();
        }
      }
      if (dirs.isEmpty()) {
        throw new GenerateException("RG:No more usabled storage locations!!!!", 404);
      }
    }

    mountIterator = dirs.iterator();
    for (RopTask task : tasks) {
      if (!mountIterator.hasNext()) {
        mountIterator = dirs.iterator();
      }
      final File ropDir = new File(mountIterator.next(), Integer.toString(ropIndex));
      final File generatorOutputDirectory = new File(ropDir, task.getId());
      if (!generatorOutputDirectory.exists() && !generatorOutputDirectory.mkdirs()) {
        throw new GenerateException("Failed to create directory " + generatorOutputDirectory.getAbsolutePath());
      }
      task.setOutputDir(generatorOutputDirectory);
      executorService.submit(task);
    }
    executorService.shutdown();
    while (!executorService.isTerminated()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {/**/}
    }
    monitorObject.set(null);

    final File ropInfoFile = getRopReportFile(ropListDir, ropIndex);
    try (final BufferedWriter w = new BufferedWriter(new FileWriter(ropInfoFile, false))) {
      for (RopTask task : tasks) {
        if (task.getError() != null) {
          System.out.println("Task " + task.getId() + " generated the following error:");
          task.getError().printStackTrace();
        }
        for (Map.Entry<String, Integer> file : task.getGeneratedFiles().entrySet()) {
          w.write(task.getGeneratedTable() + ":" + file.getKey() + ":" + file.getValue());
          w.newLine();
        }
      }
    }
    System.out.println("RG: ROP info file " + ropInfoFile.getAbsolutePath());
  }


  private static File getRopReportFile(final String ropListDir, final int ropIndex) {
    return new File(ropListDir, "rop-" + ropIndex + ".txt");
  }

  private static Map<String, TableROPGenerator> getRopGenerators(final DatabaseType dbDef,
                                                                 final Map<String, Indexer> dataIndexers,
                                                                 final Map<String, TableDefType> tableSpread,
                                                                 final Configuration ropgenConfig,
                                                                 final Configuration dbConfig) {
    final Map<String, TableROPGenerator> ropGenerators = new HashMap<>();
    for (TableType table : dbDef.getTable()) {
      int tableInstanceCount = 1;
      if (!tableSpread.containsKey(table.getId())) {
        throw new GenerateException("No data spread info found for table type " + table.getId());
      }
      final TableDefType dataSpread = tableSpread.get(table.getId());
      for (RowsPerLoadType rpl : dataSpread.getRowsPerLoad()) {
        if (rpl.getTablePercent() == 0 || rpl.getRowCount() == 0) {
          continue;
        }
        final double tiCount = Math.ceil(((double) table.getNTables() / 100) * rpl.getTablePercent());
        for (int index = 1; index <= tiCount; index++) {
          final String tableId = Util.generateName(table, tableInstanceCount);
          System.out.println("Creating ROP generator for table " + tableId + " which generates " + rpl.getRowCount() + " rows per ROP");
          final TableROPGenerator trg = new TableROPGenerator(table, tableInstanceCount, rpl.getRowCount(),
              dataIndexers, ropgenConfig, dbConfig);
          tableInstanceCount++;
          ropGenerators.put(tableId, trg);
        }
      }
    }
    return ropGenerators;
  }

  public static Map<String, Indexer> createIndexers(final DatabaseType dbDef,
                                                    final Map<String, LineAccessFile> pregenDataReaders,
                                                    final boolean initializeIndexers) {
    assert dbDef != null : "DatabaseType can't be NULL";
    assert pregenDataReaders != null : "LineAccessFile map can't be NULL";
    final Map<String, Indexer> dataIndexers = new HashMap<>();
    for (TableType table : dbDef.getTable()) {
      for (int tableInstance = 1; tableInstance <= table.getNTables(); tableInstance++) {
        for (ColumnType column : table.getColumn()) {
          for (int columnIndex = 0; columnIndex <= column.getNColumns(); columnIndex++) {
            final String indexerId = Util.getTableColumnDataID(table, tableInstance, column, columnIndex);
            final Indexer indexer = Indexers.getIndexer(indexerId, column.getDataType());
            final LineAccessFile backingData;
            final String dataId = Util.getDataTypeID(column.getDataType());
            if (pregenDataReaders.containsKey(dataId)) {
              backingData = pregenDataReaders.get(dataId);
            } else {
              backingData = null;
            }
            if (initializeIndexers) {
              indexer.initData(backingData);
            }
            final String indexerTableDataId = Util.getTableColumnDataID(table, tableInstance, column, columnIndex);
            log(Level.TRACE, "Defining indexer for " + indexerTableDataId);
            dataIndexers.put(indexerTableDataId, indexer);
          }
        }
      }
    }
    return dataIndexers;
  }

  public static Map<String, LineAccessFile> loadPregenerated(final String dataDir, final DatabaseType dbDef,
                                                             final boolean precacheData, final boolean cacheData,
                                                             final List<String> precacheIgnore) {
    final ExecutorService executorService = Executors.newFixedThreadPool(5);
    final Map<String, LineAccessFile> indexers = new HashMap<>();
    final List<String> tracker = new ArrayList<>();

    for (TableType table : dbDef.getTable()) {
      for (ColumnType column : table.getColumn()) {
        final DataTypeType dataType = column.getDataType();
        final String dataID = Util.getDataTypeID(dataType);
        if (tracker.contains(dataID)) {
          continue;
        }
        tracker.add(dataID);
        final File pregenFile = Util.getDataIDFile(dataType, dataDir);
        if (!pregenFile.exists()) {
          log(Level.TRACE, "No pregenerated data available for " + dataID);
          continue;
        }
        executorService.submit(
            new Runnable() {
              @Override
              public void run() {
                System.out.println("Loading ID: " + dataID + " from " + pregenFile.getAbsolutePath());
                final LineAccessFile laf = new LineAccessFile(dataID, pregenFile, cacheData);
                if (precacheData && !precacheIgnore.contains(dataID)) {
                  laf.precacheData();
                }
                indexers.put(dataID, laf);
                System.out.println("Loaded " + pregenFile.getAbsolutePath());
              }
            }
        );
      }
    }
    log(Level.TRACE, "Waiting for all pre-generated files to load ...");
    executorService.shutdown(); // Disable new tasks from being submitted
    while (!executorService.isTerminated()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {/**/}
    }
    log(Level.TRACE, "Loaded " + indexers.size() + " pregenerated data files");
    return indexers;
  }

  private static void log(final Level level, final String message) {
    if (logger.isEnabled(level)) {
      logger.log(level, message);
    }
  }

  private static void printFileSystemUsage(final List<File> mounts) throws IOException, InterruptedException {
    System.out.println("Filesystem usage:");
    try {
      for (File path : mounts) {
        final Map<String, String> details = Util.getFileSystemInfo(path.getAbsolutePath());
        System.out.println("\t" + path.getAbsolutePath());
        for (Map.Entry<String, String> attrs : details.entrySet()) {
          System.out.println("\t\t" + attrs.getKey() + " : " + attrs.getValue());
        }
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
