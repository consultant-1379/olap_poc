package com.ericsson.nms.dg.dbquery;

import com.ericsson.nms.dg.DDLApplyException;
import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.ddl.DDLWriter;
import com.ericsson.nms.dg.ddlapply.DBConnectionPool;
import com.ericsson.nms.dg.ddlapply.SQLExecutionTimer;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;

/**
 * User: ealferg
 * Date: 11/12/13
 * Time: 13:28
 */
public class SmallQueryLoadDB {


    private static String DB_NAME;
    private static int THREAD_POOL_SIZE;
    private static String OUTPUT_FILE_LOCATION;
    private static String OS_ENV_DELIMETER;
    private static Properties prop = new Properties();
    private static int query_total;
    private static String tab_size ;
    private static String tab_type ;
    private static List<String> cmdList = new ArrayList<>();
    private static List<String> table_name_List = new ArrayList<>();
    private static int profileNO;
    private static String DRIV;
    private static String URL;
    private static String USER;
    private static int QUERY_EXECUTION_TEST;
    private static String PASSWORD;




    public SmallQueryLoadDB() {
    }

    public static void retrieveProperties(String propFile) {
        // load a properties file
        try {
            prop.load(new FileInputStream(propFile));
        } catch (IOException e1) {
            throw new DDLApplyException("Couldn't locate the property file :\n", e1);
        }

        DB_NAME = prop.getProperty("db_name");
        USER = prop.getProperty("userid");
        PASSWORD = prop.getProperty("password");
        URL = prop.getProperty("url");
        DRIV = prop.getProperty("driver");
        OUTPUT_FILE_LOCATION = prop.getProperty("output_fileLocation");
        THREAD_POOL_SIZE = Integer.parseInt(prop.getProperty("thread_pool_size"));
        String del = prop.getProperty("os_env");
        if (del.equalsIgnoreCase("windows")) {
            OS_ENV_DELIMETER = "\\";
        } else {
            OS_ENV_DELIMETER = "/";
        }

    }

    public static void writeProperties(String propFile, String property, String value) {
        // load a properties file
        try {

            FileInputStream in = new FileInputStream(propFile);
            Properties props = new Properties();
            props.load(in);
            in.close();

            FileOutputStream out = new FileOutputStream(propFile);
            props.setProperty(property, value);
            props.store(out, null);
            out.close();
        } catch (IOException e1) {
            throw new DDLApplyException("Couldn't locate the property file :\n", e1);
        }
    }

    public static void main(String[] args) throws ParseException {

        if (args == null || args.length == 0) {
            usage();
            System.exit(0);
        }
        final Map<String, String> options = processArgs(args);
        if (!options.containsKey("-p")) {
            System.out.println("No Query Profile Specified!");
            usage();
            System.exit(0);
        }
        if (!options.containsKey("-o")) {
            System.out.println("No output directory specified!");
            usage();
            System.exit(0);
        }
        if (!options.containsKey("-c")) {
            System.out.println("No config file specified!");
            usage();
            System.exit(0);
        }
        if (!options.containsKey("-t")) {
            System.out.println("config to indicate testing not set (1 testing {random} - 0 no testing {count(*)})");
            usage();
            System.exit(0);
        }
        if (!options.containsKey("-f")) {
            System.out.println("full or light load not specified!");
            usage();
            System.exit(0);
        }
        final String outputDirectory = options.get("-o");
        final File oDir = new File(outputDirectory);
        if (!oDir.exists()) {
            if (!oDir.mkdirs()) {
                throw new GenerateException(new IOException("Failed to create output directory " + oDir.getAbsolutePath()));
            }
        }
        final String propFile = options.get("-c");
        final String full_not = options.get("-f");
        QUERY_EXECUTION_TEST = parseInt(options.get("-t"));
        profileNO = Integer.parseInt(options.get("-p"));
        switch (profileNO) {
            case 1:
                tab_size = "el";
                tab_type = "narrow";
                int concurrent;
                if (full_not.equalsIgnoreCase("full")) {
                    query_total = 675;
                    concurrent = 122;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                } else {
                    query_total = 388;
                    concurrent = 70;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                }
                break;
            case 2:
                tab_size = "el";
                tab_type = "narrow";
                if (full_not.equalsIgnoreCase("full")) {
                    query_total = 38;
                    concurrent = 1;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                } else {
                    query_total = 19;
                    concurrent = 1;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                }
                break;
            case 3:
                tab_size = "l";
                tab_type = "narrow";
                if (full_not.equalsIgnoreCase("full")) {
                    query_total = 113;
                    concurrent = 4;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                } else {
                    query_total = 57;
                    concurrent = 2;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                }
                break;
            case 4:
                tab_size = "el";
                tab_type = "medium";
                if (full_not.equalsIgnoreCase("full")) {
                    query_total = 75;
                    concurrent = 2;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                } else {
                    query_total = 38;
                    concurrent = 1;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                }
                break;
            case 5:
                tab_size = "l";
                tab_type = "medium";
                if (full_not.equalsIgnoreCase("full")) {
                    query_total = 225;
                    concurrent = 15;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                } else {
                    query_total = 113;
                    concurrent = 7;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                }
                break;
            case 6:
                tab_size = "el";
                tab_type = "wide";
                if (full_not.equalsIgnoreCase("full")) {
                    query_total = 38;
                    concurrent = 1;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                } else {
                    query_total = 19;
                    concurrent = 1;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                }
            case 7:
                tab_size = "l";
                tab_type = "wide";
                if (full_not.equalsIgnoreCase("full")) {
                    query_total = 38;
                    concurrent = 1;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                } else {
                    query_total = 19;
                    concurrent = 1;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                }
                break;
            case 8:
                tab_size = "el";
                tab_type = "narrow";
                if (full_not.equalsIgnoreCase("full")) {
                    query_total = 1;
                    concurrent = 1;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                } else {
                    query_total = 1;
                    concurrent = 1;
                    writeProperties(propFile, "max_connections", String.valueOf(concurrent));
                }
                break;
        }


        final SmallQueryLoadDB adid = new SmallQueryLoadDB();
        final DataSource ds;
        try {
            DBConnectionPool dbcpool = new DBConnectionPool(propFile);
            ds = dbcpool.setUp();
        } catch (Exception e) {
            throw new DDLApplyException("Couldn't get data source and connection pool :\n", e);
        }
        retrieveProperties(propFile);

        try {
            generateSQL(tab_size, tab_type, query_total);
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        adid.processDDLCommands(ds);
        System.out.println("Finished all threads");
    }

    private static void generateSQL(String tab_size, String tab_type, int query_total) throws ParseException {

        List<String> tab_cols = new ArrayList<>();
        List<String> colList = new ArrayList<>();
        Random rng = new Random();
        Set<Integer> col_num_List = new LinkedHashSet<>();

        String date_line = "BETWEEN ";
        String date_line2 = " AND ";
        if (DB_NAME.equalsIgnoreCase("infobright")) {
            date_line = "BETWEEN ";
            date_line2 = " AND ";
        } else if (DB_NAME.equalsIgnoreCase("EXASOL_DB")) {
            date_line = "BETWEEN TIMESTAMP ";
            date_line2 = " AND TIMESTAMP ";
        } else if (DB_NAME.equalsIgnoreCase("paraccel")) {

            date_line = "BETWEEN ";
            date_line2 = " AND ";
        }

        int i = 0;
        do {
            int tab_num = randomTableNumber(tab_type, tab_size);
            //int tab_num = 66;

            String tab_name = tab_type + "_table_" + tab_num;
            table_name_List.add(tab_name);
           /* if (tab_type.equalsIgnoreCase("narrow")) {
                while (colList.size() < 51) {
                    colList.add("column_string1_col__1_1");
                    colList.add("column_string2_col__1_1");
                    colList.add("column_string3_col__1_1");
                    colList.add("column_string4_col__1_1");
                    colList.add("column_string5_col__1_1");
                    colList.add("column_string6_col__1_1");
                    colList.add("column_string7_col__1_1");
                    colList.add("column_string8_col__1_1");
                    colList.add("column_int1_col__4_1");
                    colList.add("column_int1_col__4_2");
                    colList.add("column_int1_col__4_3");
                    colList.add("column_int1_col__4_4");
                    colList.add("column_int2_col__2_1");
                    colList.add("column_int2_col__2_2");
                    colList.add("column_int3_col__1_1");
                    colList.add("column_int4_col__1_1");
                    colList.add("column_long1_col__4_1");
                    colList.add("column_long1_col__4_2");
                    colList.add("column_long1_col__4_3");
                    colList.add("column_long1_col__4_4");
                    colList.add("column_long2_col__2_1");
                    colList.add("column_long2_col__2_2");
                    colList.add("column_long3_col__1_1");
                    colList.add("column_long4_col__1_1");
                    colList.add("column_string9_col__1_1");
                    colList.add("column_string10_col__1_1");
                    colList.add("column_string16_col__1_1");
                    colList.add("column_int5_col__4_1");
                    colList.add("column_int5_col__4_2");
                    colList.add("column_int5_col__4_3");
                    colList.add("column_int5_col__4_4");
                    colList.add("column_int6_col__1_1");
                    colList.add("column_int7_col__1_1");
                    colList.add("column_int8_col__2_1");
                    colList.add("column_int8_col__2_2");
                    colList.add("column_string11_col__1_1");
                    colList.add("column_string12_col__1_1");
                    colList.add("column_string13_col__1_1");
                    colList.add("column_string14_col__1_1");
                    colList.add("column_string15_col__1_1");
                    colList.add("column_long5_col__4_1");
                    colList.add("column_long5_col__4_2");
                    colList.add("column_long5_col__4_3");
                    colList.add("column_long5_col__4_4");
                    colList.add("column_long6_col__1_1");
                    colList.add("column_long7_col__1_1");
                    colList.add("column_long8_col__2_1");
                    colList.add("column_long8_col__2_2");
                    colList.add("column_datetime1_col__1_1");
                    colList.add("column_datetime2_col__1_1");
                    colList.add("column_datetime3_col__1_1");
                }
                while (col_num_List.size() < 20) {
                    Integer next = (rng.nextInt(50) + 1) - 1;
                    // As we're adding to a set, this will automatically do a containment check
                    col_num_List.add(next);
                }

            } else if (tab_type.equalsIgnoreCase("medium")) {
                while (colList.size() < 155) {
                    colList.add("column_string1_col__4_1");
                    colList.add("column_string1_col__4_2");
                    colList.add("column_string1_col__4_3");
                    colList.add("column_string1_col__4_4");
                    colList.add("column_string2_col__2_1");
                    colList.add("column_string2_col__2_2");
                    colList.add("column_string3_col__1_1");
                    colList.add("column_string4_col__1_1");
                    colList.add("column_string5_col__4_1");
                    colList.add("column_string5_col__4_2");
                    colList.add("column_string5_col__4_3");
                    colList.add("column_string5_col__4_4");
                    colList.add("column_string6_col__2_1");
                    colList.add("column_string6_col__2_2");
                    colList.add("column_string7_col__1_1");
                    colList.add("column_string8_col__1_1");
                    colList.add("column_int1_col__5_1");
                    colList.add("column_int1_col__5_2");
                    colList.add("column_int1_col__5_3");
                    colList.add("column_int1_col__5_4");
                    colList.add("column_int1_col__5_5");
                    colList.add("column_int2_col__5_1");
                    colList.add("column_int2_col__5_2");
                    colList.add("column_int2_col__5_3");
                    colList.add("column_int2_col__5_4");
                    colList.add("column_int2_col__5_5");
                    colList.add("column_int3_col__5_1");
                    colList.add("column_int3_col__5_2");
                    colList.add("column_int3_col__5_3");
                    colList.add("column_int3_col__5_4");
                    colList.add("column_int3_col__5_5");
                    colList.add("column_int4_col__5_1");
                    colList.add("column_int4_col__5_2");
                    colList.add("column_int4_col__5_3");
                    colList.add("column_int4_col__5_4");
                    colList.add("column_int4_col__5_5");
                    colList.add("column_long1_col__10_1");
                    colList.add("column_long1_col__10_2");
                    colList.add("column_long1_col__10_3");
                    colList.add("column_long1_col__10_4");
                    colList.add("column_long1_col__10_5");
                    colList.add("column_long1_col__10_6");
                    colList.add("column_long1_col__10_7");
                    colList.add("column_long1_col__10_8");
                    colList.add("column_long1_col__10_9");
                    colList.add("column_long1_col__10_10");
                    colList.add("column_long2_col__10_1");
                    colList.add("column_long2_col__10_2");
                    colList.add("column_long2_col__10_3");
                    colList.add("column_long2_col__10_4");
                    colList.add("column_long2_col__10_5");
                    colList.add("column_long2_col__10_6");
                    colList.add("column_long2_col__10_7");
                    colList.add("column_long2_col__10_8");
                    colList.add("column_long2_col__10_9");
                    colList.add("column_long2_col__10_10");
                    colList.add("column_long3_col__10_1");
                    colList.add("column_long3_col__10_2");
                    colList.add("column_long3_col__10_3");
                    colList.add("column_long3_col__10_4");
                    colList.add("column_long3_col__10_5");
                    colList.add("column_long3_col__10_6");
                    colList.add("column_long3_col__10_7");
                    colList.add("column_long3_col__10_8");
                    colList.add("column_long3_col__10_9");
                    colList.add("column_long3_col__10_10");
                    colList.add("column_long4_col__10_1");
                    colList.add("column_long4_col__10_2");
                    colList.add("column_long4_col__10_3");
                    colList.add("column_long4_col__10_4");
                    colList.add("column_long4_col__10_5");
                    colList.add("column_long4_col__10_6");
                    colList.add("column_long4_col__10_7");
                    colList.add("column_long4_col__10_8");
                    colList.add("column_long4_col__10_9");
                    colList.add("column_long4_col__10_10");
                    colList.add("column_string9_col__4_1 ");
                    colList.add("column_string9_col__4_2 ");
                    colList.add("column_string9_col__4_3 ");
                    colList.add("column_string9_col__4_4");
                    colList.add("column_string10_col__1_1");
                    colList.add("column_string11_col__1_1");
                    colList.add("column_string12_col__2_1");
                    colList.add("column_string12_col__2_2");
                    colList.add("column_string13_col__4_1");
                    colList.add("column_string13_col__4_2");
                    colList.add("column_string13_col__4_3");
                    colList.add("column_string13_col__4_4");
                    colList.add("column_string14_col__1_1");
                    colList.add("column_string15_col__1_1");
                    colList.add("column_string16_col__2_1");
                    colList.add("column_string16_col__2_2");
                    colList.add("column_int5_col__5_1");
                    colList.add("column_int5_col__5_2");
                    colList.add("column_int5_col__5_3");
                    colList.add("column_int5_col__5_4");
                    colList.add("column_int5_col__5_5");
                    colList.add("column_int6_col__5_1");
                    colList.add("column_int6_col__5_2");
                    colList.add("column_int6_col__5_3");
                    colList.add("column_int6_col__5_4");
                    colList.add("column_int6_col__5_5");
                    colList.add("column_int7_col__5_1");
                    colList.add("column_int7_col__5_2");
                    colList.add("column_int7_col__5_3");
                    colList.add("column_int7_col__5_4");
                    colList.add("column_int7_col__5_5");
                    colList.add("column_int8_col__5_1");
                    colList.add("column_int8_col__5_2");
                    colList.add("column_int8_col__5_3");
                    colList.add("column_int8_col__5_4");
                    colList.add("column_int8_col__5_5");
                    colList.add("column_long5_col__10_1");
                    colList.add("column_long5_col__10_2");
                    colList.add("column_long5_col__10_3");
                    colList.add("column_long5_col__10_4");
                    colList.add("column_long5_col__10_5");
                    colList.add("column_long5_col__10_6");
                    colList.add("column_long5_col__10_7");
                    colList.add("column_long5_col__10_8");
                    colList.add("column_long5_col__10_9");
                    colList.add("column_long5_col__10_10");
                    colList.add("column_long6_col__10_1");
                    colList.add("column_long6_col__10_2");
                    colList.add("column_long6_col__10_3");
                    colList.add("column_long6_col__10_4");
                    colList.add("column_long6_col__10_5");
                    colList.add("column_long6_col__10_6");
                    colList.add("column_long6_col__10_7");
                    colList.add("column_long6_col__10_8");
                    colList.add("column_long6_col__10_9");
                    colList.add("column_long6_col__10_10");
                    colList.add("column_long7_col__10_1");
                    colList.add("column_long7_col__10_2");
                    colList.add("column_long7_col__10_3");
                    colList.add("column_long7_col__10_4");
                    colList.add("column_long7_col__10_5");
                    colList.add("column_long7_col__10_6");
                    colList.add("column_long7_col__10_7");
                    colList.add("column_long7_col__10_8");
                    colList.add("column_long7_col__10_9");
                    colList.add("column_long7_col__10_10");
                    colList.add("column_long8_col__10_1");
                    colList.add("column_long8_col__10_2");
                    colList.add("column_long8_col__10_3");
                    colList.add("column_long8_col__10_4");
                    colList.add("column_long8_col__10_5");
                    colList.add("column_long8_col__10_6");
                    colList.add("column_long8_col__10_7");
                    colList.add("column_long8_col__10_8");
                    colList.add("column_long8_col__10_9");
                    colList.add("column_long8_col__10_10");
                    colList.add("column_datetime1_col__1_1");
                    colList.add("column_datetime2_col__1_1");
                    colList.add("column_datetime3_col__1_1");
                }
                while (col_num_List.size() < 40) {
                    Integer next = (rng.nextInt(154) + 1) - 1;
                    // As we're adding to a set, this will automatically do a containment check
                    col_num_List.add(next);
                }

            } else {
                while (colList.size() < 955) {
                    colList.add("column_string1_col__4_1");
                    colList.add("column_string1_col__4_2");
                    colList.add("column_string1_col__4_3");
                    colList.add("column_string1_col__4_4");
                    colList.add("column_string2_col__2_1");
                    colList.add("column_string2_col__2_2");
                    colList.add("column_string3_col__1_1");
                    colList.add("column_string4_col__1_1");
                    colList.add("column_string5_col__4_1");
                    colList.add("column_string5_col__4_2");
                    colList.add("column_string5_col__4_3");
                    colList.add("column_string5_col__4_4");
                    colList.add("column_string6_col__2_1");
                    colList.add("column_string6_col__2_2");
                    colList.add("column_string7_col__1_1");
                    colList.add("column_string8_col__1_1");
                    colList.add("column_int1_col__40_1");
                    colList.add("column_int1_col__40_2");
                    colList.add("column_int1_col__40_3");
                    colList.add("column_int1_col__40_4");
                    colList.add("column_int1_col__40_5");
                    colList.add("column_int1_col__40_6");
                    colList.add("column_int1_col__40_7");
                    colList.add("column_int1_col__40_8");
                    colList.add("column_int1_col__40_9");
                    colList.add("column_int1_col__40_10");
                    colList.add("column_int1_col__40_11");
                    colList.add("column_int1_col__40_12");
                    colList.add("column_int1_col__40_13");
                    colList.add("column_int1_col__40_14");
                    colList.add("column_int1_col__40_15");
                    colList.add("column_int1_col__40_16");
                    colList.add("column_int1_col__40_17");
                    colList.add("column_int1_col__40_18");
                    colList.add("column_int1_col__40_19");
                    colList.add("column_int1_col__40_20");
                    colList.add("column_int1_col__40_21");
                    colList.add("column_int1_col__40_22");
                    colList.add("column_int1_col__40_23");
                    colList.add("column_int1_col__40_24");
                    colList.add("column_int1_col__40_25");
                    colList.add("column_int1_col__40_26");
                    colList.add("column_int1_col__40_27");
                    colList.add("column_int1_col__40_28");
                    colList.add("column_int1_col__40_29");
                    colList.add("column_int1_col__40_30");
                    colList.add("column_int1_col__40_31");
                    colList.add("column_int1_col__40_32");
                    colList.add("column_int1_col__40_33");
                    colList.add("column_int1_col__40_34");
                    colList.add("column_int1_col__40_35");
                    colList.add("column_int1_col__40_36");
                    colList.add("column_int1_col__40_37");
                    colList.add("column_int1_col__40_38");
                    colList.add("column_int1_col__40_39");
                    colList.add("column_int1_col__40_40");
                    colList.add("column_int2_col__40_1");
                    colList.add("column_int2_col__40_2");
                    colList.add("column_int2_col__40_3");
                    colList.add("column_int2_col__40_4");
                    colList.add("column_int2_col__40_5");
                    colList.add("column_int2_col__40_6");
                    colList.add("column_int2_col__40_7");
                    colList.add("column_int2_col__40_8");
                    colList.add("column_int2_col__40_9");
                    colList.add("column_int2_col__40_10");
                    colList.add("column_int2_col__40_11");
                    colList.add("column_int2_col__40_12");
                    colList.add("column_int2_col__40_13");
                    colList.add("column_int2_col__40_14");
                    colList.add("column_int2_col__40_15");
                    colList.add("column_int2_col__40_16");
                    colList.add("column_int2_col__40_17");
                    colList.add("column_int2_col__40_18");
                    colList.add("column_int2_col__40_19");
                    colList.add("column_int2_col__40_20");
                    colList.add("column_int2_col__40_21");
                    colList.add("column_int2_col__40_22");
                    colList.add("column_int2_col__40_23");
                    colList.add("column_int2_col__40_24");
                    colList.add("column_int2_col__40_25");
                    colList.add("column_int2_col__40_26");
                    colList.add("column_int2_col__40_27");
                    colList.add("column_int2_col__40_28");
                    colList.add("column_int2_col__40_29");
                    colList.add("column_int2_col__40_30");
                    colList.add("column_int2_col__40_31");
                    colList.add("column_int2_col__40_32");
                    colList.add("column_int2_col__40_33");
                    colList.add("column_int2_col__40_34");
                    colList.add("column_int2_col__40_35");
                    colList.add("column_int2_col__40_36");
                    colList.add("column_int2_col__40_37");
                    colList.add("column_int2_col__40_38");
                    colList.add("column_int2_col__40_39");
                    colList.add("column_int2_col__40_40");
                    colList.add("column_int3_col__40_1");
                    colList.add("column_int3_col__40_2");
                    colList.add("column_int3_col__40_3");
                    colList.add("column_int3_col__40_4");
                    colList.add("column_int3_col__40_5");
                    colList.add("column_int3_col__40_6");
                    colList.add("column_int3_col__40_7");
                    colList.add("column_int3_col__40_8");
                    colList.add("column_int3_col__40_9");
                    colList.add("column_int3_col__40_10");
                    colList.add("column_int3_col__40_11");
                    colList.add("column_int3_col__40_12");
                    colList.add("column_int3_col__40_13");
                    colList.add("column_int3_col__40_14");
                    colList.add("column_int3_col__40_15");
                    colList.add("column_int3_col__40_16");
                    colList.add("column_int3_col__40_17");
                    colList.add("column_int3_col__40_18");
                    colList.add("column_int3_col__40_19");
                    colList.add("column_int3_col__40_20");
                    colList.add("column_int3_col__40_21");
                    colList.add("column_int3_col__40_22");
                    colList.add("column_int3_col__40_23");
                    colList.add("column_int3_col__40_24");
                    colList.add("column_int3_col__40_25");
                    colList.add("column_int3_col__40_26");
                    colList.add("column_int3_col__40_27");
                    colList.add("column_int3_col__40_28");
                    colList.add("column_int3_col__40_29");
                    colList.add("column_int3_col__40_30");
                    colList.add("column_int3_col__40_31");
                    colList.add("column_int3_col__40_32");
                    colList.add("column_int3_col__40_33");
                    colList.add("column_int3_col__40_34");
                    colList.add("column_int3_col__40_35");
                    colList.add("column_int3_col__40_36");
                    colList.add("column_int3_col__40_37");
                    colList.add("column_int3_col__40_38");
                    colList.add("column_int3_col__40_39");
                    colList.add("column_int3_col__40_40");
                    colList.add("column_int4_col__40_1");
                    colList.add("column_int4_col__40_2");
                    colList.add("column_int4_col__40_3");
                    colList.add("column_int4_col__40_4");
                    colList.add("column_int4_col__40_5");
                    colList.add("column_int4_col__40_6");
                    colList.add("column_int4_col__40_7");
                    colList.add("column_int4_col__40_8");
                    colList.add("column_int4_col__40_9");
                    colList.add("column_int4_col__40_10");
                    colList.add("column_int4_col__40_11");
                    colList.add("column_int4_col__40_12");
                    colList.add("column_int4_col__40_13");
                    colList.add("column_int4_col__40_14");
                    colList.add("column_int4_col__40_15");
                    colList.add("column_int4_col__40_16");
                    colList.add("column_int4_col__40_17");
                    colList.add("column_int4_col__40_18");
                    colList.add("column_int4_col__40_19");
                    colList.add("column_int4_col__40_20");
                    colList.add("column_int4_col__40_21");
                    colList.add("column_int4_col__40_22");
                    colList.add("column_int4_col__40_23");
                    colList.add("column_int4_col__40_24");
                    colList.add("column_int4_col__40_25");
                    colList.add("column_int4_col__40_26");
                    colList.add("column_int4_col__40_27");
                    colList.add("column_int4_col__40_28");
                    colList.add("column_int4_col__40_29");
                    colList.add("column_int4_col__40_30");
                    colList.add("column_int4_col__40_31");
                    colList.add("column_int4_col__40_32");
                    colList.add("column_int4_col__40_33");
                    colList.add("column_int4_col__40_34");
                    colList.add("column_int4_col__40_35");
                    colList.add("column_int4_col__40_36");
                    colList.add("column_int4_col__40_37");
                    colList.add("column_int4_col__40_38");
                    colList.add("column_int4_col__40_39");
                    colList.add("column_int4_col__40_40");
                    colList.add("column_long1_col__80_1");
                    colList.add("column_long1_col__80_2");
                    colList.add("column_long1_col__80_3");
                    colList.add("column_long1_col__80_4");
                    colList.add("column_long1_col__80_5");
                    colList.add("column_long1_col__80_6");
                    colList.add("column_long1_col__80_7");
                    colList.add("column_long1_col__80_8");
                    colList.add("column_long1_col__80_9");
                    colList.add("column_long1_col__80_10");
                    colList.add("column_long1_col__80_11");
                    colList.add("column_long1_col__80_12");
                    colList.add("column_long1_col__80_13");
                    colList.add("column_long1_col__80_14");
                    colList.add("column_long1_col__80_15");
                    colList.add("column_long1_col__80_16");
                    colList.add("column_long1_col__80_17");
                    colList.add("column_long1_col__80_18");
                    colList.add("column_long1_col__80_19");
                    colList.add("column_long1_col__80_20");
                    colList.add("column_long1_col__80_21");
                    colList.add("column_long1_col__80_22");
                    colList.add("column_long1_col__80_23");
                    colList.add("column_long1_col__80_24");
                    colList.add("column_long1_col__80_25");
                    colList.add("column_long1_col__80_26");
                    colList.add("column_long1_col__80_27");
                    colList.add("column_long1_col__80_28");
                    colList.add("column_long1_col__80_29");
                    colList.add("column_long1_col__80_30");
                    colList.add("column_long1_col__80_31");
                    colList.add("column_long1_col__80_32");
                    colList.add("column_long1_col__80_33");
                    colList.add("column_long1_col__80_34");
                    colList.add("column_long1_col__80_35");
                    colList.add("column_long1_col__80_36");
                    colList.add("column_long1_col__80_37");
                    colList.add("column_long1_col__80_38");
                    colList.add("column_long1_col__80_39");
                    colList.add("column_long1_col__80_40");
                    colList.add("column_long1_col__80_41");
                    colList.add("column_long1_col__80_42");
                    colList.add("column_long1_col__80_43");
                    colList.add("column_long1_col__80_44");
                    colList.add("column_long1_col__80_45");
                    colList.add("column_long1_col__80_46");
                    colList.add("column_long1_col__80_47");
                    colList.add("column_long1_col__80_48");
                    colList.add("column_long1_col__80_49");
                    colList.add("column_long1_col__80_50");
                    colList.add("column_long1_col__80_51");
                    colList.add("column_long1_col__80_52");
                    colList.add("column_long1_col__80_53");
                    colList.add("column_long1_col__80_54");
                    colList.add("column_long1_col__80_55");
                    colList.add("column_long1_col__80_56");
                    colList.add("column_long1_col__80_57");
                    colList.add("column_long1_col__80_58");
                    colList.add("column_long1_col__80_59");
                    colList.add("column_long1_col__80_60");
                    colList.add("column_long1_col__80_61");
                    colList.add("column_long1_col__80_62");
                    colList.add("column_long1_col__80_63");
                    colList.add("column_long1_col__80_64");
                    colList.add("column_long1_col__80_65");
                    colList.add("column_long1_col__80_66");
                    colList.add("column_long1_col__80_67");
                    colList.add("column_long1_col__80_68");
                    colList.add("column_long1_col__80_69");
                    colList.add("column_long1_col__80_70");
                    colList.add("column_long1_col__80_71");
                    colList.add("column_long1_col__80_72");
                    colList.add("column_long1_col__80_73");
                    colList.add("column_long1_col__80_74");
                    colList.add("column_long1_col__80_75");
                    colList.add("column_long1_col__80_76");
                    colList.add("column_long1_col__80_77");
                    colList.add("column_long1_col__80_78");
                    colList.add("column_long1_col__80_79");
                    colList.add("column_long1_col__80_80");
                    colList.add("column_long2_col__80_1");
                    colList.add("column_long2_col__80_2");
                    colList.add("column_long2_col__80_3");
                    colList.add("column_long2_col__80_4");
                    colList.add("column_long2_col__80_5");
                    colList.add("column_long2_col__80_6");
                    colList.add("column_long2_col__80_7");
                    colList.add("column_long2_col__80_8");
                    colList.add("column_long2_col__80_9");
                    colList.add("column_long2_col__80_10");
                    colList.add("column_long2_col__80_11");
                    colList.add("column_long2_col__80_12");
                    colList.add("column_long2_col__80_13");
                    colList.add("column_long2_col__80_14");
                    colList.add("column_long2_col__80_15");
                    colList.add("column_long2_col__80_16");
                    colList.add("column_long2_col__80_17");
                    colList.add("column_long2_col__80_18");
                    colList.add("column_long2_col__80_19");
                    colList.add("column_long2_col__80_20");
                    colList.add("column_long2_col__80_21");
                    colList.add("column_long2_col__80_22");
                    colList.add("column_long2_col__80_23");
                    colList.add("column_long2_col__80_24");
                    colList.add("column_long2_col__80_25");
                    colList.add("column_long2_col__80_26");
                    colList.add("column_long2_col__80_27");
                    colList.add("column_long2_col__80_28");
                    colList.add("column_long2_col__80_29");
                    colList.add("column_long2_col__80_30");
                    colList.add("column_long2_col__80_31");
                    colList.add("column_long2_col__80_32");
                    colList.add("column_long2_col__80_33");
                    colList.add("column_long2_col__80_34");
                    colList.add("column_long2_col__80_35");
                    colList.add("column_long2_col__80_36");
                    colList.add("column_long2_col__80_37");
                    colList.add("column_long2_col__80_38");
                    colList.add("column_long2_col__80_39");
                    colList.add("column_long2_col__80_40");
                    colList.add("column_long2_col__80_41");
                    colList.add("column_long2_col__80_42");
                    colList.add("column_long2_col__80_43");
                    colList.add("column_long2_col__80_44");
                    colList.add("column_long2_col__80_45");
                    colList.add("column_long2_col__80_46");
                    colList.add("column_long2_col__80_47");
                    colList.add("column_long2_col__80_48");
                    colList.add("column_long2_col__80_49");
                    colList.add("column_long2_col__80_50");
                    colList.add("column_long2_col__80_51");
                    colList.add("column_long2_col__80_52");
                    colList.add("column_long2_col__80_53");
                    colList.add("column_long2_col__80_54");
                    colList.add("column_long2_col__80_55");
                    colList.add("column_long2_col__80_56");
                    colList.add("column_long2_col__80_57");
                    colList.add("column_long2_col__80_58");
                    colList.add("column_long2_col__80_59");
                    colList.add("column_long2_col__80_60");
                    colList.add("column_long2_col__80_61");
                    colList.add("column_long2_col__80_62");
                    colList.add("column_long2_col__80_63");
                    colList.add("column_long2_col__80_64");
                    colList.add("column_long2_col__80_65");
                    colList.add("column_long2_col__80_66");
                    colList.add("column_long2_col__80_67");
                    colList.add("column_long2_col__80_68");
                    colList.add("column_long2_col__80_69");
                    colList.add("column_long2_col__80_70");
                    colList.add("column_long2_col__80_71");
                    colList.add("column_long2_col__80_72");
                    colList.add("column_long2_col__80_73");
                    colList.add("column_long2_col__80_74");
                    colList.add("column_long2_col__80_75");
                    colList.add("column_long2_col__80_76");
                    colList.add("column_long2_col__80_77");
                    colList.add("column_long2_col__80_78");
                    colList.add("column_long2_col__80_79");
                    colList.add("column_long2_col__80_80");
                    colList.add("column_long3_col__80_1");
                    colList.add("column_long3_col__80_2");
                    colList.add("column_long3_col__80_3");
                    colList.add("column_long3_col__80_4");
                    colList.add("column_long3_col__80_5");
                    colList.add("column_long3_col__80_6");
                    colList.add("column_long3_col__80_7");
                    colList.add("column_long3_col__80_8");
                    colList.add("column_long3_col__80_9");
                    colList.add("column_long3_col__80_10");
                    colList.add("column_long3_col__80_11");
                    colList.add("column_long3_col__80_12");
                    colList.add("column_long3_col__80_13");
                    colList.add("column_long3_col__80_14");
                    colList.add("column_long3_col__80_15");
                    colList.add("column_long3_col__80_16");
                    colList.add("column_long3_col__80_17");
                    colList.add("column_long3_col__80_18");
                    colList.add("column_long3_col__80_19");
                    colList.add("column_long3_col__80_20");
                    colList.add("column_long3_col__80_21");
                    colList.add("column_long3_col__80_22");
                    colList.add("column_long3_col__80_23");
                    colList.add("column_long3_col__80_24");
                    colList.add("column_long3_col__80_25");
                    colList.add("column_long3_col__80_26");
                    colList.add("column_long3_col__80_27");
                    colList.add("column_long3_col__80_28");
                    colList.add("column_long3_col__80_29");
                    colList.add("column_long3_col__80_30");
                    colList.add("column_long3_col__80_31");
                    colList.add("column_long3_col__80_32");
                    colList.add("column_long3_col__80_33");
                    colList.add("column_long3_col__80_34");
                    colList.add("column_long3_col__80_35");
                    colList.add("column_long3_col__80_36");
                    colList.add("column_long3_col__80_37");
                    colList.add("column_long3_col__80_38");
                    colList.add("column_long3_col__80_39");
                    colList.add("column_long3_col__80_40");
                    colList.add("column_long3_col__80_41");
                    colList.add("column_long3_col__80_42");
                    colList.add("column_long3_col__80_43");
                    colList.add("column_long3_col__80_44");
                    colList.add("column_long3_col__80_45");
                    colList.add("column_long3_col__80_46");
                    colList.add("column_long3_col__80_47");
                    colList.add("column_long3_col__80_48");
                    colList.add("column_long3_col__80_49");
                    colList.add("column_long3_col__80_50");
                    colList.add("column_long3_col__80_51");
                    colList.add("column_long3_col__80_52");
                    colList.add("column_long3_col__80_53");
                    colList.add("column_long3_col__80_54");
                    colList.add("column_long3_col__80_55");
                    colList.add("column_long3_col__80_56");
                    colList.add("column_long3_col__80_57");
                    colList.add("column_long3_col__80_58");
                    colList.add("column_long3_col__80_59");
                    colList.add("column_long3_col__80_60");
                    colList.add("column_long3_col__80_61");
                    colList.add("column_long3_col__80_62");
                    colList.add("column_long3_col__80_63");
                    colList.add("column_long3_col__80_64");
                    colList.add("column_long3_col__80_65");
                    colList.add("column_long3_col__80_66");
                    colList.add("column_long3_col__80_67");
                    colList.add("column_long3_col__80_68");
                    colList.add("column_long3_col__80_69");
                    colList.add("column_long3_col__80_70");
                    colList.add("column_long3_col__80_71");
                    colList.add("column_long3_col__80_72");
                    colList.add("column_long3_col__80_73");
                    colList.add("column_long3_col__80_74");
                    colList.add("column_long3_col__80_75");
                    colList.add("column_long3_col__80_76");
                    colList.add("column_long3_col__80_77");
                    colList.add("column_long3_col__80_78");
                    colList.add("column_long3_col__80_79");
                    colList.add("column_long3_col__80_80");
                    colList.add("column_long4_col__80_1");
                    colList.add("column_long4_col__80_2");
                    colList.add("column_long4_col__80_3");
                    colList.add("column_long4_col__80_4");
                    colList.add("column_long4_col__80_5");
                    colList.add("column_long4_col__80_6");
                    colList.add("column_long4_col__80_7");
                    colList.add("column_long4_col__80_8");
                    colList.add("column_long4_col__80_9");
                    colList.add("column_long4_col__80_10");
                    colList.add("column_long4_col__80_11");
                    colList.add("column_long4_col__80_12");
                    colList.add("column_long4_col__80_13");
                    colList.add("column_long4_col__80_14");
                    colList.add("column_long4_col__80_15");
                    colList.add("column_long4_col__80_16");
                    colList.add("column_long4_col__80_17");
                    colList.add("column_long4_col__80_18");
                    colList.add("column_long4_col__80_19");
                    colList.add("column_long4_col__80_20");
                    colList.add("column_long4_col__80_21");
                    colList.add("column_long4_col__80_22");
                    colList.add("column_long4_col__80_23");
                    colList.add("column_long4_col__80_24");
                    colList.add("column_long4_col__80_25");
                    colList.add("column_long4_col__80_26");
                    colList.add("column_long4_col__80_27");
                    colList.add("column_long4_col__80_28");
                    colList.add("column_long4_col__80_29");
                    colList.add("column_long4_col__80_30");
                    colList.add("column_long4_col__80_31");
                    colList.add("column_long4_col__80_32");
                    colList.add("column_long4_col__80_33");
                    colList.add("column_long4_col__80_34");
                    colList.add("column_long4_col__80_35");
                    colList.add("column_long4_col__80_36");
                    colList.add("column_long4_col__80_37");
                    colList.add("column_long4_col__80_38");
                    colList.add("column_long4_col__80_39");
                    colList.add("column_long4_col__80_40");
                    colList.add("column_long4_col__80_41");
                    colList.add("column_long4_col__80_42");
                    colList.add("column_long4_col__80_43");
                    colList.add("column_long4_col__80_44");
                    colList.add("column_long4_col__80_45");
                    colList.add("column_long4_col__80_46");
                    colList.add("column_long4_col__80_47");
                    colList.add("column_long4_col__80_48");
                    colList.add("column_long4_col__80_49");
                    colList.add("column_long4_col__80_50");
                    colList.add("column_long4_col__80_51");
                    colList.add("column_long4_col__80_52");
                    colList.add("column_long4_col__80_53");
                    colList.add("column_long4_col__80_54");
                    colList.add("column_long4_col__80_55");
                    colList.add("column_long4_col__80_56");
                    colList.add("column_long4_col__80_57");
                    colList.add("column_long4_col__80_58");
                    colList.add("column_long4_col__80_59");
                    colList.add("column_long4_col__80_60");
                    colList.add("column_long4_col__80_61");
                    colList.add("column_long4_col__80_62");
                    colList.add("column_long4_col__80_63");
                    colList.add("column_long4_col__80_64");
                    colList.add("column_long4_col__80_65");
                    colList.add("column_long4_col__80_66");
                    colList.add("column_long4_col__80_67");
                    colList.add("column_long4_col__80_68");
                    colList.add("column_long4_col__80_69");
                    colList.add("column_long4_col__80_70");
                    colList.add("column_long4_col__80_71");
                    colList.add("column_long4_col__80_72");
                    colList.add("column_long4_col__80_73");
                    colList.add("column_long4_col__80_74");
                    colList.add("column_long4_col__80_75");
                    colList.add("column_long4_col__80_76");
                    colList.add("column_long4_col__80_77");
                    colList.add("column_long4_col__80_78");
                    colList.add("column_long4_col__80_79");
                    colList.add("column_long4_col__80_80");
                    colList.add("column_string9_col__4_1");
                    colList.add("column_string9_col__4_2");
                    colList.add("column_string9_col__4_3");
                    colList.add("column_string9_col__4_4");
                    colList.add("column_string10_col__1_1");
                    colList.add("column_string11_col__1_1");
                    colList.add("column_string12_col__2_1");
                    colList.add("column_string12_col__2_2");
                    colList.add("column_string13_col__4_1");
                    colList.add("column_string13_col__4_2");
                    colList.add("column_string13_col__4_3");
                    colList.add("column_string13_col__4_4");
                    colList.add("column_string14_col__1_1");
                    colList.add("column_string15_col__1_1");
                    colList.add("column_string16_col__2_1");
                    colList.add("column_string16_col__2_2");
                    colList.add("column_int5_col__40_1");
                    colList.add("column_int5_col__40_2");
                    colList.add("column_int5_col__40_3");
                    colList.add("column_int5_col__40_4");
                    colList.add("column_int5_col__40_5");
                    colList.add("column_int5_col__40_6");
                    colList.add("column_int5_col__40_7");
                    colList.add("column_int5_col__40_8");
                    colList.add("column_int5_col__40_9");
                    colList.add("column_int5_col__40_10");
                    colList.add("column_int5_col__40_11");
                    colList.add("column_int5_col__40_12");
                    colList.add("column_int5_col__40_13");
                    colList.add("column_int5_col__40_14");
                    colList.add("column_int5_col__40_15");
                    colList.add("column_int5_col__40_16");
                    colList.add("column_int5_col__40_17");
                    colList.add("column_int5_col__40_18");
                    colList.add("column_int5_col__40_19");
                    colList.add("column_int5_col__40_20");
                    colList.add("column_int5_col__40_21");
                    colList.add("column_int5_col__40_22");
                    colList.add("column_int5_col__40_23");
                    colList.add("column_int5_col__40_24");
                    colList.add("column_int5_col__40_25");
                    colList.add("column_int5_col__40_26");
                    colList.add("column_int5_col__40_27");
                    colList.add("column_int5_col__40_28");
                    colList.add("column_int5_col__40_29");
                    colList.add("column_int5_col__40_30");
                    colList.add("column_int5_col__40_31");
                    colList.add("column_int5_col__40_32");
                    colList.add("column_int5_col__40_33");
                    colList.add("column_int5_col__40_34");
                    colList.add("column_int5_col__40_35");
                    colList.add("column_int5_col__40_36");
                    colList.add("column_int5_col__40_37");
                    colList.add("column_int5_col__40_38");
                    colList.add("column_int5_col__40_39");
                    colList.add("column_int5_col__40_40");
                    colList.add("column_int6_col__40_1");
                    colList.add("column_int6_col__40_2");
                    colList.add("column_int6_col__40_3");
                    colList.add("column_int6_col__40_4");
                    colList.add("column_int6_col__40_5");
                    colList.add("column_int6_col__40_6");
                    colList.add("column_int6_col__40_7");
                    colList.add("column_int6_col__40_8");
                    colList.add("column_int6_col__40_9");
                    colList.add("column_int6_col__40_10");
                    colList.add("column_int6_col__40_11");
                    colList.add("column_int6_col__40_12");
                    colList.add("column_int6_col__40_13");
                    colList.add("column_int6_col__40_14");
                    colList.add("column_int6_col__40_15");
                    colList.add("column_int6_col__40_16");
                    colList.add("column_int6_col__40_17");
                    colList.add("column_int6_col__40_18");
                    colList.add("column_int6_col__40_19");
                    colList.add("column_int6_col__40_20");
                    colList.add("column_int6_col__40_21");
                    colList.add("column_int6_col__40_22");
                    colList.add("column_int6_col__40_23");
                    colList.add("column_int6_col__40_24");
                    colList.add("column_int6_col__40_25");
                    colList.add("column_int6_col__40_26");
                    colList.add("column_int6_col__40_27");
                    colList.add("column_int6_col__40_28");
                    colList.add("column_int6_col__40_29");
                    colList.add("column_int6_col__40_30");
                    colList.add("column_int6_col__40_31");
                    colList.add("column_int6_col__40_32");
                    colList.add("column_int6_col__40_33");
                    colList.add("column_int6_col__40_34");
                    colList.add("column_int6_col__40_35");
                    colList.add("column_int6_col__40_36");
                    colList.add("column_int6_col__40_37");
                    colList.add("column_int6_col__40_38");
                    colList.add("column_int6_col__40_39");
                    colList.add("column_int6_col__40_40");
                    colList.add("column_int7_col__40_1");
                    colList.add("column_int7_col__40_2");
                    colList.add("column_int7_col__40_3");
                    colList.add("column_int7_col__40_4");
                    colList.add("column_int7_col__40_5");
                    colList.add("column_int7_col__40_6");
                    colList.add("column_int7_col__40_7");
                    colList.add("column_int7_col__40_8");
                    colList.add("column_int7_col__40_9");
                    colList.add("column_int7_col__40_10");
                    colList.add("column_int7_col__40_11");
                    colList.add("column_int7_col__40_12");
                    colList.add("column_int7_col__40_13");
                    colList.add("column_int7_col__40_14");
                    colList.add("column_int7_col__40_15");
                    colList.add("column_int7_col__40_16");
                    colList.add("column_int7_col__40_17");
                    colList.add("column_int7_col__40_18");
                    colList.add("column_int7_col__40_19");
                    colList.add("column_int7_col__40_20");
                    colList.add("column_int7_col__40_21");
                    colList.add("column_int7_col__40_22");
                    colList.add("column_int7_col__40_23");
                    colList.add("column_int7_col__40_24");
                    colList.add("column_int7_col__40_25");
                    colList.add("column_int7_col__40_26");
                    colList.add("column_int7_col__40_27");
                    colList.add("column_int7_col__40_28");
                    colList.add("column_int7_col__40_29");
                    colList.add("column_int7_col__40_30");
                    colList.add("column_int7_col__40_31");
                    colList.add("column_int7_col__40_32");
                    colList.add("column_int7_col__40_33");
                    colList.add("column_int7_col__40_34");
                    colList.add("column_int7_col__40_35");
                    colList.add("column_int7_col__40_36");
                    colList.add("column_int7_col__40_37");
                    colList.add("column_int7_col__40_38");
                    colList.add("column_int7_col__40_39");
                    colList.add("column_int7_col__40_40");
                    colList.add("column_int8_col__40_1");
                    colList.add("column_int8_col__40_2");
                    colList.add("column_int8_col__40_3");
                    colList.add("column_int8_col__40_4");
                    colList.add("column_int8_col__40_5");
                    colList.add("column_int8_col__40_6");
                    colList.add("column_int8_col__40_7");
                    colList.add("column_int8_col__40_8");
                    colList.add("column_int8_col__40_9");
                    colList.add("column_int8_col__40_10");
                    colList.add("column_int8_col__40_11");
                    colList.add("column_int8_col__40_12");
                    colList.add("column_int8_col__40_13");
                    colList.add("column_int8_col__40_14");
                    colList.add("column_int8_col__40_15");
                    colList.add("column_int8_col__40_16");
                    colList.add("column_int8_col__40_17");
                    colList.add("column_int8_col__40_18");
                    colList.add("column_int8_col__40_19");
                    colList.add("column_int8_col__40_20");
                    colList.add("column_int8_col__40_21");
                    colList.add("column_int8_col__40_22");
                    colList.add("column_int8_col__40_23");
                    colList.add("column_int8_col__40_24");
                    colList.add("column_int8_col__40_25");
                    colList.add("column_int8_col__40_26");
                    colList.add("column_int8_col__40_27");
                    colList.add("column_int8_col__40_28");
                    colList.add("column_int8_col__40_29");
                    colList.add("column_int8_col__40_30");
                    colList.add("column_int8_col__40_31");
                    colList.add("column_int8_col__40_32");
                    colList.add("column_int8_col__40_33");
                    colList.add("column_int8_col__40_34");
                    colList.add("column_int8_col__40_35");
                    colList.add("column_int8_col__40_36");
                    colList.add("column_int8_col__40_37");
                    colList.add("column_int8_col__40_38");
                    colList.add("column_int8_col__40_39");
                    colList.add("column_int8_col__40_40");
                    colList.add("column_long5_col__80_1");
                    colList.add("column_long5_col__80_2");
                    colList.add("column_long5_col__80_3");
                    colList.add("column_long5_col__80_4");
                    colList.add("column_long5_col__80_5");
                    colList.add("column_long5_col__80_6");
                    colList.add("column_long5_col__80_7");
                    colList.add("column_long5_col__80_8");
                    colList.add("column_long5_col__80_9");
                    colList.add("column_long5_col__80_10");
                    colList.add("column_long5_col__80_11");
                    colList.add("column_long5_col__80_12");
                    colList.add("column_long5_col__80_13");
                    colList.add("column_long5_col__80_14");
                    colList.add("column_long5_col__80_15");
                    colList.add("column_long5_col__80_16");
                    colList.add("column_long5_col__80_17");
                    colList.add("column_long5_col__80_18");
                    colList.add("column_long5_col__80_19");
                    colList.add("column_long5_col__80_20");
                    colList.add("column_long5_col__80_21");
                    colList.add("column_long5_col__80_22");
                    colList.add("column_long5_col__80_23");
                    colList.add("column_long5_col__80_24");
                    colList.add("column_long5_col__80_25");
                    colList.add("column_long5_col__80_26");
                    colList.add("column_long5_col__80_27");
                    colList.add("column_long5_col__80_28");
                    colList.add("column_long5_col__80_29");
                    colList.add("column_long5_col__80_30");
                    colList.add("column_long5_col__80_31");
                    colList.add("column_long5_col__80_32");
                    colList.add("column_long5_col__80_33");
                    colList.add("column_long5_col__80_34");
                    colList.add("column_long5_col__80_35");
                    colList.add("column_long5_col__80_36");
                    colList.add("column_long5_col__80_37");
                    colList.add("column_long5_col__80_38");
                    colList.add("column_long5_col__80_39");
                    colList.add("column_long5_col__80_40");
                    colList.add("column_long5_col__80_41");
                    colList.add("column_long5_col__80_42");
                    colList.add("column_long5_col__80_43");
                    colList.add("column_long5_col__80_44");
                    colList.add("column_long5_col__80_45");
                    colList.add("column_long5_col__80_46");
                    colList.add("column_long5_col__80_47");
                    colList.add("column_long5_col__80_48");
                    colList.add("column_long5_col__80_49");
                    colList.add("column_long5_col__80_50");
                    colList.add("column_long5_col__80_51");
                    colList.add("column_long5_col__80_52");
                    colList.add("column_long5_col__80_53");
                    colList.add("column_long5_col__80_54");
                    colList.add("column_long5_col__80_55");
                    colList.add("column_long5_col__80_56");
                    colList.add("column_long5_col__80_57");
                    colList.add("column_long5_col__80_58");
                    colList.add("column_long5_col__80_59");
                    colList.add("column_long5_col__80_60");
                    colList.add("column_long5_col__80_61");
                    colList.add("column_long5_col__80_62");
                    colList.add("column_long5_col__80_63");
                    colList.add("column_long5_col__80_64");
                    colList.add("column_long5_col__80_65");
                    colList.add("column_long5_col__80_66");
                    colList.add("column_long5_col__80_67");
                    colList.add("column_long5_col__80_68");
                    colList.add("column_long5_col__80_69");
                    colList.add("column_long5_col__80_70");
                    colList.add("column_long5_col__80_71");
                    colList.add("column_long5_col__80_72");
                    colList.add("column_long5_col__80_73");
                    colList.add("column_long5_col__80_74");
                    colList.add("column_long5_col__80_75");
                    colList.add("column_long5_col__80_76");
                    colList.add("column_long5_col__80_77");
                    colList.add("column_long5_col__80_78");
                    colList.add("column_long5_col__80_79");
                    colList.add("column_long5_col__80_80");
                    colList.add("column_long6_col__80_1");
                    colList.add("column_long6_col__80_2");
                    colList.add("column_long6_col__80_3");
                    colList.add("column_long6_col__80_4");
                    colList.add("column_long6_col__80_5");
                    colList.add("column_long6_col__80_6");
                    colList.add("column_long6_col__80_7");
                    colList.add("column_long6_col__80_8");
                    colList.add("column_long6_col__80_9");
                    colList.add("column_long6_col__80_10");
                    colList.add("column_long6_col__80_11");
                    colList.add("column_long6_col__80_12");
                    colList.add("column_long6_col__80_13");
                    colList.add("column_long6_col__80_14");
                    colList.add("column_long6_col__80_15");
                    colList.add("column_long6_col__80_16");
                    colList.add("column_long6_col__80_17");
                    colList.add("column_long6_col__80_18");
                    colList.add("column_long6_col__80_19");
                    colList.add("column_long6_col__80_20");
                    colList.add("column_long6_col__80_21");
                    colList.add("column_long6_col__80_22");
                    colList.add("column_long6_col__80_23");
                    colList.add("column_long6_col__80_24");
                    colList.add("column_long6_col__80_25");
                    colList.add("column_long6_col__80_26");
                    colList.add("column_long6_col__80_27");
                    colList.add("column_long6_col__80_28");
                    colList.add("column_long6_col__80_29");
                    colList.add("column_long6_col__80_30");
                    colList.add("column_long6_col__80_31");
                    colList.add("column_long6_col__80_32");
                    colList.add("column_long6_col__80_33");
                    colList.add("column_long6_col__80_34");
                    colList.add("column_long6_col__80_35");
                    colList.add("column_long6_col__80_36");
                    colList.add("column_long6_col__80_37");
                    colList.add("column_long6_col__80_38");
                    colList.add("column_long6_col__80_39");
                    colList.add("column_long6_col__80_40");
                    colList.add("column_long6_col__80_41");
                    colList.add("column_long6_col__80_42");
                    colList.add("column_long6_col__80_43");
                    colList.add("column_long6_col__80_44");
                    colList.add("column_long6_col__80_45");
                    colList.add("column_long6_col__80_46");
                    colList.add("column_long6_col__80_47");
                    colList.add("column_long6_col__80_48");
                    colList.add("column_long6_col__80_49");
                    colList.add("column_long6_col__80_50");
                    colList.add("column_long6_col__80_51");
                    colList.add("column_long6_col__80_52");
                    colList.add("column_long6_col__80_53");
                    colList.add("column_long6_col__80_54");
                    colList.add("column_long6_col__80_55");
                    colList.add("column_long6_col__80_56");
                    colList.add("column_long6_col__80_57");
                    colList.add("column_long6_col__80_58");
                    colList.add("column_long6_col__80_59");
                    colList.add("column_long6_col__80_60");
                    colList.add("column_long6_col__80_61");
                    colList.add("column_long6_col__80_62");
                    colList.add("column_long6_col__80_63");
                    colList.add("column_long6_col__80_64");
                    colList.add("column_long6_col__80_65");
                    colList.add("column_long6_col__80_66");
                    colList.add("column_long6_col__80_67");
                    colList.add("column_long6_col__80_68");
                    colList.add("column_long6_col__80_69");
                    colList.add("column_long6_col__80_70");
                    colList.add("column_long6_col__80_71");
                    colList.add("column_long6_col__80_72");
                    colList.add("column_long6_col__80_73");
                    colList.add("column_long6_col__80_74");
                    colList.add("column_long6_col__80_75");
                    colList.add("column_long6_col__80_76");
                    colList.add("column_long6_col__80_77");
                    colList.add("column_long6_col__80_78");
                    colList.add("column_long6_col__80_79");
                    colList.add("column_long6_col__80_80");
                    colList.add("column_long7_col__80_1");
                    colList.add("column_long7_col__80_2");
                    colList.add("column_long7_col__80_3");
                    colList.add("column_long7_col__80_4");
                    colList.add("column_long7_col__80_5");
                    colList.add("column_long7_col__80_6");
                    colList.add("column_long7_col__80_7");
                    colList.add("column_long7_col__80_8");
                    colList.add("column_long7_col__80_9");
                    colList.add("column_long7_col__80_10");
                    colList.add("column_long7_col__80_11");
                    colList.add("column_long7_col__80_12");
                    colList.add("column_long7_col__80_13");
                    colList.add("column_long7_col__80_14");
                    colList.add("column_long7_col__80_15");
                    colList.add("column_long7_col__80_16");
                    colList.add("column_long7_col__80_17");
                    colList.add("column_long7_col__80_18");
                    colList.add("column_long7_col__80_19");
                    colList.add("column_long7_col__80_20");
                    colList.add("column_long7_col__80_21");
                    colList.add("column_long7_col__80_22");
                    colList.add("column_long7_col__80_23");
                    colList.add("column_long7_col__80_24");
                    colList.add("column_long7_col__80_25");
                    colList.add("column_long7_col__80_26");
                    colList.add("column_long7_col__80_27");
                    colList.add("column_long7_col__80_28");
                    colList.add("column_long7_col__80_29");
                    colList.add("column_long7_col__80_30");
                    colList.add("column_long7_col__80_31");
                    colList.add("column_long7_col__80_32");
                    colList.add("column_long7_col__80_33");
                    colList.add("column_long7_col__80_34");
                    colList.add("column_long7_col__80_35");
                    colList.add("column_long7_col__80_36");
                    colList.add("column_long7_col__80_37");
                    colList.add("column_long7_col__80_38");
                    colList.add("column_long7_col__80_39");
                    colList.add("column_long7_col__80_40");
                    colList.add("column_long7_col__80_41");
                    colList.add("column_long7_col__80_42");
                    colList.add("column_long7_col__80_43");
                    colList.add("column_long7_col__80_44");
                    colList.add("column_long7_col__80_45");
                    colList.add("column_long7_col__80_46");
                    colList.add("column_long7_col__80_47");
                    colList.add("column_long7_col__80_48");
                    colList.add("column_long7_col__80_49");
                    colList.add("column_long7_col__80_50");
                    colList.add("column_long7_col__80_51");
                    colList.add("column_long7_col__80_52");
                    colList.add("column_long7_col__80_53");
                    colList.add("column_long7_col__80_54");
                    colList.add("column_long7_col__80_55");
                    colList.add("column_long7_col__80_56");
                    colList.add("column_long7_col__80_57");
                    colList.add("column_long7_col__80_58");
                    colList.add("column_long7_col__80_59");
                    colList.add("column_long7_col__80_60");
                    colList.add("column_long7_col__80_61");
                    colList.add("column_long7_col__80_62");
                    colList.add("column_long7_col__80_63");
                    colList.add("column_long7_col__80_64");
                    colList.add("column_long7_col__80_65");
                    colList.add("column_long7_col__80_66");
                    colList.add("column_long7_col__80_67");
                    colList.add("column_long7_col__80_68");
                    colList.add("column_long7_col__80_69");
                    colList.add("column_long7_col__80_70");
                    colList.add("column_long7_col__80_71");
                    colList.add("column_long7_col__80_72");
                    colList.add("column_long7_col__80_73");
                    colList.add("column_long7_col__80_74");
                    colList.add("column_long7_col__80_75");
                    colList.add("column_long7_col__80_76");
                    colList.add("column_long7_col__80_77");
                    colList.add("column_long7_col__80_78");
                    colList.add("column_long7_col__80_79");
                    colList.add("column_long7_col__80_80");
                    colList.add("column_long8_col__80_1");
                    colList.add("column_long8_col__80_2");
                    colList.add("column_long8_col__80_3");
                    colList.add("column_long8_col__80_4");
                    colList.add("column_long8_col__80_5");
                    colList.add("column_long8_col__80_6");
                    colList.add("column_long8_col__80_7");
                    colList.add("column_long8_col__80_8");
                    colList.add("column_long8_col__80_9");
                    colList.add("column_long8_col__80_10");
                    colList.add("column_long8_col__80_11");
                    colList.add("column_long8_col__80_12");
                    colList.add("column_long8_col__80_13");
                    colList.add("column_long8_col__80_14");
                    colList.add("column_long8_col__80_15");
                    colList.add("column_long8_col__80_16");
                    colList.add("column_long8_col__80_17");
                    colList.add("column_long8_col__80_18");
                    colList.add("column_long8_col__80_19");
                    colList.add("column_long8_col__80_20");
                    colList.add("column_long8_col__80_21");
                    colList.add("column_long8_col__80_22");
                    colList.add("column_long8_col__80_23");
                    colList.add("column_long8_col__80_24");
                    colList.add("column_long8_col__80_25");
                    colList.add("column_long8_col__80_26");
                    colList.add("column_long8_col__80_27");
                    colList.add("column_long8_col__80_28");
                    colList.add("column_long8_col__80_29");
                    colList.add("column_long8_col__80_30");
                    colList.add("column_long8_col__80_31");
                    colList.add("column_long8_col__80_32");
                    colList.add("column_long8_col__80_33");
                    colList.add("column_long8_col__80_34");
                    colList.add("column_long8_col__80_35");
                    colList.add("column_long8_col__80_36");
                    colList.add("column_long8_col__80_37");
                    colList.add("column_long8_col__80_38");
                    colList.add("column_long8_col__80_39");
                    colList.add("column_long8_col__80_40");
                    colList.add("column_long8_col__80_41");
                    colList.add("column_long8_col__80_42");
                    colList.add("column_long8_col__80_43");
                    colList.add("column_long8_col__80_44");
                    colList.add("column_long8_col__80_45");
                    colList.add("column_long8_col__80_46");
                    colList.add("column_long8_col__80_47");
                    colList.add("column_long8_col__80_48");
                    colList.add("column_long8_col__80_49");
                    colList.add("column_long8_col__80_50");
                    colList.add("column_long8_col__80_51");
                    colList.add("column_long8_col__80_52");
                    colList.add("column_long8_col__80_53");
                    colList.add("column_long8_col__80_54");
                    colList.add("column_long8_col__80_55");
                    colList.add("column_long8_col__80_56");
                    colList.add("column_long8_col__80_57");
                    colList.add("column_long8_col__80_58");
                    colList.add("column_long8_col__80_59");
                    colList.add("column_long8_col__80_60");
                    colList.add("column_long8_col__80_61");
                    colList.add("column_long8_col__80_62");
                    colList.add("column_long8_col__80_63");
                    colList.add("column_long8_col__80_64");
                    colList.add("column_long8_col__80_65");
                    colList.add("column_long8_col__80_66");
                    colList.add("column_long8_col__80_67");
                    colList.add("column_long8_col__80_68");
                    colList.add("column_long8_col__80_69");
                    colList.add("column_long8_col__80_70");
                    colList.add("column_long8_col__80_71");
                    colList.add("column_long8_col__80_72");
                    colList.add("column_long8_col__80_73");
                    colList.add("column_long8_col__80_74");
                    colList.add("column_long8_col__80_75");
                    colList.add("column_long8_col__80_76");
                    colList.add("column_long8_col__80_77");
                    colList.add("column_long8_col__80_78");
                    colList.add("column_long8_col__80_79");
                    colList.add("column_long8_col__80_80");
                    colList.add("column_datetime1_col__1_1");
                    colList.add("column_datetime2_col__1_1");
                    colList.add("column_datetime3_col__1_1");
                }

                while (col_num_List.size() < 80) {
                    Integer next = (rng.nextInt(954) + 1) - 1;
                    // As we're adding to a set, this will automatically do a containment check
                    col_num_List.add(next);
                }

            }

            for (int col_num : col_num_List) {
                tab_cols.add(colList.get(col_num));
            } */



           String dt=getDate(tab_name);

            Calendar cal = Calendar.getInstance();
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            cal.setTime(dateFormat.parse(dt));// all done
            String date_now = dateFormat.format(cal.getTime());
            cal.add(Calendar.MINUTE, -1);

            String date_replace = dateFormat.format(cal.getTime());
            String cmdlistadd = "select " + tab_cols + " from " + tab_name + " where column_datetime3_col__1_1 " + date_line + "'" + date_replace + "'" + date_line2 + "'" + date_now + "' GROUP BY HOUR(column_datetime3_col__1_1), "+tab_cols+";";

            if (QUERY_EXECUTION_TEST==1) {
                cmdlistadd = "select count(*) from " + tab_name +" where column_datetime3_col__1_1 " + date_line + "'" + date_replace + "'" + date_line2 + "'" + date_now+"';";
            }
           /* if (DB_NAME.equalsIgnoreCase("paraccel")) {
                cmdList.add(cmdlistadd.replace("HOUR(column_datetime3_col__1_1)", "DATEPART(HOUR, column_datetime3_col__1_1)"));
            }  */


            if (DB_NAME.equalsIgnoreCase("EXASOL_DB")) {
                cmdList.add(cmdlistadd.replace("[", "").replace("]", "").replace("/","-"));
                }else{
                cmdList.add(cmdlistadd.replace("[", "").replace("]", ""));
                }

            tab_cols.clear();
            col_num_List.clear();
            i = i + 1;
        } while (i < query_total);

    }

    private static int randomTableNumber(String table_type, String table_size) {
        int tab_num;
        tab_num = 1;
        Random rand = new Random();
        if (table_type.equalsIgnoreCase("narrow") && table_size.equalsIgnoreCase("el"))
            tab_num = rand.nextInt(2) + 19;
        else if (table_type.equalsIgnoreCase("narrow") && table_size.equalsIgnoreCase("l")) {

            tab_num = rand.nextInt(4) + 15;

        } else if (table_type.equalsIgnoreCase("medium") && table_size.equalsIgnoreCase("el")) {

            tab_num = rand.nextInt(8) + 73;

        } else if (table_type.equalsIgnoreCase("medium") && table_size.equalsIgnoreCase("l")) {

            tab_num = rand.nextInt(26) + 47;

        } else if (table_type.equalsIgnoreCase("wide") && table_size.equalsIgnoreCase("el")) {

            tab_num = 10;

        } else if (table_type.equalsIgnoreCase("wide") && table_size.equalsIgnoreCase("l")) {

            tab_num = rand.nextInt(2) + 8;

        }

        return tab_num;
    }

    protected static void executeThreads(List<DBQueryWorkerThread> queryCommandList, ThreadPoolExecutor executor) {
        for (Runnable t : queryCommandList) {
            executor.submit(t);
        }
        while (!executor.getQueue().isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new DDLApplyException("Executor Monitor Interrupted :\n", e);
            }
        }
    }

    /**
     * Print the usage
     */
    private static void usage() {

        System.out.println("-c -- Config file");
        System.out.println("-o -- Directory to write file containing Execution timings to");
        System.out.println("-t -- testing class flag, 1 for test  (-t 1|-t 0)");
        System.out.println("-f -- full or light load indicator (-f full|-f lite) ");
        System.out.println("-p -- profile of the queries to run 1-7  (-t 1 |-t 8 {testing profile}) ");

    }

    /**
     * Process the arguements passed in form shell/script to a map
     *
     * @param args Args from shell/script
     * @return Map of arg name to arg value.
     */
    private static Map<String, String> processArgs(final String[] args) {
        final Map<String, String> argMap = new HashMap<>();
        for (int index = 0; index < args.length; index++) {
            if (args[index].startsWith("-")) {
                if (index >= args.length) {
                    throw new IndexOutOfBoundsException("Argument " + args[index] + " has no value");
                }
                argMap.put(args[index], args[index + 1]);
            }
        }
        return argMap;
    }

    /**
     * @param ds
     */
    public void processDDLCommands(DataSource ds) {


        ArrayList<DBQueryWorkerThread> queryCommandList = new ArrayList<>();


        for (String cmd : cmdList) {
            queryCommandList.add(new DBQueryWorkerThread(ds, cmd));
        }

        final ThreadPoolExecutor q_executor = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

    //   queryCommandList.get(0).run();

        executeThreads(queryCommandList, q_executor);
        q_executor.shutdown();
        while (!q_executor.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {/**/}
        }


        for (DBQueryWorkerThread thread : queryCommandList) {
            double average = thread.getTotalExecutionTime() / thread.getIteration();
            System.out.println("\nAVERAGE: " + average + ", COMMAND: " + thread.getCommand());
        }


        SQLExecutionTimer q = SQLExecutionTimer.getInstance();



        writeCommandExecutionTiming(q.printCommandSpecificTimeRecords());

        writeExecutionTiming(queryCommandList, "Query");


    }

    private void writeExecutionTiming(List<DBQueryWorkerThread> commandList, String operationType) {
        DDLWriter timeRecorder = new DDLWriter();
        timeRecorder.writeToFile(OUTPUT_FILE_LOCATION, commandList, operationType, OS_ENV_DELIMETER, profileNO,table_name_List);

    }

    private void writeCommandExecutionTiming(List<String> timerecords) {
        DDLWriter timeRecorder = new DDLWriter();
        for (String s : timerecords) {
            if (s.toLowerCase().startsWith("query")) {
                timeRecorder.writeToFile(OUTPUT_FILE_LOCATION + OS_ENV_DELIMETER + "Query.csv", s);
            }

        }
    }
    private static String getDate(String tab_name) {

        Connection conn = null;
        Statement stmt = null;
        String max_date = "";
        try{
            //STEP 2: Register JDBC driver
            Class.forName(DRIV);

            //STEP 3: Open a connection
            conn = DriverManager.getConnection(URL, USER, PASSWORD);

            //STEP 4: Execute a query
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT max(column_datetime3_col__1_1) from "+tab_name+";";
            ResultSet rs = stmt.executeQuery(sql);

            //STEP 5: Extract data from result set
            while(rs.next()){
                max_date=rs.getTimestamp(1).toString().replace("-","/");

            }
            //STEP 6: Clean-up environment
            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }finally{
            //finally block used to close resources
            try{
                if(stmt!=null)
                    stmt.close();
            }catch(SQLException se2){
            }// nothing we can do
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
        return max_date;
    }



}
