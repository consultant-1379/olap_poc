package com.ericsson.nms.dg.ddl;

import com.ericsson.nms.dg.GenerateException;
import com.ericsson.nms.dg.dbquery.DBQueryWorkerThread;
import com.ericsson.nms.dg.ddlapply.DBWorkerThread;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * The Class is intended for handling all file writing tasks.
 * Features writeToFile() (overloaded) method 
 *  Created with IntelliJ IDEA.
 * User: qbhasha
 * Date: 16/10/13
 * Time: 10:39
 */
public class DDLWriter {


  String OUTPUT_FILE_NAME = "";
  final static String CMD_DELIMITER = ";";
  final static Charset ENCODING = StandardCharsets.UTF_8;
  String ext = "";

  /**
   * Base implementation. expects FilePATH and a list of strings to write.
   * It appends command delimiter at the end of each string object read from
   * List provided.
   *
   * @param filePath   Output file path
   * @param acmd     List containing DDL command
   */
  public void writeToFile(String filePath, List<String> acmd) {
    Path path = Paths.get(filePath);
    if(!path.toFile().getParentFile().exists()){
      path.toFile().getParentFile().mkdirs();
    }
    try (BufferedWriter writer = Files.newBufferedWriter(path, ENCODING)){
      for(String line : acmd){
        writer.write(line + CMD_DELIMITER);
        writer.newLine();
      }
    } catch (IOException e) {
      throw new GenerateException("Failed to write to the file:  " + path +"\n Path passed to the method was : " +filePath + "\n", e);
    }

  }
  public void writeToFile(String pFilename, StringBuilder sb) {

    BufferedWriter out = null;
    try {
      out = new BufferedWriter(new FileWriter(pFilename));
      out.write(sb.toString());
      out.flush();
      out.close();

    } catch (IOException e) {
      throw new GenerateException("Failed to write to the file:  " + pFilename +"\n Path passed to the method was : " +sb.toString() + "\n", e);
    }
  }

  /**
   *  Overloaded method. generates FilePATH from table ID and DDL type objects.
   *  Table ID can be narrow, wide or medium.
   *  DDL Type can be operation itself CREATE, DROP, ALTER
   *
   * @param ddlType  DDL command type
   * @param tableID  Table identifier
   * @param acmd    List containing DDL command
   *
   */
  public void writeToFile(String ddlType, String tableID, List<String> acmd) {
    String filePath = generateFilePath(tableID, ddlType);
    writeToFile(filePath,acmd);
  }

  /**
   *  Overloaded method. generates FilePATH from database name, table ID and DDL type objects.
   *  dbName can be Sybase, posix etc.
   *  Table ID can be narrow, wide or medium.
   *  DDL Type can be operation itself CREATE, DROP, ALTER
   *
   * @param dbName Database name
   * @param ddlType  DDL command type
   * @param tableID  Table identifier
   * @param acmd    List containing DDL command
   */
  public void writeToFile(String dbName, String ddlType, String tableID, List<String> acmd) {
    String filePath = generateFilePath(dbName, tableID, ddlType);
    writeToFile(filePath,acmd);
  }

  /**
   *  Overloaded method. generates FilePATH from table ID and DDL type, database name
   *  output directory objects.
   *
   *
   * @param dbName Database name
   * @param ddlType  DDL command type
   * @param tableID  Table identifier
   * @param acmd    List containing DDL command
   * @param prop    Properties
   */
  public void writeToFile(String dbName, String ddlType, String tableID, List<String> acmd, Properties prop ) {
    if (ddlType.equalsIgnoreCase("ALTER") || ddlType.toLowerCase().startsWith("alter")) {
      OUTPUT_FILE_NAME = prop.getProperty("upgrade_output_dir");
    }
    else {
      OUTPUT_FILE_NAME = prop.getProperty("install_output_dir");
    }
    ext =    prop.getProperty("output_file_extension");
    String filePath = generateFilePath(dbName, tableID, ddlType);
    writeToFile(filePath,acmd);
  }

  /**
   *  Overloaded method. Required for ALTER TABLE command where there must be two sets of files.
   *  1. List of commands for all the tables in a single file.
   *  2. Files for each data-type variation in the column which is supposed to be altered. eg. VARCHAR(256),
   *     INT, BIGINT, DATETIME etc.
   *
   * @param dbName Database name
   * @param ddlType  DDL command type
   * @param tableID  Table identifier
   * @param acmd    List containing DDL command
   * @param prop    Properties
   * @param datatype Column data-type
   */
  public void writeToFile(String dbName, String ddlType, String tableID, List<String> acmd, Properties prop, List<String> datatype ) {
    OUTPUT_FILE_NAME = prop.getProperty("upgrade_output_dir");
    ext =    prop.getProperty("output_file_extension");
    for(String dtype: datatype) {
      String filePath = generateFilePath(dbName, tableID, ddlType, dtype);
      List<String> datatypecmd = new ArrayList<>();
      for(String s: acmd){
        if (s.contains(dtype)) {
          if (s.contains("DROP")) {
            datatypecmd.add(s.substring(0,s.indexOf("#")));
          }
          else if (s.contains("ADD")) {
            datatypecmd.add(s);
          }
        }

      }
      writeToFile(filePath,datatypecmd);
    }
  }

  /**
   * Base method for generation of FILE PATH.
   * Uses Table type(ID) and DDL type objects.
   *  Table ID can be narrow, wide or medium.
   *  DDL Type can be operation itself CREATE, DROP, ALTER
   *
   * @param tableID    Table identifier
   * @param ddlType    DDL command type
   * @return   Output file path
   */
  private String generateFilePath( String tableID, String ddlType ) {
    String cmdType = getCmdType(ddlType);

    return (OUTPUT_FILE_NAME + cmdType +"_" + tableID + "." + ext);
  }

  private String getCmdType(String ddlType) {
    if (ddlType.contains(" ")) {
      return ddlType.substring(0,ddlType.indexOf(" "));
    }
    return ddlType;
  }

  /**
   * Overloaded method to Include Database name for FILE PATH generation.
   *
   * @param dbName  Database Name
   * @param tableID Table identifier
   * @param ddlType DDL command type
   * @return     Output file path
   */
  private String generateFilePath(String dbName, String tableID, String ddlType) {
    String cmdType = getCmdType(ddlType);

    return (OUTPUT_FILE_NAME + cmdType +"_" + tableID +"_" + "for"+ dbName +"." + ext);
  }

  /**
   * Overloaded method to Include Database name and Data type for FILE PATH generation.
   * Primarily used to distinguish the commands clubbed together for ALTER TABLE commands
   * based upon Column's data type (VARCHAR, INT, BIGINT etc.).
   *
   * @param dbName  Database Name
   * @param tableID  Table identifier
   * @param ddlType  DDL command type
   * @param datatype  Column data-type
   * @return  Output file path
   */
  private String generateFilePath(String dbName, String tableID, String ddlType, String datatype) {
    String cmdType = getCmdType(ddlType);
    return (OUTPUT_FILE_NAME + datatype + "_" + cmdType +"_" + tableID +"_" + "for"+ dbName + "." + ext);
  }

  public void writeToFile(String output_file_location, List<DBWorkerThread> commandList, String operationType, String del) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

    Date d = new Date();
    String dt = dateFormat.format(d).toString();
    String filePath = output_file_location + del + operationType + del + operationType + "AverageTiming-" + dt + ".csv";
    Path path = Paths.get(filePath);
    if(!path.toFile().getParentFile().exists()){
      path.toFile().getParentFile().mkdirs();
    }
    try (BufferedWriter writer = Files.newBufferedWriter(path, ENCODING)){
      writer.write("COMMAND, ITERATIONS, AVERAGE(millisecond)");
      writer.newLine();
      String command = "";
      for (DBWorkerThread thread : commandList) {
        double average = thread.getTotalExecutionTime() / thread.getIteration();
        if(thread.getCommand().toLowerCase().startsWith("create") || thread.getCommand().toLowerCase().startsWith("alter")) {
          command = thread.getCommand().substring(0,thread.getCommand().indexOf(" ("));
        }else if(thread.getCommand().toLowerCase().startsWith("insert")) {
          command = "AGGREGATE " + thread.getCommand().substring((thread.getCommand().indexOf("FROM") + 4), (thread.getCommand().indexOf("WHERE") -1) ).trim();
        }
        else {
          command = thread.getCommand().substring(0,thread.getCommand().length()-1);
        }
        writer.write(command + ", " + thread.getIteration() + ", " + average );
        writer.newLine();
        System.out.println("\nAVERAGE: " + average + ", ITERATION: " + thread.getIteration() + ", COMMAND: " + thread.getCommand());
      }
      writer.flush();
      writer.close();
    } catch (IOException e) {
      throw new GenerateException("Failed to write to the file:  " + path +"\n Path passed to the method was : " +filePath + "\n", e);
    }
    finally {

    }


  }
    public void writeToFile(String output_file_location, List<DBQueryWorkerThread> commandList, String operationType, String del, int Profile, List<String> table_name_List) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

        Date d = new Date();
        String dt = dateFormat.format(d).toString();
        String filePath = output_file_location + del + operationType + del + del + Profile + del + operationType + "AverageTiming-" + dt + ".csv";
        Path path = Paths.get(filePath);
        if(!path.toFile().getParentFile().exists()){
            path.toFile().getParentFile().mkdirs();
        }
        try (BufferedWriter writer = Files.newBufferedWriter(path, ENCODING)){
            writer.write("Profile, AVERAGE, Table_Name");
            writer.newLine();
            String command = "";
            int table_num=0;
            for (DBQueryWorkerThread thread : commandList) {
                double average = thread.getTotalExecutionTime() / thread.getIteration();
                //command = thread.getCommand().substring(0,thread.getCommand().length()-1);
                writer.write(Profile + ", " + average + ", " + table_name_List.get(table_num)/*+ ", " + command*/ );
                writer.newLine();
             table_num++;
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new GenerateException("Failed to write to the file:  " + path +"\n Path passed to the method was : " +filePath + "\n", e);
        }
        finally {

        }


    }

  public void writeToFile(String output_file_location, String s1) {
    String filePath = output_file_location ;
    Path path = Paths.get(filePath);
    if(!path.toFile().getParentFile().exists()){
      path.toFile().getParentFile().mkdirs();
    }
    try (BufferedWriter writer = Files.newBufferedWriter(path, ENCODING)){

      writer.write("COMMAND, START TIME, END TIME, EXECUTION TIME (millisecond)");
      writer.newLine();
      writer.write(s1);
      writer.flush();
      writer.close();
    } catch (IOException e) {
      throw new GenerateException("Failed to write to the file:  " + path +"\n Path passed to the method was : " +filePath + "\n", e);
    }
  }
}
