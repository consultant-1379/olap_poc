package com.ericsson.nms.dg;

import com.ericsson.nms.dg.schema.DatabaseType;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.UnmarshalException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventLocator;
import javax.xml.bind.util.ValidationEventCollector;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * User: eeipca
 */
public class DataConfigParser {
  private static final String DDL_GENERATOR_CLASS_KEY = "ddl_generator_class";
  private static final String SCHEMA_PACKAGE = System.getProperty("SCHEMA_PACKAGE", "com.ericsson.nms.dg.schema");
  public static final String SCHEMA_TABLEDEF_FILE;
  public static final String SCHEMA_LOADDEF_FILE;

  /**
   * Check we can see the XSD file. If the SCHEMA property isnt set, look up the value on the classpath
   */
  static {
    SCHEMA_TABLEDEF_FILE = linkSchema(System.getProperty("SCHEMA_DATAGEN", "datagen.xsd"));
    SCHEMA_LOADDEF_FILE = linkSchema(System.getProperty("SCHEMA_DATAGEN", "ropgen.xsd"));
  }

  private static String linkSchema(final String schemaFile) {
    if (!new File(schemaFile).exists()) {
      final URL url = ClassLoader.getSystemResource(schemaFile);
      if (url == null) {
        throw new RuntimeException("XML schema file not found!", new FileNotFoundException(schemaFile));
      }
      try {
        return url.toURI().getPath();
      } catch (URISyntaxException e) {
        throw new RuntimeException("XML schema file not found!", e);
      }
    } else {
      return schemaFile;
    }
  }

  /**
   * Print the usage
   */
  private static void usage() {
    System.out.println("-i -- Input XML file");
    System.out.println("-o -- Directory to write DDL to");
    System.out.println("-c -- Config file");
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
   * Main function
   *
   * @param args Args from shell (or script)
   */
  public static void main(final String[] args) {
    if (args == null || args.length == 0) {
      usage();
      System.exit(0);
    }
    final Map<String, String> options = processArgs(args);
    if (!options.containsKey("-i")) {
      System.out.println("No input file provided!");
      usage();
      System.exit(0);
    }
    if (!options.containsKey("-c")) {
      System.out.println("No config file specified!");
      usage();
      System.exit(0);
    }
    final String xmlFile = options.get("-i");
    final String configFile = options.get("-c");
    final DatabaseType data = parse(xmlFile, SCHEMA_TABLEDEF_FILE, DatabaseType.class);
    final Properties config = loadProperties(configFile);
    final DdlGenerator generator = getDdlGenerator(config);
    generator.generate(data, config);
  }

  /**
   * Load the DDL generator class.
   * The class is loaded via the ddl_generator_class property in the config file
   *
   * @param config The config properties
   * @return DefaultDDLGenerator instance
   * @throws GenerateException
   */
  private static DdlGenerator getDdlGenerator(final Properties config) throws GenerateException {
    final String ddlGeneratorClass = config.getProperty(DDL_GENERATOR_CLASS_KEY);
    if (ddlGeneratorClass == null || ddlGeneratorClass.length() == 0) {
      throw new GenerateException("No " + DDL_GENERATOR_CLASS_KEY + " entry found in config!");
    }
    try {
      final Class<?> klass = Class.forName(ddlGeneratorClass);
      final Class<? extends DdlGenerator> targetClass = klass.asSubclass(DdlGenerator.class);
      return targetClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new GenerateException("No class called " + ddlGeneratorClass + " found on CLASSPATH", e);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new GenerateException("Failed to instantiate " + ddlGeneratorClass, e);
    }
  }

  /**
   * Load the config properties file to a Properties Object
   *
   * @param configFile The config file to load
   * @return Properties Object representing the configFile
   */
  private static Properties loadProperties(final String configFile) throws GenerateException {
    final Properties config = new Properties();
    try {
      config.load(new FileInputStream(configFile));
    } catch (IOException e) {
      throw new GenerateException(e);
    }
    return config;
  }

  /**
   * Unmarshal (and validate) the XML file to JAXB Objects matching the schema.
   *
   * @param xmlFile The database definition file.
   * @return Root JAXB Object.
   */
  public static <T> T parse(final String xmlFile, final String schemaFile, final Class<T> klass) throws GenerateException {
    try {
      final JAXBContext jaxbContext = JAXBContext.newInstance(SCHEMA_PACKAGE);
      final Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
      unmarshaller.setSchema(getSchema(schemaFile));
      final ValidationEventCollector validationEventCollector = new ValidationEventCollector();
      unmarshaller.setEventHandler(validationEventCollector);
      try {
        final File file = new File(xmlFile);
        if (!file.exists()) {
          throw new GenerateException(new FileNotFoundException(xmlFile));
        }
        final Source source = new StreamSource(new FileInputStream(file));
        final JAXBElement<T> contents = unmarshaller.unmarshal(source, klass);
        return contents.getValue();
      } catch (UnmarshalException ue) {
        final StringBuilder sb = new StringBuilder();
        if (validationEventCollector.hasEvents()) {
          for (ValidationEvent ve : validationEventCollector.getEvents()) {
            final String msg = ve.getMessage();
            final ValidationEventLocator vel = ve.getLocator();
            final int line = vel.getLineNumber();
            final int column = vel.getColumnNumber();
            sb.append("Validation error: ").append(xmlFile).append(": Line:").append(line).append(" Column:").append(column).append(": ").append(msg);
          }
          throw new GenerateException(sb.toString());
        } else {
          throw new GenerateException("Failed to parse " + xmlFile, ue);
        }
      } catch (FileNotFoundException e) {
        throw new GenerateException("Failed to parse " + xmlFile, e);
      }
    } catch (final JAXBException e) {
      throw new GenerateException("Failed to parse " + xmlFile, e);
    }
  }

  /**
   * Get an instance a Schema from an XSD file
   *
   * @param schemaFile The XSD file
   * @return A Schema object instance
   */
  private static Schema getSchema(final String schemaFile) throws GenerateException {
    final SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    try {
      return sf.newSchema(new File(schemaFile));
    } catch (final SAXException e) {
      throw new GenerateException("Failed to parse" + schemaFile, e);
    }
  }
}

