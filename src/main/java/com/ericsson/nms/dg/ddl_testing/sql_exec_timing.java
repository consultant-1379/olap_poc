package com.ericsson.nms.dg.ddl_testing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Properties;


/**
 * Class to execute the SQL statements towards databases.
 *
 */
public class sql_exec_timing {

	static Statement stmt;
	static Connection con;
	static StringBuilder sb = new StringBuilder();
	static HashSet<String> commandSet = new HashSet<String>();

	static String userid;
	static String password;
	static String driver;
	static String url;
	static int iterations;
	static String sql_fileLocation;
	static String output_fileLocation;

	public static void main(String args[]) {
		try{
			return_statment();
		} catch (DGException dge) {
			dge.printStackTrace();
		}

	}

	public static void writeToFile(String pFilename, StringBuilder sb)
			throws IOException {
		BufferedWriter out = new BufferedWriter(new FileWriter(pFilename));
		out.write(sb.toString());
		out.flush();
		out.close();
	}

	public static Connection getConnection() {
	
		try {
			con = DriverManager.getConnection(url, userid, password);

		} catch (SQLException ex) {
			System.err.println("SQLException: " + ex.getMessage());
		}

		return con;
	}

	static public String getContents(File aFile) {

		StringBuilder contents = new StringBuilder();

		try {

			BufferedReader input = new BufferedReader(new FileReader(aFile));
			try {
				String line = null; // not declared within while loop
				/*
				 * readLine is a bit quirky : it returns the content of a line
				 * MINUS the newline. it returns null only for the END of the
				 * stream. it returns an empty String if two newlines appear in
				 * a row.
				 */
				while ((line = input.readLine()) != null) {
					contents.append(line);
					contents.append(System.getProperty("line.separator"));
				}
			} finally {
				input.close();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		return contents.toString();
	}

	public static void return_statment() throws DGException {
	
		Properties prop = new Properties();

		// load a properties file
		try {
			prop.load(new FileInputStream(
					"C:\\olap_poc\\olap_poc\\src\\main\\resources\\conf\\file_sybase.properties"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		userid = prop.getProperty("userid");
		password = prop.getProperty("password");
		driver = prop.getProperty("driver");
		url = prop.getProperty("url");
		iterations = Integer.parseInt(prop.getProperty("iterations"));
		sql_fileLocation = prop.getProperty("sql_fileLocation");
		output_fileLocation = prop.getProperty("output_fileLocation");
		
		try{
			Class.forName(driver);
		} catch (ClassNotFoundException cnfe) {
			throw new DGException(cnfe, "Failed to load class " + driver
					+ " Make sure the class/jar is on the CLASSPATH");
		}

		File[] testFiles = new File(sql_fileLocation).listFiles();

		Arrays.sort(testFiles, new Comparator<File>() {
			public int compare(File f1, File f2) {
				return Long.valueOf(f1.lastModified()).compareTo(
						f2.lastModified());
			}
		});
		
		Connection con = null;
				
		try{
		
			con = getConnection();

			for (int x = 1; x <= iterations; x++) {
				System.out.println("Starting Iteration " + x);
				for (File file : testFiles) {					
					String String = getContents(file);
					String command = file.getName();
					commandSet.add(command);
					try {
						stmt = con.createStatement();
						long start_time = System.nanoTime();
						stmt.executeUpdate(String);
						long end_time = System.nanoTime();
						double exec_sec = (end_time - start_time) / 1e6;

						sb.append(x + "," + command + "," + exec_sec + "\n");

					} catch (SQLException ex) {
						System.err.println("SQLException: " + ex.getMessage());
					} finally {
						try {
							stmt.close();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

				}
				System.out.println("Finihing Interation " + x);
			}
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (Throwable t) {
					/* Ignore this... */}
			}

		}
		try {
			System.out.println("Writing out execution times to output files");
			for (String s : commandSet) {
				String[] lines = sb.toString().split("\\n");
				StringBuilder sb_local = new StringBuilder();
				String output_location = output_fileLocation + "\\" + s
						+ ".txt";
				for (String line : lines) {
					if (line.toLowerCase().contains(s.toLowerCase())) {
						String string_replace = s + ",";
						sb_local.append(line.replace(string_replace, "") + "\n");

					}

					writeToFile(output_location, sb_local);
				}
			}
		} catch (IOException ex) {
			throw new DGException(ex, "Failed to write changes to file");
		} finally {
			System.out
					.println("Finished writing out execution times to output files");
		}
	}
}

class DGException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DGException(final Throwable cause, final String message) {
		super(message, cause);
	}
}

// End of class

