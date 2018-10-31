/**
 * AssignFlights2, a JDBC program for Homework 4 of cs430/630 Fall 2016
 * Reads tables flights1, aircraft1, certified1, and start_location to
 * find employees (pilots) certified for aircraft that have cruising range
 * sufficient to fly as directed in flights1 to the destination in one hop.
 * The pilots need to be at the origin city, as recorded in start_location. 
 * The program fills tables flight_assignmants and delayed_flights with
 * successful assignments and flights delayed because no pilot is available,
 * respectively. Table new_location is filled to record where the pilots end up.
 * 
 * This version of the program is written without using a cursor across flights1,
 * to handle the more dynamic case in which flights could be added to flights1
 * while this program is running. The assignFlight() method now finds an
 * unprocessed flight in the flights1 table and does the work for it.
 * An additional column "is_processed" in flights1 records whether or not
 * an individual flight has been processed.
 * After the flights are processed this way, the new_location table is filled.
 * 
 * The program produces more output than a production program would, to aid
 * in its understanding. Student solutions may produce much less output.
 * The official results of this program are in tables assigned_flights,
 * delayed_flights, and new_location.
 */

import java.io.IOException;
import java.util.Scanner;
import java.sql.*;

public class tryComplexQuery {
	private static Connection connection = null;

	public static void main(String args[]) {
		String dbSys = null;
		String username = null;
		String password = null;
		String connStr = null;
		Scanner in = null;
		try {
			in = new Scanner(System.in);
			System.out.println("Please enter information for connection to the database");
			dbSys = readEntry(in, "Using Oracle (o) or MySql (m)? ");
			// Prompt the user for connect information
			if (dbSys.equals("o")) {
				username = readEntry(in, "Oracle username: ");
				password = readEntry(in, "Oracle password: ");
				String host = readEntry(in, "host: ");
				String port = readEntry(in, "port (often 1521): ");
				String sid = readEntry(in, "sid (site id): ");
				connStr = "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid;
			} else if (dbSys.equals("m")) {// MySQL--
				username = readEntry(in, "MySQL username: ");
				password = readEntry(in, "MySQL password: ");
				String host = readEntry(in, "host: ");
				String port = readEntry(in, "port (often 3306): ");
				String db = username + "db";
				connStr = "jdbc:mysql://" + host + ":" + port + "/" + db;
			}
		} catch (IOException e) {
			System.out.println("Problem with user input, please try again\n");
			System.exit(1);
		}
		System.out.println("using connection string: " + connStr);
		System.out.print("Connecting to the database...");

		try {
			connection = getConnected(connStr, username, password);
		} catch (SQLException except) {
			System.out.println("Problem with JDBC Connection");
			System.out.println(except.getMessage());
			System.exit(2);
		} 

		Statement stmt = null;
    ResultSet rset = null;
		try {
			// Create a statement
			stmt = connection.createStatement();

  String q = "SELECT e.eid FROM employees e"
    + "WHERE NOT EXISTS ("
    + "(SELECT a.aid FROM aircraft a WHERE a.cruisingrange < 2000 and a.aid <> 16)"
    + " MINUS " 
    + "(SELECT c.aid FROM certified c WHERE c.eid=e.eid))";
rset = stmt.executeQuery(q);
while (rset.next()) {
  System.out.println(rset.getInt("eid") + "\n");
}
  System.out.println("Done");
} finally {
  stmt.close();
  }
}
	public static Connection getConnected(String connStr, String user, String password) throws SQLException {

		System.out.println("using connection string: " + connStr);
		System.out.print("Connecting to the database...");
		System.out.flush();

		// Connect to the database
		Connection conn = DriverManager.getConnection(connStr, user, password);
		System.out.println("connected.");
		return conn;
	}

	public static void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close(); // this also closes the Statement and
								// ResultSet, if any
			} catch (SQLException e) {
				System.out.println("Problem with closing JDBC Connection\n");
				printSQLException(e);
			}
		}
	}

	// print out all exceptions connected to e by nextException or getCause
	static void printSQLException(SQLException e) {
		// SQLExceptions can be delivered in lists (e.getNextException)
		// Each such exception can have a cause (e.getCause, from Throwable)
		while (e != null) {
			System.out.println("SQLException Message:" + e.getMessage());
			Throwable t = e.getCause();
			while (t != null) {
				System.out.println("SQLException Cause:" + t);
				t = t.getCause();
			}
			e = e.getNextException();
		}
	}

	// super-simple prompted input from user
	public static String readEntry(Scanner in, String prompt) throws IOException {
		System.out.print(prompt);
		return in.nextLine().trim();
	}

}
