import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

class FindPilot1 {
	public static void main(String args[]) {
		String dbSys = null;
		Scanner in = null;
		try {
			in = new Scanner(System.in);
			System.out
					.println("Please enter information to test connection to the database");
			dbSys = readEntry(in, "Using Oracle (o), MySql (m) or HSQLDB (h)? ");

		} catch (IOException e) {
			System.out.println("Problem with user input, please try again\n");
			System.exit(1);
		}
		// Prompt the user for connect information
		String user = null;
		String password = null;
		String connStr = null;
		String yesNo;
		try {
			if (dbSys.equals("o")) {
				user = readEntry(in, "user: ");
				password = readEntry(in, "password: ");
				yesNo = readEntry(in,
						"use canned Oracle connection string (y/n): ");
				if (yesNo.equals("y")) {
					String host = readEntry(in, "host: ");
					String port = readEntry(in, "port (often 1521): ");
					String sid = readEntry(in, "sid (site id): ");
					connStr = "jdbc:oracle:thin:@" + host + ":" + port + ":"
							+ sid;
				} else {
					connStr = readEntry(in, "connection string: ");
				}
			} else if (dbSys.equals("m")) {// MySQL--
				user = readEntry(in, "user: ");
				password = readEntry(in, "password: ");
				yesNo = readEntry(in,
						"use canned MySql connection string (y/n): ");
				if (yesNo.equals("y")) {
					String host = readEntry(in, "host: ");
					String port = readEntry(in, "port (often 3306): ");
					String db = user + "db";
					connStr = "jdbc:mysql://" + host + ":" + port + "/" + db;
				} else {
					connStr = readEntry(in, "connection string: ");
				}
			} else if (dbSys.equals("h")) { // HSQLDB (Hypersonic) db
				yesNo = readEntry(in,
						"use canned HSQLDB connection string (y/n): ");
				if (yesNo.equals("y")) {
					String db = readEntry(in, "db or <CR>: ");
					connStr = "jdbc:hsqldb:hsql://localhost/" + db;
				} else {
					connStr = readEntry(in, "connection string: ");
				}
				user = "sa";
				password = "";
			} else {
				user = readEntry(in, "user: ");
				password = readEntry(in, "password: ");
				connStr = readEntry(in, "connection string: ");
			}
		} catch (IOException e) {
			System.out.println("Problem with user input, please try again\n");
			System.exit(3);
		}
		System.out.println("using connection string: " + connStr);
		System.out.print("Connecting to the database...");
		System.out.flush();
		Connection conn = null;
		// Connect to the database
		// Use finally clause to close connection
		try {
			conn = DriverManager.getConnection(connStr, user, password);
			System.out.println("connected.");
			findPilot(conn);
		} catch (SQLException e) {
			System.out.println("Problem with JDBC Connection\n");
			printSQLException(e);
			System.exit(4);
		} finally {
			// Close the connection, if it was obtained, no matter what happens
			// above or within called methods
			if (conn != null) {
				try {
					conn.close(); // this also closes the Statement and
									// ResultSet, if any
				} catch (SQLException e) {
					System.out
							.println("Problem with closing JDBC Connection\n");
					printSQLException(e);
					System.exit(5);
				}
			}
		}
	}

	
	static void findPilot(Connection conn) throws SQLException 
	{

		PreparedStatement p = null;
		ResultSet rset = null;
		PreparedStatement p1 = null, p2 = null;


String query = "select f.origin, f.flno, f.destination, f.distance, e.ename, e.eid, a.aid, a.aname, a.cruisingrange, e.salary from flights f, aircraft a, certified c, employees e1, employees e where a.aid=c.aid and e.eid=c.eid and a.cruisingrange > f.distance and e.salary < e1.salary and E.eid not in (Select fa.eid from assigned_flights fa) order by e.salary, f.departs";

      	try {
				p = conn.prepareStatement(query);
				rset = p.executeQuery();
        while (rset.next()) {

String eid = rset.getString("eid");
String flno = rset.getString("flno");
String aid = rset.getString("eid");
int eid1 = rset.getInt("eid");
int flno1 = rset.getInt("flno");
int aid1 = rset.getInt("eid");

			if (eid1 > 0)   // found eid inserting into assigned_flights
			{
      p = conn.prepareStatement(query);
				String query_4 = "INSERT INTO assigned_flights(flno, aid, eid)  " + "VALUES (?, ?, ?)";
				p1 = conn.prepareStatement(query_4);
				p1.setInt(1, flno1);
				p1.setInt(2, aid1);
				p1.setInt(3, eid1);
				p1.executeUpdate();
				p1.close();
				System.out.println("assigned_flights row inserted");
			} else {  // inserting delayed_flight
				String query_5 = "INSERT INTO delayed_flights (flno)  " + "VALUES (?)";
				p2 = conn.prepareStatement(query_5);
				p2.setInt(1, flno1);
				p2.executeUpdate();
				p2.close();
			System.out.println("delayed_flight" + flno + " from "
					+ " delayed_flights row inserted");
		}
  }

		} finally {
			p.close();
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
	public static String readEntry(Scanner in, String prompt)
			throws IOException {
		System.out.print(prompt);
		return in.nextLine().trim();
	}
}
