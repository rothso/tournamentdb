import java.sql.*;

public class Test {
  private static final String DB_URL = "jdbc:mysql://localhost:3306/?useSSL=false";
  private static final String USER = "root";
  private static final String PASS = "root";

  public static void main(String[] args) {
    try (
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        Statement stmt = conn.createStatement()
    ) {
      // Create database
      String sql = "CREATE DATABASE DemoDB";
      stmt.executeUpdate(sql);
      System.out.println("Database created.");

      // Use database
      sql = "USE DemoDB";
      stmt.executeUpdate(sql);

      sql = "CREATE TABLE Employees(emp_id INT UNSIGNED, first_name VARCHAR(255), last_name " +
          "VARCHAR(255), birthday DATE, PRIMARY KEY (emp_id))";
      stmt.executeUpdate(sql);
      System.out.println("Tables created");

      sql = "INSERT INTO Employees VALUES (?, ?, ?, ?)";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.clearParameters();
      pstmt.setInt(1, 123);
      pstmt.setString(2, "George");
      pstmt.setString(3, "Joestar");
      pstmt.setString(4, "1970-01-11");
      pstmt.executeUpdate();
      pstmt.close();

      sql = "SELECT * FROM Employees";
      ResultSet myRS = stmt.executeQuery(sql);

      while (myRS.next()) {
        System.out.println("SSN is: " + myRS.getString("emp_id") +
            ", First name is: " + myRS.getString("first_name") +
            ", Last name is: " + myRS.getString("last_name"));
      }

      sql = "DROP DATABASE DemoDB";
      stmt.executeUpdate(sql);
      System.out.println("Database dropped.");
    } catch (SQLException se) {
      System.out.println("SQL Exception.");
      se.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.out.println("Run complete. Shutting down.");
  }
}