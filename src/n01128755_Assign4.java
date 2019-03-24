import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Scanner;

public class n01128755_Assign4 {
  private static final String DB_NAME = "PlayerDB_Assign4";
  private static final String DB_URL = "jdbc:mysql://localhost:3306/?useSSL=false";
  private static final String USER = "root";
  private static final String PASS = "root";

  public static void main(String[] args) {
    try (
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        Statement stmt = conn.createStatement()
    ) {
      // Create the database
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + DB_NAME);

      // Use the database
      stmt.execute("USE " + DB_NAME);

      SportDatabase db = new SportDatabase(conn, stmt);
      db.createSchema();
      db.insert("players", "players.csv");
      db.insert("teams", "teams.csv");
      db.insert("members", "members.csv");
      db.insert("tournaments", "tournaments.csv");
      db.insert("matches", "matches_v2.csv");
      db.insert("earnings", "earnings.csv");

      // Drop the database
      //stmt.execute("DROP DATABASE " + DB_NAME);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

class SportDatabase {
  private final Connection conn;
  private final Statement stmt;

  SportDatabase(Connection conn, Statement stmt) {
    this.conn = conn;
    this.stmt = stmt;
  }

  void createSchema() throws SQLException {
    stmt.execute("CREATE TABLE IF NOT EXISTS players\n" +
        "(\n" +
        "  player_id   INT UNSIGNED PRIMARY KEY,\n" +
        "  tag         VARCHAR(30) BINARY   NOT NULL UNIQUE,\n" +
        "  real_name   VARCHAR(255)         NOT NULL,\n" +
        "  nationality CHAR(2)              NOT NULL,\n" +
        "  birthday    DATE                 NOT NULL,\n" +
        "  game_race   ENUM ('T', 'P', 'Z') NOT NULL\n" +
        ")");

    stmt.execute("CREATE TABLE IF NOT EXISTS teams\n" +
        "(\n" +
        "  team_id   INT UNSIGNED PRIMARY KEY,\n" +
        "  name      VARCHAR(255) NOT NULL,\n" +
        "  founded   DATE         NOT NULL,\n" +
        "  disbanded DATE\n" +
        ")");

    stmt.execute("CREATE TABLE IF NOT EXISTS members\n" +
        "(\n" +
        "  player     INT UNSIGNED NOT NULL,\n" +
        "  team       INT UNSIGNED NOT NULL,\n" +
        "  start_date DATE         NOT NULL,\n" +
        "  end_date   DATE,\n" +
        "  PRIMARY KEY (player, start_date),\n" +
        "  FOREIGN KEY (player) REFERENCES players (player_id),\n" +
        "  FOREIGN KEY (team) REFERENCES teams (team_id)\n" +
        ")");

    stmt.execute("CREATE TABLE IF NOT EXISTS tournaments\n" +
        "(\n" +
        "  tournament_id INT UNSIGNED PRIMARY KEY,\n" +
        "  name          VARCHAR(255) NOT NULL,\n" +
        "  region        ENUM ('AM', 'EU', 'KR', 'AS'),\n" +
        "  major         BOOL         NOT NULL\n" +
        ");");

    stmt.execute("CREATE TABLE IF NOT EXISTS matches\n" +
        "(\n" +
        "  match_id   INT UNSIGNED PRIMARY KEY,\n" +
        "  date       DATE             NOT NULL,\n" +
        "  tournament INT UNSIGNED     NOT NULL,\n" +
        "  playerA    INT UNSIGNED     NOT NULL,\n" +
        "  playerB    INT UNSIGNED     NOT NULL,\n" +
        "  scoreA     TINYINT UNSIGNED NOT NULL,\n" +
        "  scoreB     TINYINT UNSIGNED NOT NULL,\n" +
        "  offline    BOOL             NOT NULL,\n" +
        "  FOREIGN KEY (tournament) REFERENCES tournaments (tournament_id),\n" +
        "  FOREIGN KEY (playerA) REFERENCES players (player_id),\n" +
        "  FOREIGN KEY (playerB) REFERENCES players (player_id)\n" +
        ")");

    stmt.execute("CREATE TABLE IF NOT EXISTS earnings\n" +
        "(\n" +
        "  tournament  INT UNSIGNED     NOT NULL,\n" +
        "  player      INT UNSIGNED     NOT NULL,\n" +
        "  prize_money INT UNSIGNED     NOT NULL,\n" +
        "  position    TINYINT UNSIGNED NOT NULL,\n" +
        "  PRIMARY KEY (tournament, player),\n" +
        "  FOREIGN KEY (tournament) REFERENCES tournaments (tournament_id),\n" +
        "  FOREIGN KEY (player) REFERENCES players (player_id)\n" +
        ")");
  }

  void insert(String table, String fileName) throws Exception {
    File file = new File(fileName);

    // Speed up insertions (~3 seconds instead of 7 minutes)
    conn.setAutoCommit(false);

    // Dynamically create the prepared statement
    int nCols = new Scanner(file).nextLine().split(",", -1).length;
    String placeholders = String.join(", ", Collections.nCopies(nCols, "?"));
    String sql = "INSERT INTO " + table + " VALUES (" + placeholders + ")";
    PreparedStatement insert = conn.prepareStatement(sql);

    // Insert using data from the file
    Scanner scanner = new Scanner(file);
    while (scanner.hasNextLine()) {
      String[] cols = scanner.nextLine().split(",", -1);
      for (int i = 0; i < nCols; i++) {
        String val = cols[i].replace("\"", "");
        if (val.equals("true")) insert.setBoolean(i + 1, true);
        else if (val.equals("false")) insert.setBoolean(i + 1, false);
        else insert.setString(i + 1, !val.equals("") ? val : null);
      }
      insert.addBatch();
    }
    insert.executeBatch();
    conn.commit();

    // Clean up
    insert.close();
    conn.setAutoCommit(true);
  }
}
