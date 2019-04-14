import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Scanner;

/**
 * @author Rothanak So
 */
public class n01128755_Assign4 {

  public static void main(String[] args) throws Exception {
    // Create the database
    SportDatabase db = new SportDatabase();

    // Insert data into the database
    db.createSchema();
    try {
      db.insert("players", "players.csv");
      db.insert("teams", "teams.csv");
      db.insert("members", "members.csv");
      db.insert("tournaments", "tournaments.csv");
      db.insert("matches", "matches_v2.csv");
      db.insert("earnings", "earnings.csv");
    } catch (BatchUpdateException e) {
      // Data is already inserted, silently ignore...
    }

    // Query the database
    int option;
    Scanner input = new Scanner(System.in);
    showMenu();
    while ((option = input.nextInt()) != 0) {
      input.nextLine();
      switch (option) {
        case 1:
          System.out.print("Enter the birth year: ");
          int year = input.nextInt();
          System.out.print("Enter the birth month: ");
          int month = input.nextInt();
          db.query1(year, month);
          break;
        case 2:
          System.out.print("Enter the player id: ");
          int player = input.nextInt();
          System.out.print("Enter the team id: ");
          int team = input.nextInt();
          db.query2(player, team);
          break;
        case 3:
          System.out.print("Enter the nationality: ");
          String nationality = input.nextLine();
          System.out.print("Enter the birth year: ");
          int birthYear = input.nextInt();
          db.query3(nationality, birthYear);
          break;
        case 4:
          db.query4();
          break;
        case 5:
          db.query5();
          break;
        case 6:
          db.query6();
          break;
        case 7:
          db.query7();
          break;
        default:
          System.out.println("Invalid option, please try again.");
      }
      showMenu();
    }

    // Print an exit message
    System.out.println("Goodbye!");

    // Drop the database
    db.close();
  }

  private static void showMenu() {
    System.out.println("\n" +
        "0: Quit\n" +
        "1: Get players by month and year of birth\n" +
        "2: Add a player to a team\n" +
        "3: Get players by nationality and year of birth\n" +
        "4: Get players who have attained a triple crown\n" +
        "5: Get former members of the team \"Root Gaming\"\n" +
        "6: Get the players with the highest P vs T winrates\n" +
        "7: Get teams founded before 2011 that are still active\n");
  }
}

class SportDatabase implements Closeable {
  private static final String DB_NAME = "PlayerDB_Assign4";
  private static final String DB_URL = "jdbc:mysql://localhost:3306/?useSSL=false";
  private static final String USER = "root";
  private static final String PASS = "root";

  private final Connection conn;
  private final Statement stmt;

  SportDatabase() throws SQLException {
    conn = DriverManager.getConnection(DB_URL, USER, PASS);

    stmt = conn.createStatement();
    stmt.execute("CREATE DATABASE IF NOT EXISTS " + DB_NAME);
    stmt.execute("USE " + DB_NAME);
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

    System.out.println("Inserted table " + table + " from " + fileName);
  }

  void query1(int birthYear, int birthMonth) throws SQLException {
    ResultSet rs = stmt.executeQuery(
        "SELECT real_name, tag, nationality\n" +
            "FROM players\n" +
            "WHERE YEAR(birthday) = " + birthYear + "\n" +
            "  AND MONTH(birthday) = " + birthMonth);
    if (rs.next()) {
      int i = 1;
      System.out.printf("    %-20s %-20s %-20s\n", "Real Name", "Tag", "Nationality");
      do {
        System.out.printf("%-3d %-20s %-20s %-20s\n", i++,
            rs.getString("real_name"),
            rs.getString("tag"),
            rs.getString("nationality"));
      } while (rs.next());
    } else {
      System.out.println("No results found.");
    }
  }

  void query2(int playerId, int teamId) throws SQLException {
    int rowsUpdated = stmt.executeUpdate(
        "UPDATE members\n" +
            "SET end_date = NOW()\n" +
            "where player = " + playerId + "\n" +
            "  AND team != " + teamId + "\n" +
            "  AND end_date IS NULL");
    try {
      int rowsInserted = stmt.executeUpdate(
          "INSERT INTO members\n" +
              "SELECT " + playerId + ", " + teamId + ", CURRENT_DATE(), NULL\n" +
              "FROM members\n" +
              "WHERE NOT EXISTS(SELECT *\n" +
              "                 FROM members\n" +
              "                 WHERE player = " + playerId + "\n" +
              "                   AND TEAM = " + teamId + "\n" +
              "                   AND end_date IS NULL)\n" +
              "LIMIT 1");
      if (rowsUpdated > 0)
        System.out.println("Departed player from old team.");
      System.out.println(rowsInserted > 0
          ? "Added player to new team."
          : "Player already on team.");
    } catch (SQLIntegrityConstraintViolationException e) {
      // Either the player ID or team ID does not exist
      System.out.println("Unable to assign player to team (player or team does not exist).");
    }
  }

  void query3(String nationality, int birthYear) throws SQLException {
    ResultSet rs = stmt.executeQuery(
        "SELECT real_name, birthday\n" +
            "FROM players\n" +
            "WHERE nationality = '" + nationality + "'\n" +
            "  AND YEAR(birthday) = " + birthYear);
    if (rs.next()) {
      int i = 1;
      System.out.printf("    %-20s %-20s\n", "Real Name", "Birthday");
      do {
        System.out.printf("%-3d %-20s %-20s\n", i++,
            rs.getString("real_name"),
            rs.getString("birthday"));
      } while (rs.next());
    } else {
      System.out.println("No results found.");
    }
  }

  void query4() throws SQLException {
    ResultSet rs = stmt.executeQuery(
        "SELECT tag, game_race\n" +
            "FROM (\n" +
            "         SELECT *\n" +
            "         FROM players\n" +
            "                  INNER JOIN earnings e ON players.player_id = e.player\n" +
            "                  INNER JOIN tournaments t ON e.tournament = t.tournament_id\n" +
            "         WHERE major = TRUE\n" +
            "           AND position = 1\n" +
            "           AND region IS NOT NULL\n" +
            "         GROUP BY tag, region) as t1\n" +
            "GROUP BY tag\n" +
            "HAVING COUNT(*) = 3"
    );
    int i = 1;
    System.out.printf("    %-20s %-20s\n", "Tag", "Game Race");
    while (rs.next()) {
      System.out.printf("%-3d %-20s %-20s\n", i++,
          rs.getString("tag"),
          rs.getString("game_race"));
    }
  }

  void query5() throws SQLException {
    ResultSet rs = stmt.executeQuery(
        "SELECT tag, real_name, MAX(end_date) as departure\n" +
            "FROM members\n" +
            "         INNER JOIN teams t on members.team = t.team_id\n" +
            "         INNER JOIN players p on members.player = p.player_id\n" +
            "WHERE name = 'Root Gaming'\n" +
            "  AND player NOT IN (\n" +
            "    SELECT player\n" +
            "    FROM members\n" +
            "             INNER JOIN teams t on members.team = t.team_id\n" +
            "    WHERE name = 'Root Gaming'\n" +
            "      AND end_date is NULL)\n" +
            "GROUP BY player"
    );
    int i = 1;
    System.out.printf("    %-20s %-20s %-20s\n", "Tag", "Real Name", "Most Recent Departure");
    while (rs.next()) {
      System.out.printf("%-3d %-20s %-20s %-20s\n", i++,
          rs.getString("tag"),
          rs.getString("real_name"),
          rs.getString("departure"));
    }
  }

  void query6() throws SQLException {
    ResultSet rs = stmt.executeQuery(
        "SELECT p1.tag, p1.nationality, SUM(IF(scoreA > scoreB, 1, 0)) / COUNT(*) * 100 as " +
            "winrate\n" +
            "FROM (SELECT playerA, playerB, scoreA, scoreB\n" +
            "      FROM matches\n" +
            "      UNION ALL\n" +
            "      SELECT playerB, playerA, scoreB, scoreA\n" +
            "      FROM matches) m\n" +
            "         INNER JOIN players p1 ON m.playerA = p1.player_id AND p1.game_race = 'P'\n" +
            "         INNER JOIN players p2 ON m.playerB = p2.player_id AND p2.game_race = 'T'\n" +
            "GROUP BY playerA\n" +
            "HAVING COUNT(*) >= 10\n" +
            "   AND winrate > 65\n" +
            "ORDER BY winrate DESC"
    );
    int i = 1;
    System.out.printf("    %-20s %-20s %-20s\n", "Tag", "Nationality", "P vs. T Win Rate");
    while (rs.next()) {
      System.out.printf("%-3d %-20s %-20s %-20s\n", i++,
          rs.getString("tag"),
          rs.getString("nationality"),
          rs.getString("winrate"));
    }
  }

  void query7() throws SQLException {
    ResultSet rs = stmt.executeQuery(
        "SELECT name,\n" +
            "       founded,\n" +
            "       SUM(CASE WHEN game_race = 'P' THEN 1 ELSE 0 END) as protoss,\n" +
            "       SUM(CASE WHEN game_race = 'T' THEN 1 ELSE 0 END) as terran,\n" +
            "       SUM(CASE WHEN game_race = 'Z' THEN 1 ELSE 0 END) as zerg\n" +
            "FROM teams\n" +
            "         INNER JOIN members m on teams.team_id = m.team\n" +
            "         INNER JOIN players p on m.player = p.player_id\n" +
            "WHERE YEAR(founded) < 2011\n" +
            "  AND disbanded IS NULL\n" +
            "  AND end_date IS NULL\n" +
            "GROUP BY name\n" +
            "ORDER BY name"
    );
    int i = 1;
    System.out.printf("    %-20s %-20s %-20s %-20s %-20s\n",
        "Name", "Founded", "# Protoss", "# Terran", "# Zerg");
    while (rs.next()) {
      System.out.printf("%-3d %-20s %-20s %-20s %-20s %-20s\n", i++,
          rs.getString("name"),
          rs.getString("founded"),
          rs.getString("protoss"),
          rs.getString("terran"),
          rs.getString("zerg"));
    }
  }

  @Override
  public void close() throws IOException {
    try {
      stmt.execute("DROP DATABASE " + DB_NAME);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
