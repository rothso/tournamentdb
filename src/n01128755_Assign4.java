import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

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

      SportDatabase db = new SportDatabase(stmt);
      db.createSchema();

      // Drop the database
      //stmt.execute("DROP DATABASE " + DB_NAME);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

class SportDatabase {
  private final Statement stmt;

  SportDatabase(Statement stmt) {
    this.stmt = stmt;
  }

  void createSchema() throws SQLException {
    stmt.execute("CREATE TABLE IF NOT EXISTS players\n" +
        "(\n" +
        "  player_id   INT UNSIGNED PRIMARY KEY,\n" +
        "  tag         VARCHAR(30)          NOT NULL UNIQUE,\n" +
        "  real_name   VARCHAR(255)         NOT NULL,\n" +
        "  nationality CHAR(2)              NOT NULL,\n" +
        "  birthday    DATE                 NOT NULL,\n" +
        "  game_race   ENUM ('T', 'P', 'Z') NOT NULL\n" +
        ");");

    stmt.execute("CREATE TABLE IF NOT EXISTS teams\n" +
        "(\n" +
        "  team_id   INT UNSIGNED PRIMARY KEY,\n" +
        "  name      VARCHAR(255) NOT NULL,\n" +
        "  founded   DATE         NOT NULL,\n" +
        "  disbanded DATE\n" +
        ");");

    stmt.execute("CREATE TABLE IF NOT EXISTS members\n" +
        "(\n" +
        "  player     INT UNSIGNED NOT NULL,\n" +
        "  team       INT UNSIGNED NOT NULL,\n" +
        "  start_date DATE         NOT NULL,\n" +
        "  end_date   DATE,\n" +
        "  PRIMARY KEY (player, start_date),\n" +
        "  FOREIGN KEY (player) REFERENCES players (player_id),\n" +
        "  FOREIGN KEY (team) REFERENCES teams (team_id)\n" +
        ");");

    stmt.execute("CREATE TABLE IF NOT EXISTS tournaments\n" +
        "(\n" +
        "  tournament_id INT UNSIGNED PRIMARY KEY,\n" +
        "  name          VARCHAR(255) NOT NULL,\n" +
        "  region        ENUM ('AM', 'EU', 'KR'),\n" +
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
        ");");

    stmt.execute("CREATE TABLE IF NOT EXISTS earnings\n" +
        "(\n" +
        "  tournament  INT UNSIGNED     NOT NULL,\n" +
        "  player      INT UNSIGNED     NOT NULL,\n" +
        "  prize_money INT UNSIGNED     NOT NULL,\n" +
        "  position    TINYINT UNSIGNED NOT NULL,\n" +
        "  PRIMARY KEY (tournament, player),\n" +
        "  FOREIGN KEY (tournament) REFERENCES tournaments (tournament_id),\n" +
        "  FOREIGN KEY (player) REFERENCES players (player_id)\n" +
        ");");
  }
}