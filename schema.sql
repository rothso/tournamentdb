CREATE TABLE IF NOT EXISTS players
(
  player_id   INT UNSIGNED PRIMARY KEY,
  tag         VARCHAR(30)          NOT NULL UNIQUE,
  real_name   VARCHAR(255)         NOT NULL,
  nationality CHAR(2)              NOT NULL,
  birthday    DATE                 NOT NULL,
  game_race   ENUM ('T', 'P', 'Z') NOT NULL
);

CREATE TABLE IF NOT EXISTS teams
(
  team_id   INT UNSIGNED PRIMARY KEY,
  name      VARCHAR(255) NOT NULL,
  founded   DATE         NOT NULL,
  disbanded DATE
);

CREATE TABLE IF NOT EXISTS members
(
  player     INT UNSIGNED NOT NULL,
  team       INT UNSIGNED NOT NULL,
  start_date DATE         NOT NULL,
  end_date   DATE,
  PRIMARY KEY (player, start_date),
  FOREIGN KEY (player) REFERENCES players (player_id),
  FOREIGN KEY (team) REFERENCES teams (team_id)
);

CREATE TABLE IF NOT EXISTS tournaments
(
  tournament_id INT UNSIGNED PRIMARY KEY,
  name          VARCHAR(255) NOT NULL,
  region        ENUM ('AM', 'EU', 'KR'),
  major         BOOL         NOT NULL
);

CREATE TABLE IF NOT EXISTS matches
(
  match_id   INT UNSIGNED PRIMARY KEY,
  date       DATE             NOT NULL,
  tournament INT UNSIGNED     NOT NULL,
  playerA    INT UNSIGNED     NOT NULL,
  playerB    INT UNSIGNED     NOT NULL,
  scoreA     TINYINT UNSIGNED NOT NULL,
  scoreB     TINYINT UNSIGNED NOT NULL,
  offline    BOOL             NOT NULL,
  FOREIGN KEY (tournament) REFERENCES tournaments (tournament_id),
  FOREIGN KEY (playerA) REFERENCES players (player_id),
  FOREIGN KEY (playerB) REFERENCES players (player_id)
);

CREATE TABLE IF NOT EXISTS earnings
(
  tournament  INT UNSIGNED     NOT NULL,
  player      INT UNSIGNED     NOT NULL,
  prize_money INT UNSIGNED     NOT NULL,
  position    TINYINT UNSIGNED NOT NULL,
  PRIMARY KEY (tournament, player),
  FOREIGN KEY (tournament) REFERENCES tournaments (tournament_id),
  FOREIGN KEY (player) REFERENCES players (player_id)
);