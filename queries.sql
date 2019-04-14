USE PlayerDB_Assign4;

-- 1
SELECT real_name, tag, nationality
FROM players
WHERE YEAR(birthday) = 1990
  AND MONTH(birthday) = 12;

-- 2
UPDATE members
SET end_date = CURRENT_DATE()
where player = 6
  AND team != 7
  AND end_date IS NULL;

INSERT INTO members
SELECT 6, 8, CURRENT_DATE(), NULL
FROM members
WHERE NOT EXISTS(SELECT *
                 FROM members
                 WHERE player = 6
                   AND TEAM = 8
                   AND end_date IS NULL)
LIMIT 1;

-- 3
SELECT real_name, birthday
FROM players
WHERE nationality = 'KR'
  AND YEAR(birthday) = '1988';

-- 4 (good)
SELECT tag, game_race
FROM (
         SELECT *
         FROM players
                  INNER JOIN earnings e ON players.player_id = e.player
                  INNER JOIN tournaments t ON e.tournament = t.tournament_id
         WHERE major = TRUE
           AND position = 1
           AND region IS NOT NULL
         GROUP BY tag, region) as t1
GROUP BY tag
HAVING COUNT(*) = 3;

-- 5 (good)
SELECT tag, real_name, MAX(end_date) as depature
FROM members
         INNER JOIN teams t on members.team = t.team_id
         INNER JOIN players p on members.player = p.player_id
WHERE name = 'Root Gaming'
  AND player NOT IN (
    SELECT player
    FROM members
             INNER JOIN teams t on members.team = t.team_id
    WHERE name = 'Root Gaming'
      AND end_date is NULL)
GROUP BY player;

-- 6 (good)
SELECT p1.tag, p1.nationality, SUM(IF(scoreA > scoreB, 1, 0)) / COUNT(*) * 100 as winrate
FROM (SELECT playerA, playerB, scoreA, scoreB
      FROM matches
      UNION ALL
      SELECT playerB, playerA, scoreB, scoreA
      FROM matches) m
         INNER JOIN players p1 ON m.playerA = p1.player_id AND p1.game_race = 'P'
         INNER JOIN players p2 ON m.playerB = p2.player_id AND p2.game_race = 'T'
GROUP BY playerA
HAVING COUNT(*) >= 10
   AND winrate > 65
ORDER BY winrate DESC;

-- 7 (good)
SELECT name,
       founded,
       SUM(CASE WHEN game_race = 'P' THEN 1 ELSE 0 END) as protoss,
       SUM(CASE WHEN game_race = 'T' THEN 1 ELSE 0 END) as terran,
       SUM(CASE WHEN game_race = 'Z' THEN 1 ELSE 0 END) as zerg
FROM teams
         INNER JOIN members m on teams.team_id = m.team
         INNER JOIN players p on m.player = p.player_id
WHERE YEAR(founded) < 2011
  AND disbanded IS NULL
  AND end_date IS NULL
GROUP BY name
ORDER BY name;