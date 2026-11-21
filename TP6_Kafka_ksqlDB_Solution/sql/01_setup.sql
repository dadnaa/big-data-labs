-- TP6 : Kafka et ksqlDB — surveillance des modifications Netflix
-- A executer dans le CLI :  RUN SCRIPT '/sql/01_setup.sql';   (ou instruction par instruction)
-- Les requetes Push (EMIT CHANGES sans CREATE) sont dans 02_push_queries.sql (elles ne se terminent pas).

-- Lire les topics depuis le debut
SET 'auto.offset.reset' = 'earliest';

-- ===== 1) Type personnalise (type de before et after) =====
CREATE TYPE season_length AS STRUCT<season_id INT, episode_count INT>;
SHOW TYPES;

-- ===== 2) + 3) Collections =====
-- titles : TABLE (donnee de reference : dernier etat par cle id ; sert d'enrichissement par jointure)
-- production_changes : STREAM (suite immuable d'evenements : chaque changement compte)
CREATE TABLE titles (
  id INT PRIMARY KEY,
  title VARCHAR
) WITH (
  KAFKA_TOPIC='titles',
  PARTITIONS=4,
  VALUE_FORMAT='JSON'
);

CREATE STREAM production_changes (
  rowkey VARCHAR KEY,
  title_id INT,
  change_type VARCHAR,
  before season_length,
  after season_length,
  created_at VARCHAR
) WITH (
  KAFKA_TOPIC='production_changes',
  PARTITIONS=4,
  VALUE_FORMAT='JSON',
  TIMESTAMP='created_at',
  TIMESTAMP_FORMAT='yyyy-MM-dd HH:mm:ss'
);

SHOW TABLES;
SHOW STREAMS;
DESCRIBE titles;
DESCRIBE production_changes;

-- ===== 4) Insertion de donnees (jeu d'essai invente) =====
INSERT INTO titles (id, title) VALUES (1, 'Stranger Things');
INSERT INTO titles (id, title) VALUES (2, 'Black Mirror');
INSERT INTO titles (id, title) VALUES (3, 'The Crown');

INSERT INTO production_changes (rowkey, title_id, change_type, before, after, created_at)
VALUES ('key1', 1, 'season_length', STRUCT(season_id := 1, episode_count := 12), STRUCT(season_id := 1, episode_count := 8), '2021-02-08 11:30:00');

INSERT INTO production_changes (rowkey, title_id, change_type, before, after, created_at)
VALUES ('key2', 1, 'season_length', STRUCT(season_id := 1, episode_count := 8), STRUCT(season_id := 1, episode_count := 10), '2021-02-08 11:45:00');

INSERT INTO production_changes (rowkey, title_id, change_type, before, after, created_at)
VALUES ('key3', 2, 'season_length', STRUCT(season_id := 1, episode_count := 6), STRUCT(season_id := 1, episode_count := 4), '2022-05-10 09:00:00');

-- after->season_id NULL : on doit retomber sur before->season_id (question 7)
INSERT INTO production_changes (rowkey, title_id, change_type, before, after, created_at)
VALUES ('key4', 3, 'season_length', STRUCT(season_id := 2, episode_count := 10), STRUCT(season_id := CAST(NULL AS INT), episode_count := 8), '2023-06-01 14:20:00');

-- autres types de changement (pas de before/after)
INSERT INTO production_changes (rowkey, title_id, change_type, created_at)
VALUES ('key5', 2, 'release_date', '2022-07-01 10:00:00');

INSERT INTO production_changes (rowkey, title_id, change_type, created_at)
VALUES ('key6', 1, 'season_budget', '2023-04-14 11:59:59');

-- ===== 7) Stream derive : uniquement les changements de duree de saison =====
CREATE STREAM season_length_changes
WITH (
  KAFKA_TOPIC = 'season_length_changes',
  VALUE_FORMAT = 'JSON',
  PARTITIONS = 4,
  REPLICAS = 1
) AS SELECT
  ROWKEY,
  title_id,
  IFNULL(after->season_id, before->season_id) AS season_id,
  before->episode_count AS old_episode_count,
  after->episode_count AS new_episode_count,
  created_at
FROM production_changes
WHERE change_type = 'season_length'
EMIT CHANGES;

-- ===== 9) Stream enrichi : jointure stream-table avec titles =====
CREATE STREAM season_length_changes_enriched
WITH (
  KAFKA_TOPIC = 'season_length_changes_enriched',
  VALUE_FORMAT = 'JSON',
  PARTITIONS = 4,
  TIMESTAMP = 'created_at',
  TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss'
) AS SELECT
  s.title_id AS title_id,
  s.ROWKEY AS rowkey,
  t.title AS title,
  s.season_id AS season_id,
  s.old_episode_count AS old_episode_count,
  s.new_episode_count AS new_episode_count,
  s.created_at AS created_at
FROM season_length_changes s
INNER JOIN titles t ON s.title_id = t.id
EMIT CHANGES;

-- ===== 10) Table derivee : nb de changements par titre + derniere duree, fenetre tumbling 1 heure =====
CREATE TABLE season_length_change_counts
WITH (
  KAFKA_TOPIC = 'season_length_change_counts',
  VALUE_FORMAT = 'JSON',
  PARTITIONS = 4
) AS SELECT
  title,
  COUNT(*) AS change_count,
  LATEST_BY_OFFSET(new_episode_count) AS episode_count
FROM season_length_changes_enriched
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY title
EMIT CHANGES;

SHOW QUERIES;
