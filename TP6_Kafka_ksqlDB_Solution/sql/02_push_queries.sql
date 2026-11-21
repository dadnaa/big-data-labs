-- Requetes Push : a lancer UNE PAR UNE dans le CLI (elles ne se terminent pas : Ctrl+C pour arreter).
SET 'auto.offset.reset' = 'earliest';

-- ===== 5) Changements crees avant le 2023-04-14 12:00:00 =====
-- (created_at est un VARCHAR au format ISO : la comparaison de chaines respecte l'ordre chronologique)
SELECT * FROM production_changes
WHERE created_at < '2023-04-14 12:00:00'
EMIT CHANGES;

-- ===== 6) change_type commence par 'season' =====
SELECT * FROM production_changes
WHERE change_type LIKE 'season%'
EMIT CHANGES;

-- ===== 8) Titres (identifiants) des videos de season_length_changes =====
SELECT title_id FROM season_length_changes EMIT CHANGES;
-- (le NOM du titre n'existe qu'apres la jointure de la question 9 :)
SELECT title_id, title FROM season_length_changes_enriched EMIT CHANGES;

-- ===== Verification de la question 10 =====
SELECT * FROM season_length_change_counts EMIT CHANGES;

-- ===== Requete Pull (etape 5 de l'enonce, hors questions) : renvoie l'etat courant puis se termine =====
-- (non verifie sur table fenetree selon la version de ksqlDB)
SELECT * FROM season_length_change_counts WHERE title = 'Stranger Things';
