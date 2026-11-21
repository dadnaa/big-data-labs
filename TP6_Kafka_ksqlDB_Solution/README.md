# Solution TP6 — Kafka et ksqlDB (surveillance des modifications Netflix)

## Contenu
```
TP6_Kafka_ksqlDB_Solution/
├── docker-compose.yml          # ⚠️ standard (zookeeper, broker, ksqldb-server:8088, ksqldb-cli) — utilisez celui du prof s'il existe
└── sql/
    ├── 01_setup.sql            # questions 1, 2, 3, 4, 7, 9, 10 (+ SHOW/DESCRIBE)
    └── 02_push_queries.sql     # questions 5, 6, 8 (+ vérification de 10) : requêtes Push, à lancer une par une
```

## Exécution
```bash
docker-compose up -d
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088     # énoncé : même commande
ksql> RUN SCRIPT '/sql/01_setup.sql';                         # (dossier sql monté dans ksqldb-cli par ce compose)
ksql> SHOW STREAMS; SHOW TABLES; SHOW QUERIES;
```
Puis copier-coller les requêtes de `02_push_queries.sql` **une par une** (Ctrl+C pour arrêter chaque requête Push).
Si vous n'utilisez pas ce compose, collez simplement le contenu de `01_setup.sql` instruction par instruction.

## Vérifié / non vérifié
- **Rien n'a été exécuté** (pas de Docker ni de ksqlDB dans mon environnement). Les instructions suivent la syntaxe des slides du Chapitre 4 et de l'énoncé du TP6 ; ma connaissance de ksqlDB sert pour les détails (STRUCT, INSERT, jointure).
- Points les plus susceptibles de demander un ajustement selon votre version de ksqlDB :
  1. `STRUCT(season_id := CAST(NULL AS INT), ...)` (valeur NULL dans un STRUCT) ;
  2. la **clé** du stream de la question 9 (jointure stream-table : la clé du résultat est la colonne de jointure) ;
  3. le `GROUP BY title` de la question 10 (l'énoncé dit « pour chaque title » ; les slides regroupent par `title_id`) ;
  4. la requête Pull sur table fenêtrée.
- Les données insérées (question 4) sont **inventées** : l'énoncé ne donne qu'un exemple d'enregistrement par topic.

## Choix / interprétations
- **titles = TABLE** (référence : dernier état par `id`, jointure sur la clé primaire) ; **production_changes = STREAM** (événements immuables).
- `uuid` des slides n'est pas déclaré : il n'est pas dans l'exemple d'enregistrement de l'énoncé.
- **Q8** : les « titres » sont les `title_id` (le nom n'est disponible qu'après la jointure de Q9).
- **Q5** : comparaison de chaînes ISO sur `created_at` (VARCHAR).
