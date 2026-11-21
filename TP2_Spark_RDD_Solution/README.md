# Solution TP2 — Spark RDD (cluster Docker + arbres.csv)

## Contenu
```
TP2_Spark_RDD_Solution/
├── arbres.py                  # programme PySpark : questions (1) a (4)
├── data/arbres_sample.csv     # echantillon FICTIF (6 lignes) pour tester le script
└── commands/
    ├── 01_docker_cluster.sh   # hote : reseau, 3 conteneurs, copie du programme et des donnees
    ├── 02_spark_in_master.sh  # master : fichier slaves, start-all.sh, spark-submit
    └── 03_test_local.sh       # test sans cluster
```
Les `.sh` sont des **listes de commandes a copier une par une**.

## Ce que fait arbres.py
1. `textFile` → RDD, `count()` = nombre de lignes (avec et sans en-tete).
2. Hauteur moyenne = somme des hauteurs (`reduce`) / `count()`.
3. Paires (hauteur, genre) → `sortByKey(False)` → `first()` → genre du plus grand arbre.
4. Paires (genre, 1) → `reduceByKey(a+b)` → nombre d'arbres par genre.

## Verifie / non verifie
- **Verifie :** `arbres.py` execute avec pyspark (mode local) sur l'echantillon fictif ; resultats corrects (moyenne 20.33, Platanus, 3/2/1).
- **Non verifie :** le vrai `arbres.csv` (non fourni dans les documents) et le cluster Docker. Le script detecte le separateur (`;` ou `,`) et les colonnes **par leur nom** dans l'en-tete : `GENRE` et `HAUTEURENM`. Si votre fichier nomme ces colonnes autrement, modifiez les deux lignes `colonnes.index(...)`.
- Lignes a hauteur non numerique ou ligne incomplete : ignorees pour (2) et (3). Les hauteurs a 0 (valeur manquante frequente) sont comptees dans la moyenne ; pour les exclure, ajouter `.filter(lambda p: p[0] > 0)` apres `paires`.

## Points d'attention (cluster)
- Le CSV doit exister **au meme chemin sur master et workers** (`/root/arbres.csv`) ; sinon : chargez-le dans HDFS (`start-hadoop.sh`, `hadoop fs -put`) et passez `hdfs:///user/root/arbres.csv` en argument.
- `start-all.sh` demarre les workers par SSH : si erreur, `service ssh start` sur les 3 conteneurs.
- Chemin Spark suppose : `/usr/local/spark` (indique par l'enonce).
