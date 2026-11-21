# -*- coding: utf-8 -*-
# TP3 Spark SQL -- genere depuis le notebook ; les cellules sont separees par '# %%'
# %%
# # TP3 — Spark SQL (dataset N-Gram)
# Colonnes : `ngram` (String), `Year` (int), `Count` (int), `Pages` (int), `Books` (int).
# Chaque question : **(1) SQL** puis **(2) API DataFrame**.
# %%
# Google Colab : installer pyspark (dans un fichier .py, cette ligne est un commentaire)
# %pip install -q pyspark
# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("TP3_SparkSQL").getOrCreate()
# %%
# ## 1) Créer un DataFrame à partir de ngram.csv
# Séparateur (`\t` ou `,`) et présence d'un en-tête sont détectés automatiquement (le fichier réel n'étant pas fourni).
# %%
PATH = "/content/ngram.csv"     # Colab : importer le fichier dans /content

premiere_ligne = spark.sparkContext.textFile(PATH).first()
SEP = "\t" if "\t" in premiere_ligne else ","
HEADER = not premiere_ligne.split(SEP)[1].strip().isdigit()   # 2e champ = Year : non numerique => en-tete

schema = StructType([
    StructField("ngram", StringType()),
    StructField("Year",  IntegerType()),
    StructField("Count", IntegerType()),
    StructField("Pages", IntegerType()),
    StructField("Books", IntegerType()),
])
df = spark.read.csv(PATH, sep=SEP, header=HEADER, schema=schema)
df.printSchema()
df.show(5, False)
# %%
# ## 2) Enregistrer le DataFrame comme table temporaire
# %%
df.createOrReplaceTempView("ngram")   # (registerTempTable des slides est déprécié)
# %%
# ## 3.1 Bi-grammes dont Count > 5
# %%
sql1 = spark.sql("SELECT * FROM ngram WHERE `Count` > 5")
api1 = df.filter(F.col("Count") > 5)
sql1.show(5, False)
# %%
# ## 3.2 Nombre total de bi-grammes dans chaque année
# (ici = nombre de lignes par année ; si l'on veut le nombre total d'occurrences : `SUM(Count)`)
# %%
sql2 = spark.sql("SELECT Year, COUNT(*) AS nb_bigrammes FROM ngram GROUP BY Year ORDER BY Year")
api2 = df.groupBy("Year").agg(F.count("*").alias("nb_bigrammes")).orderBy("Year")
sql2.show()
# %%
# ## 3.3 Bi-grammes ayant le plus grand Count dans chaque année
# (les ex æquo sont conservés)
# %%
sql3 = spark.sql("""
    SELECT n.*
    FROM ngram n
    JOIN (SELECT Year, MAX(`Count`) AS maxc FROM ngram GROUP BY Year) m
      ON n.Year = m.Year AND n.`Count` = m.maxc
    ORDER BY n.Year
""")
maxpa = df.groupBy("Year").agg(F.max("Count").alias("maxc"))
api3 = (df.join(maxpa, "Year")
          .filter(F.col("Count") == F.col("maxc"))
          .drop("maxc").orderBy("Year"))
sql3.show(5, False)
# %%
# ## 3.4 Bi-grammes apparus dans 20 années différentes
# %%
sql4 = spark.sql("SELECT ngram FROM ngram GROUP BY ngram HAVING COUNT(DISTINCT Year) = 20")
api4 = (df.groupBy("ngram").agg(F.countDistinct("Year").alias("nb_annees"))
          .filter(F.col("nb_annees") == 20).select("ngram"))
sql4.show(5, False)
# %%
# ## 3.5 `!` dans la 1re partie et `9` dans la 2e (parties séparées par un espace)
# %%
sql5 = spark.sql("""
    SELECT * FROM ngram
    WHERE split(ngram, ' ')[0] LIKE '%!%' AND split(ngram, ' ')[1] LIKE '%9%'
""")
parts = F.split(F.col("ngram"), " ")
api5 = df.filter(parts.getItem(0).contains("!") & parts.getItem(1).contains("9"))
sql5.show(5, False)
# %%
# ## 3.6 Bi-grammes apparus dans toutes les années présentes dans les données
# %%
sql6 = spark.sql("""
    SELECT ngram FROM ngram
    GROUP BY ngram
    HAVING COUNT(DISTINCT Year) = (SELECT COUNT(DISTINCT Year) FROM ngram)
""")
nb_annees = df.select("Year").distinct().count()
api6 = (df.groupBy("ngram").agg(F.countDistinct("Year").alias("nb_annees"))
          .filter(F.col("nb_annees") == nb_annees).select("ngram"))
sql6.show(5, False)
# %%
# ## 3.7 Total de pages et de livres par bi-gramme et par année, trié alphabétiquement
# %%
sql7 = spark.sql("""
    SELECT ngram, Year, SUM(Pages) AS total_pages, SUM(Books) AS total_books
    FROM ngram GROUP BY ngram, Year ORDER BY ngram, Year
""")
api7 = (df.groupBy("ngram", "Year")
          .agg(F.sum("Pages").alias("total_pages"), F.sum("Books").alias("total_books"))
          .orderBy("ngram", "Year"))
sql7.show(5, False)
# %%
# ## 3.8 Nombre de bi-grammes différents par année, année décroissante
# %%
sql8 = spark.sql("""
    SELECT Year, COUNT(DISTINCT ngram) AS nb_bigrammes_distincts
    FROM ngram GROUP BY Year ORDER BY Year DESC
""")
api8 = (df.groupBy("Year").agg(F.countDistinct("ngram").alias("nb_bigrammes_distincts"))
          .orderBy(F.col("Year").desc()))
sql8.show()
# %%
# ## Vérification : SQL et API donnent-ils le même résultat ?
# %%
def lignes(d):
    return sorted(tuple(r) for r in d.select(sorted(d.columns)).collect())

for i in range(1, 9):
    a, b = globals()["sql%d" % i], globals()["api%d" % i]
    print("3.%d :" % i, "OK" if lignes(a) == lignes(b) else "DIFFERENT", "(%d lignes)" % a.count())
