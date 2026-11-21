# -*- coding: utf-8 -*-
# TP4 Spark MLlib -- genere depuis le notebook ; cellules separees par '# %%'
# %%
# # TP4 — Spark MLlib (KMeans sur les achats)
# Données : fichiers du dossier `tp4_data` (colonnes : InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country).
# %%
# %pip install -q pyspark
# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder.appName("TP4_MLlib").getOrCreate()
# %%
# ## 1) Stocker le contenu de tous les fichiers de `tp4_data` dans un DataFrame
# %%
DATA = "/content/tp4_data/*.csv"     # Colab : dossier tp4_data importé dans /content

df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")     # InvoiceDate -> timestamp, UnitPrice -> double, ...
      .load(DATA))
print(df.count(), "lignes")
# %%
# ## 2) Afficher le schéma
# %%
df.printSchema()
# %%
# ## 3) Remplacer les valeurs manquantes par 0
# `na.fill(0)` ne touche que les colonnes **numériques** (ex. `CustomerID`) ; les colonnes texte (ex. `Description`) restent inchangées.
# %%
df = df.na.fill(0)
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()   # nulls restants
# %%
# ## 4) Nouvelle colonne `day_of_week` (jour de la semaine de `InvoiceDate`)
# %%
df = df.withColumn("day_of_week", F.date_format(F.col("InvoiceDate"), "EEEE"))   # Monday, Tuesday...
df.select("InvoiceDate", "day_of_week").show(5, False)
# %%
# ## 5) Ensemble d'apprentissage (avant 2010-12-13) / ensemble de test (à partir de 2010-12-13)
# %%
train = df.where(F.col("InvoiceDate") < F.lit("2010-12-13").cast("timestamp"))
test  = df.where(F.col("InvoiceDate") >= F.lit("2010-12-13").cast("timestamp"))
print("train :", train.count(), "| test :", test.count())
# %%
# ## 6) StringIndexer : jours de la semaine → valeurs numériques
# %%
indexer = StringIndexer(inputCol="day_of_week", outputCol="day_of_week_index",
                        handleInvalid="keep")   # "keep" : un jour inconnu du train ne fait pas planter le test
# %%
# ## 7) Problème de l'ordre implicite (samedi = 6 > lundi = 1)
# **Solution : One-Hot Encoding** → chaque jour devient un indicateur 0/1 dans un vecteur, sans ordre.
# %%
encoder = OneHotEncoder(inputCol="day_of_week_index", outputCol="day_of_week_encoded")
# %%
# ## 8) VectorAssembler : UnitPrice, Quantity, day_of_week_encoded
# %%
vectorAssembler = VectorAssembler(inputCols=["UnitPrice", "Quantity", "day_of_week_encoded"],
                                  outputCol="features")
# %%
# ## 9) Pipeline (étapes 6, 7, 8)
# %%
pipeline = Pipeline(stages=[indexer, encoder, vectorAssembler])
# %%
# ## 10) Le StringIndexer doit connaître les valeurs uniques à indexer
# **Solution : `fit` du pipeline sur l'ensemble d'apprentissage** (le StringIndexer y apprend les jours distincts et leur index).
# %%
fittedPipeline = pipeline.fit(train)
# %%
# ## 11) Transformer l'ensemble d'apprentissage
# %%
transformedTraining = fittedPipeline.transform(train)
transformedTraining.select("UnitPrice", "Quantity", "day_of_week", "day_of_week_index", "day_of_week_encoded", "features").show(5, False)
# %%
# ## 12) Instance de KMeans (k = 20)
# %%
kmeans = KMeans(k=20, seed=1, featuresCol="features")
# %%
# ## 13) Apprentissage de KMeans sur l'ensemble de l'étape 11
# %%
kmModel = kmeans.fit(transformedTraining)
# %%
# ## 14) Prédictions sur l'ensemble de test
# Le test doit d'abord passer par le **pipeline déjà ajusté** (même indexation que le train).
# %%
predictions = kmModel.transform(fittedPipeline.transform(test))
predictions.select("UnitPrice", "Quantity", "day_of_week", "features", "prediction").show(5, False)
# %%
# ## 15) Coefficient de silhouette
# %%
evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction",
                                metricName="silhouette")      # silhouette (distance euclidienne au carré)
silhouette = evaluator.evaluate(predictions)
print("Silhouette = ", silhouette)      # entre -1 et 1 : plus c'est proche de 1, mieux les clusters sont séparés
