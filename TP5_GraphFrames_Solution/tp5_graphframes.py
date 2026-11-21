# -*- coding: utf-8 -*-
# TP5 GraphFrames -- genere depuis le notebook ; cellules separees par '# %%'
# %%
# # TP5 — Spark GraphFrames (stations et trajets de vélos)
# Fichiers : `station_data.csv` (stations → sommets) et `trip_data.csv` (voyages → arcs).
# %%
# Colab : pyspark 3.5 + wrapper Python de GraphFrames (la partie Java est chargée à l'étape suivante)
# Si Colab demande de redémarrer l'environnement : le faire, puis ré-exécuter à partir de la cellule suivante.
# %pip install -q pyspark==3.5.3 graphframes-py
# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import desc
from graphframes import GraphFrame

spark = (SparkSession.builder.appName("TP5_GraphFrames")
         .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12")
         .getOrCreate())
# %%
# ## 1) Un DataFrame par fichier
# %%
bikeStations = (spark.read.option("header", "true").option("inferSchema", "true")
                .csv("/content/station_data.csv"))
tripData = (spark.read.option("header", "true").option("inferSchema", "true")   # inferSchema : Duration numérique (sinon tri alphabétique à la question 9 !)
            .csv("/content/trip_data.csv"))
bikeStations.show(3, False)
tripData.printSchema()
# %%
# ## 2) Table des sommets : renommer `name` en `id`
# %%
stationVertices = bikeStations.withColumnRenamed("name", "id").distinct()
# %%
# ## 3) Table des arcs : `Start Station` → `src`, `End Station` → `dst`
# %%
tripEdges = (tripData
             .withColumnRenamed("Start Station", "src")
             .withColumnRenamed("End Station", "dst"))
# %%
# ## 4) Créer le GraphFrame
# %%
stationGraph = GraphFrame(stationVertices, tripEdges)
print("Stations :", stationGraph.vertices.count())
print("Voyages dans le graphe :", stationGraph.edges.count(), "| dans les données :", tripData.count())
# %%
# ## 5) Nombre de voyages entre chaque source et destination (décroissant)
# %%
stationGraph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(10, False)
# %%
# ## 6) Voyages qui commencent ou se terminent à 'Townsend at 7th' (décroissant)
# %%
(stationGraph.edges
    .where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
    .groupBy("src", "dst").count()
    .orderBy(desc("count"))
    .show(10, False))
# %%
# ## 7) Sommets qui n'ont jamais été destination d'un voyage partant de 'Spear at Folsom'
# %%
destinations = (stationGraph.edges.where("src = 'Spear at Folsom'")
                .select(F.col("dst").alias("id")).distinct())
jamais_destination = stationGraph.vertices.select("id").subtract(destinations)   # subtract : cf. cours Ch2 P2
jamais_destination.show(truncate=False)
# %%
# ## 8) Station avec le plus grand nombre de voyages entrants
# %%
stationGraph.inDegrees.orderBy(desc("inDegree")).show(1, False)
# %%
# ## 9) Voyage de plus grande durée
# `Duration` doit être numérique (inferSchema) ; sinon le tri serait alphabétique (« 9000 » > « 86000 »).
# %%
stationGraph.edges.orderBy(desc("Duration")).show(1, False)
# %%
# ## 10) Sous-graphe : voyages qui commencent ou se terminent à 'Townsend at 7th'
# %%
townAnd7thEdges = stationGraph.edges.where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
subgraph = GraphFrame(stationGraph.vertices, townAnd7thEdges)
print("Arcs du sous-graphe :", subgraph.edges.count())
# %%
# ## 11) Motif « triangle » entre trois stations
# %%
motifs = stationGraph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
motifs.select(F.col("a.id").alias("a"), F.col("b.id").alias("b"), F.col("c.id").alias("c")).show(10, False)
# %%
# ## 12) Chemins à trois sommets qui commencent à 'Townsend at 7th'
# %%
chemins = (stationGraph.find("(a)-[ab]->(b); (b)-[bc]->(c)")
           .filter("a.id = 'Townsend at 7th'"))
chemins.select(F.col("a.id").alias("a"), F.col("b.id").alias("b"), F.col("c.id").alias("c")).show(10, False)
# Variante « trois sommets tous différents » :
# chemins.filter("b.id != a.id AND c.id != b.id AND c.id != a.id")
# %%
# ## Plan B (sans GraphFrames) : questions 11 et 12 par auto-jointures
# Utile si l'installation de GraphFrames échoue ; donne les mêmes chemins.
# %%
e = tripEdges.select("src", "dst")

# 11) triangle a -> b -> c -> a
triangles = (e.alias("ab")
             .join(e.alias("bc"), F.col("ab.dst") == F.col("bc.src"))
             .join(e.alias("ca"), (F.col("bc.dst") == F.col("ca.src")) & (F.col("ca.dst") == F.col("ab.src")))
             .select(F.col("ab.src").alias("a"), F.col("ab.dst").alias("b"), F.col("bc.dst").alias("c")))

# 12) chemins a -> b -> c depuis 'Townsend at 7th'
chemins_b = (e.alias("ab").where("ab.src = 'Townsend at 7th'")
             .join(e.alias("bc"), F.col("ab.dst") == F.col("bc.src"))
             .select(F.col("ab.src").alias("a"), F.col("ab.dst").alias("b"), F.col("bc.dst").alias("c")))
print("triangles :", triangles.count(), "| chemins depuis Townsend at 7th :", chemins_b.count())
