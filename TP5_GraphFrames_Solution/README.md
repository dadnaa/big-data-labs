# Solution TP5 — GraphFrames (stations et trajets de vélos)

## Contenu
- `tp5_graphframes.ipynb` : notebook **Google Colab** (une cellule par question 1 à 12 + Plan B).
- `tp5_graphframes.py` : même code en script (cellules séparées par `# %%`).
- `data/station_data.csv`, `data/trip_data.csv` : données **fictives** (8 stations, 34 voyages) pour tester.

## Utilisation sur Colab
1. Téléverser `station_data.csv` et `trip_data.csv` dans `/content`.
2. Exécuter les cellules dans l'ordre. La cellule 1 installe `pyspark==3.5.3` et `graphframes-py` ; la cellule 2 charge le package Java `graphframes:graphframes:0.8.4-spark3.5-s_2.12` (téléchargé au démarrage de la session Spark → connexion Internet nécessaire). Si Colab demande un redémarrage, le faire puis reprendre à la cellule 2.

## Vérifié / non vérifié
- **Vérifié** (pyspark en local, données fictives, avec une *doublure* de la classe GraphFrame qui reproduit `.vertices`, `.edges`, `.inDegrees`) : questions 1 à 10, et le **Plan B** (auto-jointures) des questions 11-12 — ses comptages (216 triangles, 54 chemins sur l'échantillon) correspondent à un calcul Python indépendant.
- **NON vérifié :** la vraie bibliothèque GraphFrames (le package Java n'est pas téléchargeable depuis mon environnement) : l'installation, `GraphFrame(...)` et les appels `find(...)` des questions 11 et 12 sont écrits d'après le cours (syntaxe des slides) mais n'ont pas été exécutés. Les vrais fichiers de données ne m'ont pas été fournis non plus.
- Versions : le coordonnée `0.8.4-spark3.5-s_2.12` provient de la documentation GraphFrames consultée ; elle doit correspondre à la version de Spark (3.5 ici). Si votre Colab a une autre version de Spark, alignez les deux.

## Points importants
- `Duration` doit être lue comme **nombre** (`inferSchema`), sinon la question 9 trie les durées alphabétiquement. (Les slides lisent sans `inferSchema`.)
- Une ligne de `find` par **combinaison d'arcs** : s'il y a plusieurs voyages identiques entre deux stations, les chemins/triangles sont comptés plusieurs fois (comme dans le Plan B).
- Question 12 : j'ai pris « trois sommets » = chemin a→b→c (2 arcs) sans exiger des sommets distincts ; la variante avec sommets distincts est en commentaire.
