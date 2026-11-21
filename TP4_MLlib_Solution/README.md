# Solution TP4 — Spark MLlib (KMeans)

## Contenu
- `tp4_mllib.ipynb` : notebook **Google Colab**, une cellule par question (1 à 15).
- `tp4_mllib.py` : même code en script (cellules séparées par `# %%`).
- `data/tp4_data_sample/` : 6 petits CSV **fictifs** au format « une journée par fichier » (pour tester).

## Utilisation sur Colab
1. Importer le notebook ; téléverser le dossier `tp4_data` dans `/content` (chemin utilisé : `/content/tp4_data/*.csv`).
2. Exécuter les cellules dans l'ordre.

## Vérifié / non vérifié
- **Vérifié :** exécution complète sous pyspark (mode local) sur les CSV fictifs : lecture, schéma, `na.fill(0)`, `day_of_week`, découpage train/test, pipeline (StringIndexer → OneHotEncoder → VectorAssembler), KMeans k=20, prédictions, silhouette (≈ 0,51 sur l'échantillon — valeur sans signification pour vos vraies données).
- **Non vérifié :** vos vrais fichiers `tp4_data` (non fournis) ; le notebook n'a pas été ouvert dans Colab.
- Supposé : colonnes `InvoiceDate` (timestamp, ex. `2010-12-01 08:26:00`), `UnitPrice`, `Quantity`, lues avec `header=true` et `inferSchema=true`. Si `InvoiceDate` est lu en texte (autre format de date), ajouter `F.to_timestamp("InvoiceDate", "<format>")` avant l'étape 4.
- Un message `java.io.FileNotFoundException ... *.csv` peut apparaître dans les logs avec le joker `*.csv` : sans conséquence si le DataFrame se charge.

## Points techniques
- `na.fill(0)` ne remplit que les colonnes numériques (ex. `CustomerID`) ; `Description` (texte) garde ses valeurs manquantes.
- `OneHotEncoder` supprime par défaut la dernière catégorie (`dropLast=True`) : 4 jours distincts → vecteur de taille 3.
- `handleInvalid="keep"` du StringIndexer évite une erreur si un jour du test est absent du train.
