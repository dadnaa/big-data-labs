# Solution TP3 — Spark SQL (N-Gram)

## Contenu
- `tp3_sparksql.ipynb` : notebook pour **Google Colab** (une cellule par question, SQL puis API DataFrame).
- `tp3_sparksql.py` : le même code en script (cellules séparées par `# %%`).
- `data/ngram_sample.csv` : échantillon **fictif** (73 lignes, tabulations, sans en-tête) pour tester.

## Utilisation sur Colab
1. Importer `tp3_sparksql.ipynb` (Fichier → Importer) et téléverser votre `ngram.csv` dans `/content`.
2. Exécuter les cellules dans l'ordre (la 1re installe pyspark).
3. La dernière cellule compare SQL et API : chaque question doit afficher `OK`.

## Vérifié / non vérifié
- **Vérifié :** le script a tourné sous pyspark (mode local) sur l'échantillon fictif, dans deux formats (tabulation sans en-tête ; virgule avec en-tête). Les 8 questions donnent le même résultat en SQL et en API.
- **Non vérifié :** votre vrai `ngram.csv` (non fourni) ; le notebook `.ipynb` lui-même n'a pas été ouvert dans Colab (il est généré depuis le script testé).
- Séparateur (`\t` ou `,`) et en-tête sont détectés d'après la 1re ligne. Si le fichier a une autre forme, fixer `SEP` et `HEADER` à la main.

## Choix d'interprétation
- **3.2** « nombre total de bi-grammes par année » = `COUNT(*)` (nombre de lignes). Si l'énoncé veut le nombre d'occurrences : `SUM(Count)`.
- **3.3** les ex æquo sont conservés.
- **3.4** « 20 années différentes » = exactement 20 (`COUNT(DISTINCT Year) = 20`).
- **3.5** `split(ngram,' ')[0]` contient `!` et `[1]` contient `9`.
- **3.7** somme de `Pages` et `Books` par couple (ngram, Year), trié par ngram puis Year.
