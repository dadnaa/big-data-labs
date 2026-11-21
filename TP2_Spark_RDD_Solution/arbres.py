
import sys
from pyspark import SparkConf, SparkContext

chemin = sys.argv[1] if len(sys.argv) > 1 else "file:///root/arbres.csv"

config = SparkConf().setAppName("TP2_Arbres")
sc = SparkContext(conf=config)
sc.setLogLevel("ERROR")

# ---------- (1) Creer le RDD et afficher le nombre de lignes ----------
lignes = sc.textFile(chemin)
print("(1) Nombre de lignes (en-tete inclus) : {}".format(lignes.count()))

entete = lignes.first()
sep = ";" if ";" in entete else ","             # separateur detecte depuis l'en-tete
colonnes = [c.strip().upper() for c in entete.split(sep)]
iGenre = colonnes.index("GENRE")                # indices trouves par NOM de colonne
iHaut = colonnes.index("HAUTEURENM")            
donnees = lignes.filter(lambda l: l != entete).map(lambda l: l.split(sep))
print("    Nombre d'arbres (sans en-tete)    : {}".format(donnees.count()))


def hauteur_genre(champs):
    """(hauteur, genre) ou None si la ligne est incomplete / hauteur non numerique."""
    try:
        return (float(champs[iHaut].replace(",", ".")), champs[iGenre])
    except (ValueError, IndexError):
        return None


# paires (cle = hauteur, valeur = genre)
paires = donnees.map(hauteur_genre).filter(lambda p: p is not None)

# ---------- (2) Hauteur moyenne ----------
hauteurs = paires.map(lambda p: p[0])
moyenne = hauteurs.reduce(lambda a, b: a + b) / hauteurs.count()
print("(2) Hauteur moyenne des arbres : {:.2f} m".format(moyenne))

# ---------- (3) Genre du plus grand arbre ----------
plus_grand = paires.sortByKey(False).first()    # False = ordre decroissant des cles
print("(3) Genre du plus grand arbre : {} (hauteur = {})".format(plus_grand[1], plus_grand[0]))

# ---------- (4) Nombre d'arbres par genre ----------
par_genre = (donnees
             .filter(lambda c: len(c) > iGenre)
             .map(lambda c: (c[iGenre], 1))
             .reduceByKey(lambda a, b: a + b))
print("(4) Nombre d'arbres par genre :")
for genre, nb in par_genre.sortBy(lambda x: -x[1]).collect():
    print("    {} : {}".format(genre, nb))

sc.stop()
