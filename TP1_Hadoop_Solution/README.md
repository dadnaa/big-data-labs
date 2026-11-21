# Solution TP1 — Hadoop (cluster Docker + HDFS + WordCount)

Module Big Data — ESI-SBA. Solution complete du TP1 : projet Maven pret a compiler + scripts de commandes.

## Contenu
```
TP1_Hadoop_Solution/
├── README.md
├── commands/
│   ├── 01_docker_setup.sh        # hote : image, reseau, 3 conteneurs, entree dans le master
│   ├── 02_hdfs_in_master.sh      # master : start-hadoop.sh + mkdir/put/ls/tail
│   └── 03_build_and_run.sh       # build du jar, copie, lancement du job, affichage du resultat
└── wordcount/                    # projet Maven (groupId hadoop.mapreduce, artifactId wordcount)
    ├── pom.xml
    └── src/main/java/tp1/
        ├── TokenizerMapper.java  # classe MAP
        ├── IntSumReducer.java    # classe REDUCE
        └── WordCount.java        # classe Driver
```

## Ordre d'execution
1. `commands/01_docker_setup.sh` (hote) → vous etes dans `root@hadoop-master:~#`
2. `commands/02_hdfs_in_master.sh` (dans le master)
3. `commands/03_build_and_run.sh` : partie « hote » dans `wordcount/`, partie « master » ensuite.

Les fichiers `.sh` sont des **listes de commandes a copier une par une** (surtout sous Windows), pas des scripts a lancer d'un bloc : l'etape `docker exec` ouvre un shell interactif.

## Points d'attention
- **Version Java :** verifier `docker exec -it hadoop-master java -version` et aligner `maven.compiler.source/target` dans `pom.xml` (1.8 par defaut ; mettre 1.7 si le conteneur est en Java 7).
- **Repertoire de sortie :** `output` ne doit pas exister, sinon `FileAlreadyExistsException` → `hadoop fs -rm -r output`.
- **Nom de classe :** `tp1.WordCount` (package + classe).
- **purchases.txt :** deja dans le repertoire principal du master (selon l'enonce). Sinon `purchases.txt.gz` : `gunzip purchases.txt.gz`.
- **Interfaces web :** NameNode `http://localhost:50070`, YARN `http://localhost:8088`.
- Sans Maven : IntelliJ → Project Structure → Artifacts → JAR, puis copier ce jar a la place de `target/wordcount.jar`.

## Resultat attendu
`hadoop fs -cat output/part-r-00000` affiche des lignes `mot<TAB>nombre` (donnees separees par tabulations : date, heure, magasin, article, cout, paiement).

## Mapping avec le cours (Ch1)
Driver = `Configuration`, `GenericOptionsParser`, `Job`, `setJarByClass/MapperClass/ReducerClass`, `setOutputKeyClass/ValueClass`, `FileInputFormat/FileOutputFormat`, `waitForCompletion(true)`.
Map : `Mapper<Object, Text, Text, IntWritable>`. Reduce : `Reducer<Text, IntWritable, Text, IntWritable>`.
