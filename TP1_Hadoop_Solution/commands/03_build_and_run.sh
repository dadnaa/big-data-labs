
mvn clean package                                            
docker cp target/wordcount.jar hadoop-master:/root/wordcount.jar


hadoop fs -rm -r output 2>/dev/null
hadoop jar wordcount.jar tp1.WordCount input output            
hadoop fs -ls output                                           
hadoop fs -cat output/part-r-00000 | head -20                
