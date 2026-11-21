

./start-hadoop.sh
jps                                   

hadoop fs -mkdir -p input            
ls                                   
hadoop fs -put purchases.txt input    
hadoop fs -ls input                   
hadoop fs -tail input/purchases.txt   
