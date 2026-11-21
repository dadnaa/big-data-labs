
docker network create --driver=bridge spark

docker run -itd --net=spark -p 8080:8080 -p 7077:7077 --name spark-master --hostname spark-master liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd --net=spark --name spark-slave1 --hostname spark-slave1 liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd --net=spark --name spark-slave2 --hostname spark-slave2 liliasfaxi/spark-hadoop:hv-2.7.2
docker ps       
docker cp arbres.py  spark-master:/root/arbres.py
docker cp arbres.csv spark-master:/root/arbres.csv
docker cp arbres.csv spark-slave1:/root/arbres.csv
docker cp arbres.csv spark-slave2:/root/arbres.csv

docker exec -it spark-master bash
