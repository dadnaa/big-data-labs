
docker pull liliasfaxi/spark-hadoop:hv-2.7.2

docker network create --driver=bridge hadoop

docker run -itd --net=hadoop -p 50070:50070 -p 8088:8088 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd --net=hadoop -p 8040:8042 --name hadoop-slave1 --hostname hadoop-slave1 liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd --net=hadoop -p 8041:8042 --name hadoop-slave2 --hostname hadoop-slave2 liliasfaxi/spark-hadoop:hv-2.7.2

docker ps

docker exec -it hadoop-master bash
