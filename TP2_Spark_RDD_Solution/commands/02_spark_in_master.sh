
cat > /usr/local/spark/conf/slaves <<'EOT'
spark-slave1
spark-slave2
EOT
cat /usr/local/spark/conf/slaves

/usr/local/spark/sbin/start-all.sh
jps                    

spark-submit --master spark://spark-master:7077 /root/arbres.py
