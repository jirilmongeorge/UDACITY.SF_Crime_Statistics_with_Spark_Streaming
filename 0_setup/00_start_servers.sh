
# nohup /usr/bin/zookeeper-server-start ../config/zookeeper.properties > startup_zK.log &
# nohup /usr/bin/kafka-server-start ../config/server.properties > startup_Kfk.log &
# sh ../start.sh

nohup systemctl start confluent-zookeeper > startup_zk.log &
nohup systemctl start confluent-kafka > startup_kfk.log &
