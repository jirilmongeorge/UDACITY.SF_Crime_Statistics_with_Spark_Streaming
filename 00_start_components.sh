
nohup /usr/bin/zookeeper-server-start config/zookeeper.properties > startup_zK.log &
nohup /usr/bin/kafka-server-start config/server.properties > startup_Kfk.log &
sh start.sh
