docker exec -it kafka kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic datasource-topic
docker exec -it kafka kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic flink-query1-weekly
docker exec -it kafka kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic flink-query1-monthly
docker exec -it kafka kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic flink-query2-weekly
docker exec -it kafka kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic flink-query2-monthly