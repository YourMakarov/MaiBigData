# BigDataFlink

Макаров Глеб Александрович М8О-209СВ-24

Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

Запуск:
1) docker-compose build  
2) docker-compose up -d zookeeper kafka postgres  
3) docker-compose exec kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic raw-mock  
4) docker-compose up -d kafka-producer  
5) docker-compose up -d jobmanager taskmanager  
6) docker-compose up -d flink-job  
