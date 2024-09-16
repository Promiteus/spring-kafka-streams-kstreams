### Пример работы основных функций Kafka Streams  

###Стеки:
Java 17, Spring Boot 3, Maven, Kafka Streams, Docker, Docker Compose.


### Запуск проекта

1. В корне проекта вызвать команду:

```
docker-compose up -d
```
Результат - сервисы zookeeper и broker kafka должны быть запущены без ошибок:
```
roman@roman-MS-7C83:~/IdeaProjects/kafka/kafka-streams-2$ docker-compose ps
  Name               Command            State                                         Ports                                       
----------------------------------------------------------------------------------------------------------------------------------
broker      /etc/confluent/docker/run   Up      0.0.0.0:9092->9092/tcp,:::9092->9092/tcp, 0.0.0.0:9101->9101/tcp,:::9101->9101/tcp
zookeeper   /etc/confluent/docker/run   Up      0.0.0.0:2181->2181/tcp,:::2181->2181/tcp, 2888/tcp, 3888/tcp   
```  

2. Собрать проект (вызвать в корне команду):
```
mvn clean package
```

3. Запустить проект:
```
mvn spring-boot:run
```

### Пример работы функция filter() и mapValues()  
