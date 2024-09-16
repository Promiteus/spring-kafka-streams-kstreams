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

### Преобразование строки с меткой "important" в верхний регистр. Пример работы функций filter() и mapValues().  
* Топики "input-data" (входящие данные), "output-data" (данные после преобразования)
* В файле application.yml выставить профиль spring.profile.active=string-value  
* Класс топологии: ***KafkaStreamsStringTransformTranslation.java***    
* Запустить проект:  
```
mvn spring-boot:run
```
* Результат в консоле приложения:  
```
[input-data]: 9e22d603-fa7f-48a4-86e4-7225ac65d525, Hello, Kafka! important
[output-data]: 9e22d603-fa7f-48a4-86e4-7225ac65d525, HELLO, KAFKA! IMPORTANT
Message sent successfully. Offset: 1
[input-data]: 772f00b3-1461-4026-9888-9a6d298dfd31, Hello, Kafka! important
[output-data]: 772f00b3-1461-4026-9888-9a6d298dfd31, HELLO, KAFKA! IMPORTANT
Message sent successfully. Offset: 2
[input-data]: b779c049-5c68-4c7a-a034-91fde25d8ef4, Hello, Kafka! important
[output-data]: b779c049-5c68-4c7a-a034-91fde25d8ef4, HELLO, KAFKA! IMPORTANT
Message sent successfully. Offset: 3
```  

### Добавление ключа в сообщения потока. Пример работы функции selectKey().
* Топики "input-data" (входящие данные), "output-data-with-key" (данные после преобразования)
* В файле application.yml выставить профиль spring.profile.active=json-select-key-value
* Класс топологии: ***KafkaSteamsPurchaseSelectKeyTranslation.java***
* Запустить проект:
```
mvn spring-boot:run
```
* Результат в консоле приложения:
```
Message sent successfully. Offset: 29
[input-data]: null, Purchase(id=ba649c77-2bc7-426b-8618-b2b25cd9c82e, name=cola, price=100.0, currency=RUR, timestamp=1726458268937)
[output-data-with-key]: ba649c77-2bc7-426b-8618-b2b25cd9c82e, Purchase(id=ba649c77-2bc7-426b-8618-b2b25cd9c82e, name=COLA, price=100.0, currency=null, timestamp=1726458268937)
Message sent successfully. Offset: 30
[input-data]: null, Purchase(id=7aae5c4f-8142-4968-af7f-5a28bd2a4a7d, name=cola, price=100.0, currency=RUR, timestamp=1726458270939)
[output-data-with-key]: 7aae5c4f-8142-4968-af7f-5a28bd2a4a7d, Purchase(id=7aae5c4f-8142-4968-af7f-5a28bd2a4a7d, name=COLA, price=100.0, currency=null, timestamp=1726458270939)
Message sent successfully. Offset: 31
[input-data]: null, Purchase(id=516a66c1-6f3e-405f-9983-6ec6b748f212, name=cola, price=100.0, currency=RUR, timestamp=1726458272940)
[output-data-with-key]: 516a66c1-6f3e-405f-9983-6ec6b748f212, Purchase(id=516a66c1-6f3e-405f-9983-6ec6b748f212, name=COLA, price=100.0, currency=null, timestamp=1726458272940)

```