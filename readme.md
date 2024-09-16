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
* Класс топологии: ***KafkaStreamsStringTransformTopology.java***    
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
* Класс топологии: ***KafkaSteamsPurchaseSelectKeyTopology.java***
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

### Разветвление потоков. Пример работы функции branch(), разделение потоков по цене.
* Топики "input-data" (входящие данные), "output-above-50-data" (данные после преобразования - цена больше 50), "output-less-50-data" (данные после преобразования - цена меньше 50)
* В файле application.yml выставить профиль spring.profile.active=json-branch-value
* Класс топологии: ***KafkaStreamsPurchaseBranchTopology.java***
* Запустить проект:
```
mvn spring-boot:run
```
* Результат в консоле приложения:  
```
[input-data]: null, Purchase(id=a39e4f8d-bcbe-423e-87cf-edf0b30d84e2, name=cola, price=89.13753562593423, currency=RUR, timestamp=1726459805159)
[output-above-50-data]: null, Purchase(id=a39e4f8d-bcbe-423e-87cf-edf0b30d84e2, name=cola, price=89.13753562593423, currency=RUR, timestamp=1726459805159)
Message sent successfully. Offset: 25
[input-data]: null, Purchase(id=7425aaef-ca89-49c8-94d8-1be7acbc9ac6, name=cola, price=126.2010466125455, currency=RUR, timestamp=1726459807160)
[output-above-50-data]: null, Purchase(id=7425aaef-ca89-49c8-94d8-1be7acbc9ac6, name=cola, price=126.2010466125455, currency=RUR, timestamp=1726459807160)
Message sent successfully. Offset: 26
[input-data]: null, Purchase(id=7a36cd32-7a59-49f0-ac30-823a7582bdfa, name=cola, price=42.7332938180981, currency=RUR, timestamp=1726459809161)
[output-less-50-data]: null, Purchase(id=7a36cd32-7a59-49f0-ac30-823a7582bdfa, name=cola, price=42.7332938180981, currency=RUR, timestamp=1726459809161)

```

### Объединение потоков. Пример работы функции join() и функции временного окна JoinWindows.of().
* В топики input-topic-join-1 и input-topic-join-2 с интервалом в 1 сек. (можете поэкспериментировать) подаются тестовые сообщения с ***одинаковыми ключами***. Эти сообщения должны обединиться в третий выходной поток "joined-data" только если временной интервал между сообщениями не больше интервала окна в функции ***JoinWindows.of(Duration.ofMillis(1500)***. В просессе экспериметов можно менять цифры в toTopic() и функции JoinWindows.of(Duration.ofMillis(1500)) топологии KafkaStreamsPurchaseJoinTranslation.java. 
  По поточно Purchase 1 и Purchase 2 объединяются в CorrelatePurchase.
* В файле application.yml выставить профиль spring.profile.active=json-join-values
* Класс топологии: ***KafkaStreamsPurchaseJoinTopology.java***
* Запустить проект:
```
mvn spring-boot:run
```
* Результат в консоле приложения: 
```
Message sent successfully. Offset: 113
[joined-data]: purchase, CorrelatePurchase(firstPurchaseDateTime=1726460808255, secondPurchaseDateTime=1726460807254, totalPrice=135746.80024917756, purchases=[Purchase(id=aded815d-0783-4cf9-ac0b-add9186d0494, name=cola, price=138.073235152939, currency=RUR, timestamp=1726460808255), Purchase(id=95891918-9ecc-4bc1-9efc-8642b1685f8a, name=Smartphone, price=135608.72701402463, currency=RUR, timestamp=1726460807254)])
Message sent successfully. Offset: 113
[joined-data]: purchase, CorrelatePurchase(firstPurchaseDateTime=1726460808255, secondPurchaseDateTime=1726460809256, totalPrice=169220.59064452088, purchases=[Purchase(id=aded815d-0783-4cf9-ac0b-add9186d0494, name=cola, price=138.073235152939, currency=RUR, timestamp=1726460808255), Purchase(id=57331195-fa09-48da-9cc2-15ae829c2ffd, name=Smartphone, price=169082.51740936795, currency=RUR, timestamp=1726460809256)])

```
