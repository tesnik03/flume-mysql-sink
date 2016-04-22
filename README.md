# flume-mysql-sink
get kafka events in mysql

This project DOESN'T make any useful but sometimes we all need hacks. This is one of similar hack. 
Currently there is no quick way to find how many message you have received in kafka queue? what were the message those came between time A and time B. If you want to do a quick peek of all these, you have to all way to HDFS and have some database like impala or hive to that.I personally recommend to avoid this situation. 
Considering if volumne of your kafka messages are small, you can do this by putting messages in mysql. This project uses MySql as the sink.

##Prerequisites 
1. Create database 
  * 
    ```
      create database message_container;
    ```
2. Create table in mysql using -
  
    ```
            create table messages(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
                          message varchar(1000), 
                          updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                          created_at DATETIME DEFAULT NULL)
                          
    ```
3. Flume config file

  ```
    flume3.sources  = kafka-source-3
    flume3.channels = kafka-channel-3
    flume3.sinks    = mysql-sink-3

    flume3.sources.kafka-source-3.type = org.apache.flume.source.kafka.KafkaSource
    flume3.sources.kafka-source-3.zookeeperConnect = localhost:2181
    flume3.sources.kafka-source-3.topic = <<Kafka topic>>
    flume3.sources.kafka-source-3.batchSize = 1
    flume3.sources.kafka-source-3.channels = kafka-channel-3
    
    flume3.channels.kafka-channel-3.type   = memory
    flume3.sinks.mysql-sink-3.channel = kafka-channel-3
    flume3.sinks.mysql-sink-3.type = com.tesnik.flume.sink.MySqlSink
    flume3.sinks.mysql-sink-3.url = <<URL of the server>>
    flume3.sinks.mysql-sink-3.password = <<User name >>
    flume3.sinks.mysql-sink-3.user = << password >>
    flume3.sinks.mysql-sink-3.driver = <<Driver class>>

    flume3.channels.kafka-channel-3.capacity = 10000
    flume3.channels.kafka-channel-3.transactionCapacity = 1000
  ```

