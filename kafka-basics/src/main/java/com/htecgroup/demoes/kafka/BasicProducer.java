package com.htecgroup.demoes.kafka;

import java.time.LocalDateTime;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class BasicProducer {

  public static void main(String[] args) {

    // create props
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    // create a producerRecord
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world - " + LocalDateTime.now());

    // send data - async
    producer.send(producerRecord);

    // flush and close - sync
    producer.flush();

    // also does flush
    producer.close();
  }

}
