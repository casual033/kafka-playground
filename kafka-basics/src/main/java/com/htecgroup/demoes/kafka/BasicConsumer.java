package com.htecgroup.demoes.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BasicConsumer {

  static Logger logger = Logger.getLogger("BasicConsumer");

  public static void main(String[] args) {



    String groupId = "testGroup3";

    // create props
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    // subscribe consumer
    consumer.subscribe(Collections.singletonList("demo_java"));

    while(true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

      for (ConsumerRecord<String, String> record : records) {
        logger.info(String.format("Topic: %s, Partition: %s, Offset: %s, New record: %s ",
            record.topic(), record.partition(), record.offset(), record.value()));
      }
    }

  }

}
