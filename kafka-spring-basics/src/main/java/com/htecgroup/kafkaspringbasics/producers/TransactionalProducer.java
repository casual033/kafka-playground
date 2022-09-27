package com.htecgroup.kafkaspringbasics.producers;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TransactionalProducer {

  private final String topic;
  private final KafkaProducer<String, String> transactionalKafkaProducer;


  public TransactionalProducer(
      @Qualifier("transactionalKafkaProducer") final KafkaProducer<String, String> transactionalKafkaProducer,
      @Value("${topicConfig.testTopic}") final String topic
      ) {
    this.transactionalKafkaProducer = transactionalKafkaProducer;
    this.topic = topic;
  }

  public void send(List<String> messages) {
    try {
      log.info("Starting transaction for: {} messages", messages.size());

      transactionalKafkaProducer.beginTransaction();

      for (String message : messages) {
        log.info("Topic: {}, Sending message {}", topic, message);
        transactionalKafkaProducer.send(new ProducerRecord<>(topic, message));
      }
    } catch (Exception e) {
      transactionalKafkaProducer.abortTransaction();
    }
  }
}
