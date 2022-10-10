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

  public static final String ERROR_MESSAGE = "ERROR";

  private final String topic;
  private final KafkaProducer<String, String> transactionalKafkaProducer;


  public TransactionalProducer(
      @Qualifier("transactionalKafkaProducer") final KafkaProducer<String, String> transactionalKafkaProducer,
      @Value("${topicConfig.testTopic}-transactional") final String topic
      ) {
    this.transactionalKafkaProducer = transactionalKafkaProducer;
    this.topic = topic;
  }

  public void send(List<String> messages) {
    try {
      log.info("Starting transaction for: {} messages", messages.size());

      transactionalKafkaProducer.beginTransaction();

      for (String message : messages) {
        if (message.equals(ERROR_MESSAGE)) {
          log.info("Aborting!!! Topic: {}, Error message received {}", topic, message);
          throw new RuntimeException("Invalid message");
        }
        log.info("Topic: {}, Sending message {}", topic, message);
        transactionalKafkaProducer.send(new ProducerRecord<>(topic, message));

        // if it happens too fast abort might be successful
        Thread.sleep(100);
      }
      transactionalKafkaProducer.commitTransaction();
    } catch (Exception e) {
      transactionalKafkaProducer.abortTransaction();
    }
  }
}
