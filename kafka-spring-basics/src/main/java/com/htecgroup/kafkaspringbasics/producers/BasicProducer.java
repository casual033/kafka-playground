package com.htecgroup.kafkaspringbasics.producers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.retry.RetryException;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BasicProducer {


  private final KafkaOperations<String, String> kafkaOperations;
  private final String topic;


  public BasicProducer(final KafkaOperations<String, String> kafkaOperations,
      @Value("${topicConfig.testTopic}") final String topic
      ) {
    this.kafkaOperations = kafkaOperations;
    this.topic = topic;
  }

  public void send(String messageKey, String messageValue) {
    try {
      log.info("Topic: {}, Sending message {}", topic, messageValue);
      kafkaOperations.send(topic, messageKey, messageValue);
    } catch (Exception e) {
      throw new RetryException(e.getMessage());
    }

    kafkaOperations.flush();
  }
}
