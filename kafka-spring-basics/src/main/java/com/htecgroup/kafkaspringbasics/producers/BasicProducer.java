package com.htecgroup.kafkaspringbasics.producers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.RetryException;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BasicProducer {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  private final String topic;


  public BasicProducer(final KafkaTemplate<String, String> kafkaTemplate,
                      @Value("${topicConfig.testTopic}") final String topic){
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
  }

  public void send(String message) {
    try {
      log.info("Topic: {}, Sending message {}", topic, message);
      kafkaTemplate.send(topic, message);
    } catch (Exception e) {
      throw new RetryException(e.getMessage());
    }
  }
}
