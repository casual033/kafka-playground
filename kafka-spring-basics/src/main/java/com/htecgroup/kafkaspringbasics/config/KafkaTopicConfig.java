package com.htecgroup.kafkaspringbasics.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

  private final String testTopic;
  private final Short replicationFactor;

  public KafkaTopicConfig(
      @Value("${topicConfig.testTopic}") String testTopic,
      @Value("${spring.kafka.replicationFactor}") Short replicationFactor
  ) {
    this.testTopic = testTopic;
    this.replicationFactor = replicationFactor;
  }

  @Bean
  public NewTopic newTopic() {
    return new NewTopic(testTopic, 1, replicationFactor);
  }

}
