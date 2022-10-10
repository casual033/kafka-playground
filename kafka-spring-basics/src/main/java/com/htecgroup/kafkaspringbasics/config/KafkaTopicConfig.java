package com.htecgroup.kafkaspringbasics.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

  private final String testTopic;
  private final Short replicationFactor;
  private final Integer partitionCount;

  public KafkaTopicConfig(
      @Value("${topicConfig.testTopic}") String testTopic,
      @Value("${topicConfig.partitionCount}") Integer partitionCount,
      @Value("${spring.kafka.replicationFactor}") Short replicationFactor
  ) {
    this.testTopic = testTopic;
    this.replicationFactor = replicationFactor;
    this.partitionCount = partitionCount;
  }

  @Bean
  public NewTopic dataIngestionTopic() {
    return new NewTopic(testTopic, 1, replicationFactor);
  }

  @Bean
  public NewTopic dataIngestionTransactionalTopic() {
    return new NewTopic(testTopic + "-transactional", 1, replicationFactor);
  }
}
