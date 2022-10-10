package com.htecgroup.kafkaspringbasics.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

@Slf4j
@Configuration
public class KafkaConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${message-consumer.group-id:transactional-consumer}")
  private String groupId;

  @Value("${spring.kafka.concurrency:1}")
  private Integer concurrency;

  @Value("${kafka.auto.offset.reset:latest}")
  private String autoOffsetSetting;

  @Bean(name="transactionalKafkaProducer")
  public KafkaProducer<String, String> getKafkaProducer() {

    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", bootstrapServers);
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
    producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15000);

    producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-test-id");

    KafkaProducer<String, String> producer = new KafkaProducer<>(
        producerProperties);

    // very important - it doesn't work without this
    producer.initTransactions();

    return producer;
  }

  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    return new DefaultKafkaConsumerFactory<>(properties);
  }

  @Bean(name = "transactionalConsumerFactory")
  public ConcurrentKafkaListenerContainerFactory<String, String>
  kafkaListenerContainerFactory() {

    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());

    return factory;
  }

}
