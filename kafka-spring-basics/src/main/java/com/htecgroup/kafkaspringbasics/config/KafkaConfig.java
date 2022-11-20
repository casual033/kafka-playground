package com.htecgroup.kafkaspringbasics.config;

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
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Configuration
public class KafkaConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.concurrency:1}")
  private Integer concurrency;

  @Value("${kafka.auto.offset.reset:latest}")
  private String autoOffsetSetting;

  @Bean(name="kafkaProducer")
  public KafkaProducer<String, String> getKafkaProducer() {

    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", getBootstrapServers());
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
    producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15000);
    producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    KafkaProducer<String, String> producer = new KafkaProducer<>(
        producerProperties);

    return producer;
  }

  @Bean(name = "customKafkaListenerContainerFactory")
  public ConcurrentKafkaListenerContainerFactory<String, String> customKafkaListenerContainerFactory() {

    ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory((ConsumerFactory<? super String, ? super String>) noAutoCommitConsumerFactory());
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.TIME);
    factory.getContainerProperties().setAckTime(2000);
    factory.setCommonErrorHandler(new DefaultErrorHandler((consumerRecord, e) -> {
      System.out.println(e);
      // send to DLQ for example
    }, new FixedBackOff(1000, 4)));

    return factory;
  }

  @Bean("noAutoCommitConsumerFactory")
  public ConsumerFactory<?, ?> noAutoCommitConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 3000);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetSetting);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  private String getBootstrapServers() {
    return bootstrapServers;
  }
}
