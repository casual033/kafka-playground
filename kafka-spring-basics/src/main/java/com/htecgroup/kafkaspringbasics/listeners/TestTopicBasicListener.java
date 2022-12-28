package com.htecgroup.kafkaspringbasics.listeners;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TestTopicBasicListener {

  @KafkaListener(
      topics = {"${topicConfig.testTopic}"},
      groupId = "${spring.kafka.consumer.group-id}" + "-basic"
  )
  public void onMessage(String message, Acknowledgment acknowledgment) {
    log.info("Consumed new message: {}", message);
    acknowledgment.acknowledge();
  }
}
