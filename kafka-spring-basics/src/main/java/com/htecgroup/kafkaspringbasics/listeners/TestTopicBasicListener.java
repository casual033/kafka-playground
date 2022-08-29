package com.htecgroup.kafkaspringbasics.listeners;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TestTopicBasicListener {

  List<String> messages = new ArrayList<>();

  @KafkaListener(
      topics = {"${topicConfig.testTopic}"},
      groupId = "${spring.kafka.consumer.group-id}" + "-basic"
  )
  public void onMessage(String message) {
    log.info("Consumer new message: {}", message);
    messages.add(message);
  }

  public List<String> getReceivedMessages() {
    return messages;
  }
}
