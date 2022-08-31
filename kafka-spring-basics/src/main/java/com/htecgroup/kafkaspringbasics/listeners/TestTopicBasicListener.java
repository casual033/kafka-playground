package com.htecgroup.kafkaspringbasics.listeners;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TestTopicBasicListener {

  Set<String> messages = new HashSet<>();

  @KafkaListener(
      topics = {"${topicConfig.testTopic}"},
      groupId = "${spring.kafka.consumer.group-id}" + "-basic"
  )
  public void onMessage(String message) {
    log.info("Consumer new message: {}", message);
    messages.add(message);
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      log.error("Can't sleep", e);
    }
  }

  public List<String> getReceivedMessages() {
    return new ArrayList<>(messages);
  }
}
