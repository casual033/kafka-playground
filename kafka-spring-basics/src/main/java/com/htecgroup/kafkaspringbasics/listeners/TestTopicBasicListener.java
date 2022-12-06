package com.htecgroup.kafkaspringbasics.listeners;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class TestTopicBasicListener {

  List<String> messages = new ArrayList<>();

  @SneakyThrows
  @KafkaListener(
      topics = {"${topicConfig.testTopic}"},
      groupId = "${spring.kafka.consumer.group-id}" + "-basic",
      containerFactory = "customKafkaListenerContainerFactory"
  )
  public void onMessage(String message) {

      log.info("Message received: {}", message);
      if(message.equals("msg3")) {
          Thread.sleep(5000);
      } else {
          Thread.sleep(1000);
      }
      messages.add(message);

  }

  public List<String> getReceivedMessages() {
    return messages;
  }
}
