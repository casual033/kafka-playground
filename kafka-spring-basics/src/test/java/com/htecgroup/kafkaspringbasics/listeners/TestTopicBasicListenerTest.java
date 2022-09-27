package com.htecgroup.kafkaspringbasics.listeners;

import com.htecgroup.kafkaspringbasics.producers.BasicProducer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@Slf4j
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(count = 3)
class TestTopicBasicListenerTest {

  @Autowired
  BasicProducer producer;

  @Autowired
  TestTopicBasicListener consumer;

  @Test
  public void givenEmbeddedKafkaBroker_whenSendingWithSimpleProducer_thenMessageReceived() throws InterruptedException {

    String testMessage = "test message ";

    int messageCount = 0;
    for (int i = 0; i < 10; i++) {
      messageCount++;
      producer.send(testMessage + messageCount);
    }

    Thread.sleep(10000);
    List<String> messages = consumer.getReceivedMessages();

    log.info("Received total of: {} messages", messages.size());

    Map<String, Long> messagesCount = messages.stream()
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    messagesCount.forEach((message,count) -> {
      log.info("Received total of: {} times, message: {} ", count, message);
    } );

    Assertions.assertEquals(10, messages.size());
    Assertions.assertTrue(messages.contains(testMessage + 1));
    Assertions.assertTrue(messages.contains(testMessage + 4));
    Assertions.assertTrue(messages.contains(testMessage + 9));

  }

}