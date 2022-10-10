package com.htecgroup.kafkaspringbasics.listeners;

import com.htecgroup.kafkaspringbasics.producers.TransactionalProducer;
import java.util.ArrayList;
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
class TestTopicTransactionalListenerTest {

  @Autowired
  TransactionalProducer producer;

  @Autowired
  TestTopicTransactionalListener consumer;

  @Test
  public void givenEmbeddedKafkaBroker_whenSendingWithTransactionalProducer_thenMessagesReceived() throws InterruptedException {

    String testMessage = "test message ";
    List<String> messagesToSend = new ArrayList<>();

    int messageCount = 0;
    for (int i = 0; i < 10; i++) {
      messageCount++;
      messagesToSend.add(testMessage + messageCount);
    }

    // send all messages or none
    producer.send(messagesToSend);

    Thread.sleep(10000);
    List<String> receivedMessages = consumer.getReceivedMessages();

    log.info("Received total of: {} messages", receivedMessages.size());

    Map<String, Long> messagesCount = receivedMessages.stream()
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    messagesCount.forEach((message,count) -> {
      log.info("Received total of: {} times, message: {} ", count, message);
    } );

    Assertions.assertEquals(10, receivedMessages.size());
    Assertions.assertTrue(receivedMessages.contains(testMessage + 1));
    Assertions.assertTrue(receivedMessages.contains(testMessage + 4));
    Assertions.assertTrue(receivedMessages.contains(testMessage + 9));

  }

  @Test
  public void givenEmbeddedKafkaBroker_whenSendingErrorMessageWithTransactionalProducer_thenNoMessagesReceived() throws InterruptedException {

    String testMessage = "test message ";
    List<String> messagesToSend = new ArrayList<>();

    // random position to put the error message on
    int randomInt = (int) (Math.random() * (9 - 4) + 4);

    int messageCount = 0;
    for (int i = 0; i < 10; i++) {
      messageCount++;
      if (i == randomInt) {
        messagesToSend.add(TransactionalProducer.ERROR_MESSAGE);
      } else {
        messagesToSend.add(testMessage + messageCount);
      }
    }

    // send all messages or none
    producer.send(messagesToSend);

    Thread.sleep(10000);
    List<String> receivedMessages = consumer.getReceivedMessages();

    log.info("Received total of: {} messages", receivedMessages.size());

    Map<String, Long> messagesCount = receivedMessages.stream()
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    messagesCount.forEach((message,count) -> {
      log.info("Received total of: {} times, message: {} ", count, message);
    } );

    Assertions.assertEquals(0, receivedMessages.size());
  }

}