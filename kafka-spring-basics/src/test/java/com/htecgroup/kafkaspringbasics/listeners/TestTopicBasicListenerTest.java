package com.htecgroup.kafkaspringbasics.listeners;

import com.htecgroup.kafkaspringbasics.producers.BasicProducer;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9095", "port=9095" })
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

    Assertions.assertEquals(10, messages.size());
    Assertions.assertEquals(testMessage + 1, messages.get(0));
    Assertions.assertEquals(testMessage + 5, messages.get(4));
    Assertions.assertEquals(testMessage + 10, messages.get(9));

  }

}