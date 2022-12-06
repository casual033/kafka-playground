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
@EmbeddedKafka(partitions = 1, topics = "${topicConfig.testTopic}", brokerProperties = {})
class TestTopicBasicListenerTest {

  @Autowired
  BasicProducer producer;

  @Autowired
  TestTopicBasicListener consumer;

  @Test
  public void givenEmbeddedKafkaBroker_whenSendingWithProducer_thenMessageReceivedManualImmediate() throws InterruptedException {

    producer.send("msg1");
    producer.send("msg2");

    Thread.sleep(30000);
    List<String> messages = consumer.getReceivedMessages();

    Assertions.assertEquals(2, messages.size());
    long countMsg1 = messages.stream().filter(msg -> msg.equals("msg1")).count();
    long countMsg2 = messages.stream().filter(msg -> msg.equals("msg2")).count();

    Assertions.assertEquals(1, countMsg1);
    Assertions.assertEquals(1, countMsg2);
  }
}