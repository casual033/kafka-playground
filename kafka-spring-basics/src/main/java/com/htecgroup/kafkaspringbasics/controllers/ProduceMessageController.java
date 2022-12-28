package com.htecgroup.kafkaspringbasics.controllers;

import com.htecgroup.kafkaspringbasics.producers.BasicProducer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = "/producer")
@Slf4j
@AllArgsConstructor
public class ProduceMessageController {

  private final BasicProducer basicProducer;

  @PostMapping(value = "/message", consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity produceMessage(@RequestBody String message) {
    basicProducer.send(message);
    return ResponseEntity.ok().build();
  }
}
