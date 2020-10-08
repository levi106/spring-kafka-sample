package com.example.kafka.controller;

import com.example.kafka.service.Producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("send")
public class KafkaController {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaController.class);
    @Autowired
    private Producer producer;
    
    @RequestMapping(value="/{count}", method=RequestMethod.GET)
    public void get(@PathVariable() String count) {
        int c = Integer.parseInt(count);
        for (int i = 0; i < c; i++) {
            producer.sendMessage();
        }
        LOG.info("{} messages were sent", c);
    }
}
