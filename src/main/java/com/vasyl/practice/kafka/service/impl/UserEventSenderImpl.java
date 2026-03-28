package com.vasyl.practice.kafka.service.impl;

import com.vasyl.practice.kafka.service.EventSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserEventSenderImpl<User> implements EventSender<User> {

    @Autowired
    KafkaTemplate<String, User> kafkaTemplate;

    @Override
    public void sendEvent(User eventBody, String topic, int partition, String key) {
        kafkaTemplate.send(topic, partition, key, eventBody);
    }
}
