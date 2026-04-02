package com.vasyl.practice.kafka.service;

public interface EventSender<T> {

    void sendEvent(T eventBody, String topic, Integer partition, String key);
}
