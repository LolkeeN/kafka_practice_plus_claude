package com.vasyl.practice.kafka.controller;

import com.vasyl.practice.kafka.dto.User;
import com.vasyl.practice.kafka.service.EventSender;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/event")
@RequiredArgsConstructor
public class EventController {

    @Autowired
    private final EventSender<User> eventSender;

    @PostMapping("/{topic}")
    void sendEvent(@RequestBody User user, @PathVariable String topic, @RequestParam Integer partition, @RequestParam String key) {
        eventSender.sendEvent(user, topic, partition, key);
        log.info("sent event: {}", user);
    }
}
