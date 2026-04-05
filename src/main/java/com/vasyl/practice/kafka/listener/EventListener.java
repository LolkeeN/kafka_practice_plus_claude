package com.vasyl.practice.kafka.listener;

import com.vasyl.practice.kafka.dto.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EventListener {

    @KafkaListener(
//            topicPartitions = @TopicPartition(
//            topic = "user-event",
//            partitions = {"0", "1", "2"}
//    )
            topics = {"user-event", "another-user-event"},
            groupId = "fanout",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handle(User user) {
        log.info("Received event: {}", user);
    }

    @KafkaListener(
//            topicPartitions = @TopicPartition(
//            topic = "user-event",
//            partitions = {"0", "1", "2"}
//    )
            topics = {"user-event"},
            groupId = "fanout2",
            containerFactory = "filterKafkaListenerContainerFactory"
    )
    public void handle2(User user) {
        log.info("Received event2: {}", user);

        if (user.getUsername().contains("error")) {
            RuntimeException ex = new RuntimeException("ERROR!!!");
            log.error("Exception occurred in handle2() for user '{}': {}", user.getUsername(), ex.getMessage(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = {"user-event.DLT"},
            groupId = "fanout2",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleDlt(User user) {
        log.warn("DLT received message: {}", user);
    }
}
