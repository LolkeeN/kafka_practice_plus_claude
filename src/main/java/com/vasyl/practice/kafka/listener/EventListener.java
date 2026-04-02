package com.vasyl.practice.kafka.listener;

import com.vasyl.practice.kafka.dto.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
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
            containerFactory = "filterKafkaListenerContainerFactory"
    )
    void handle(User user) {
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
    void handle2(User user) {
        log.info("Received event2: {}", user);

        if (user.getUsername().contains("error")) {
            throw new RuntimeException("ERROR!!!");
        }
    }
}
