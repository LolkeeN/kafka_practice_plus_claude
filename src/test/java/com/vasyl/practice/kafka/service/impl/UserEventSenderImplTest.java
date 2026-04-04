package com.vasyl.practice.kafka.service.impl;

import com.vasyl.practice.kafka.dto.User;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class UserEventSenderImplTest {

    @Mock
    KafkaTemplate<String, User> kafkaTemplate;

    @InjectMocks
    UserEventSenderImpl<User> sender;

    @Test
    void sendEvent_delegatesToKafkaTemplateWithAllParams() {
        User user = new User();
        user.setUsername("testuser");
        user.setBalance(BigDecimal.valueOf(20000));

        sender.sendEvent(user, "user-event", 0, "key1");

        verify(kafkaTemplate).send("user-event", 0, "key1", user);
    }

    @Test
    void sendEvent_withNullPartitionAndKey_passesNullsToTemplate() {
        User user = new User();
        user.setUsername("testuser");
        user.setBalance(BigDecimal.valueOf(500));

        sender.sendEvent(user, "user-event", null, null);

        verify(kafkaTemplate).send("user-event", null, null, user);
    }

    @Test
    void sendEvent_differentTopic_routesToCorrectTopic() {
        User user = new User();
        user.setUsername("alice");
        user.setBalance(BigDecimal.valueOf(3000));

        sender.sendEvent(user, "another-user-event", 1, null);

        verify(kafkaTemplate).send("another-user-event", 1, null, user);
    }
}
