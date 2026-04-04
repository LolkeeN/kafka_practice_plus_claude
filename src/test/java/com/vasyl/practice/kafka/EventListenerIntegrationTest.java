package com.vasyl.practice.kafka;

import com.vasyl.practice.kafka.dto.User;
import com.vasyl.practice.kafka.listener.EventListener;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;

import static org.mockito.Mockito.after;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {"user-event", "another-user-event", "user-event.DLT"})
class EventListenerIntegrationTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockitoSpyBean
    EventListener eventListener;

    private User user(String username, double balance) {
        User u = new User();
        u.setUsername(username);
        u.setBalance(BigDecimal.valueOf(balance));
        return u;
    }

    private void postToTopic(String topic, User user) throws Exception {
        mockMvc.perform(post("/api/event/" + topic)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(user)))
                .andExpect(status().isOk());
    }

    @Test
    void handle_receivesEventFromUserEventTopic() throws Exception {
        User alice = user("alice", 5_000);

        postToTopic("user-event", alice);

        verify(eventListener, timeout(5_000)).handle(alice);
    }

    @Test
    void handle_receivesEventFromAnotherUserEventTopic() throws Exception {
        User bob = user("bob", 3_000);

        postToTopic("another-user-event", bob);

        verify(eventListener, timeout(5_000)).handle(bob);
    }

    @Test
    void handle2_receivesEventWhenBalanceMeetsThreshold() throws Exception {
        User charlie = user("charlie", 15_000);

        postToTopic("user-event", charlie);

        verify(eventListener, timeout(5_000)).handle2(charlie);
    }

    @Test
    void handle2_isNotCalledWhenBalanceBelowThreshold() throws Exception {
        User dave = user("dave", 500);

        postToTopic("user-event", dave);

        // Wait for handle() to confirm the message was consumed, then verify handle2 was skipped
        verify(eventListener, timeout(5_000)).handle(dave);
        verify(eventListener, after(2_000).never()).handle2(dave);
    }

    @Test
    void handle2_isCalledAndThrowsForErrorUsername() throws Exception {
        User errorUser = user("error-user", 20_000);

        postToTopic("user-event", errorUser);

        // handle2 should be invoked (and throw RuntimeException internally)
        verify(eventListener, timeout(5_000).atLeastOnce()).handle2(errorUser);
    }
}
