package com.vasyl.practice.kafka.controller;

import com.vasyl.practice.kafka.dto.User;
import com.vasyl.practice.kafka.service.EventSender;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;

import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventController.class)
class EventControllerTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockitoBean
    EventSender<User> eventSender;

    @Test
    void sendEvent_withAllParams_returns200AndDelegatesToSender() throws Exception {
        User user = new User();
        user.setUsername("testuser");
        user.setRealName("Test User");
        user.setAge(30);
        user.setBalance(BigDecimal.valueOf(15000));

        mockMvc.perform(post("/api/event/user-event")
                        .param("partition", "1")
                        .param("key", "myKey")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(user)))
                .andExpect(status().isOk());

        verify(eventSender).sendEvent(user, "user-event", 1, "myKey");
    }

    @Test
    void sendEvent_withoutOptionalParams_delegatesToSenderWithNulls() throws Exception {
        User user = new User();
        user.setUsername("alice");
        user.setRealName("Alice Smith");
        user.setAge(25);
        user.setBalance(BigDecimal.valueOf(5000));

        mockMvc.perform(post("/api/event/another-user-event")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(user)))
                .andExpect(status().isOk());

        verify(eventSender).sendEvent(user, "another-user-event", null, null);
    }

    @Test
    void sendEvent_withOnlyPartition_delegatesCorrectly() throws Exception {
        User user = new User();
        user.setUsername("bob");
        user.setBalance(BigDecimal.valueOf(1000));

        mockMvc.perform(post("/api/event/user-event")
                        .param("partition", "2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(user)))
                .andExpect(status().isOk());

        verify(eventSender).sendEvent(user, "user-event", 2, null);
    }
}
